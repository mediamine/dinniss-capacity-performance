-- =============================================================================
-- MATERIALIZED VIEWS
-- =============================================================================
-- Run AFTER 01_create_views.sql (which creates the base key views these depend on).
--
-- Creation order (dependency chain):
--   1. leave_status_by_staff_date
--   2. 1_Job_Task_Details_Table
--   3. 4_Timesheet_Table  →  keys_time (regular view)
--   4. key02_job_task_staff_id
--   5. 2_Staff_Task_Allocation_byDay  →  key07_is_billable (regular view)
--   6. 3_Staff_Performance_Table
--
-- For daily refresh use 02_refresh_materialized_views.sql instead.
-- Only re-run this file when the view structure changes (adding/changing columns).
-- =============================================================================
-- leave_status_by_staff_date (Optimization 1)
-- Pre-computes is_full_day / has_partial_leave / partial_leave_hrs_per_day
-- per (staff_name, date). Eliminates the O(n⁴) inline jobtask × nested COUNT
-- subquery that previously ran inside every calendar-scanning lateral in
-- 1_Job_Task_Details_Table. Must be created before 1_Job_Task_Details_Table.
DROP MATERIALIZED VIEW IF EXISTS leave_status_by_staff_date CASCADE;


CREATE MATERIALIZED VIEW leave_status_by_staff_date AS
SELECT
    lta."Name" AS staff_name,
    cal."Date",
    COALESCE(BOOL_OR(mpd.mins_per_day = 480), FALSE) AS is_full_day,
    COALESCE(BOOL_OR(mpd.mins_per_day != 480), FALSE) AS has_partial_leave,
    -- sum of partial-leave hours per day (replaces inline AllocatedMins/WorkableDays/60 in tlh lateral)
    COALESCE(
        SUM(
            CASE
                WHEN mpd.mins_per_day != 480 THEN mpd.mins_per_day / 60.0
            END
        ),
        0
    ) AS partial_leave_hrs_per_day
FROM
    jobtask lt
    JOIN jobtaskassignee lta ON lta."JobTaskID" = lt."RemoteID"::uuid
    LEFT JOIN jobdetails ljd ON ljd."RemoteID"::text = lt."JobDetailsRemoteID"::text
    CROSS JOIN LATERAL (
        -- mins_per_day = AllocatedMinutes / WorkableDays(leave task) — computed once per leave task row
        SELECT
            lta."AllocatedMinutes"::float / NULLIF(
                (
                    SELECT
                        COUNT(*)
                    FROM
                        key01_calendar_date wcal
                    WHERE
                        wcal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
                        AND wcal."Date" <= COALESCE(
                            lt."DueDate",
                            CASE
                                WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                                ELSE LEAST(ljd."CompletedDate", ljd."DueDate")
                            END
                        )
                        AND wcal."WeekEnd" = FALSE
                        AND wcal."PublicHoliday" = FALSE
                        AND EXISTS (
                            SELECT
                                1
                            FROM
                                excel_workable_days lewd
                            WHERE
                                lewd.staffname = lta."Name"
                                AND lewd.day_of_week = wcal."Weekday"
                                AND lewd.working_day = TRUE
                        )
                ),
                0
            ) AS mins_per_day
    ) mpd
    JOIN key01_calendar_date cal ON cal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
    AND cal."Date" <= COALESCE(
        lt."DueDate",
        CASE
            WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
            ELSE LEAST(ljd."CompletedDate", ljd."DueDate")
        END
    )
WHERE
    lt."IsDeleted" = FALSE
    AND lta."AllocatedMinutes" > 0
    AND (
        lt."Name" ILIKE '%Holiday%'
        OR lt."Name" ILIKE '%Sick leave%'
        OR lt."Name" ILIKE '%Other leave%'
    )
GROUP BY
    lta."Name",
    cal."Date";


CREATE UNIQUE INDEX ON leave_status_by_staff_date (staff_name, "Date");


-- 1_Job_Task_Details_Table (Optimizations 2, 3, 4)
-- Materialized to avoid recomputing on every downstream query (Opt 2).
-- 8 calendar laterals consolidated into cal_counts using FILTER aggregation (Opt 3).
-- tmt lateral uses column comparisons instead of string concatenation (Opt 4).
-- Build: ~26s. Query: 57ms / 1000 rows.
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table" CASCADE;


CREATE MATERIALIZED VIEW "1_Job_Task_Details_Table" AS
SELECT
    b."Job_Task_Staff_ID",
    b."Job_ID",
    b."Task_UUID",
    b."Task_Name",
    b."EstimatedMinutes",
    b."ActualMinutes",
    b."Task_Billable",
    b."Task_Completed",
    b."Staff_UUID",
    b."Staff_Name",
    b."Client_Name",
    b."StartDate",
    b."DueDate",
    b."Task_Allocated_Mins",
    b."Task_Type",
    b."StartDateAdjusted",
    b."DueDateAdjusted",
    b."Is_Task_a_Leave",
    -- Workable_Days_Between_Task: computed via LATERAL so derived columns below can share it
    cal_counts.wdb_cnt AS "Workable_Days_Between_Task",
    -- Workable_Hrs_Between_Task = Workable_Days_Between_Task * 8
    cal_counts.wdb_cnt * 8 AS "Workable_Hrs_Between_Task",
    -- Initial_Avg_Mins_perWorkDay = DIVIDE(Task_Allocated_Mins, Workable_Days_Between_Task, BLANK())
    b."Task_Allocated_Mins"::float / NULLIF(cal_counts.wdb_cnt, 0) AS "Initial_Avg_Mins_perWorkDay",
    -- Total_Leave_Hrs_between_Workable_Days: only for non-leave tasks
    --   DAX: IF(Is_Task_a_Leave=FALSE, CALCULATE(SUM([Initial_Allo_Hrs_perWorkDay_KPI01]),
    --            FILTER(Staff_Name matches), FILTER(Task_Category="Leave Tasks"),
    --            FILTER(Is_Full_Day_Leave=FALSE), DATESBETWEEN(Date, StartDateAdjusted, DueDateAdjusted)), BLANK())
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN cal_counts.tlh_hrs
    END AS "Total_Leave_Hrs_between_Workable_Days",
    -- Rev_Workable_Days_Between_Task = (Workable_Hrs - Total_Leave_Hrs) / 8 (only for non-leave tasks)
    --   DAX: IF(Is_Task_a_Leave=FALSE, (Workable_Hrs_Between_Task - Total_Leave_Hrs_between_Workable_Days) / 8, BLANK())
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN (
            cal_counts.wdb_cnt * 8 - COALESCE(cal_counts.tlh_hrs, 0)
        ) / 8.0
    END AS "Rev_Workable_Days_Between_Task",
    -- Avg_Mins_perWorkDay_WITHOUT_Leave = DIVIDE(Task_Allocated_Mins, Rev_Workable_Days_Between_Task, BLANK()) when not leave
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN b."Task_Allocated_Mins"::float / NULLIF(
            (
                cal_counts.wdb_cnt * 8 - COALESCE(cal_counts.tlh_hrs, 0)
            ) / 8.0,
            0
        )
    END AS "Avg_Mins_perWorkDay_WITHOUT_Leave",
    -- Total_Task_Mins_WorkDays_WITHOUT_Leave = SUM(Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02)*60
    --   = COUNT(non-leave workable days) * Avg_Mins_perWorkDay_WITHOUT_Leave
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN cal_counts.ttl_cnt::float * b."Task_Allocated_Mins"::float / NULLIF(
            (
                cal_counts.wdb_cnt * 8 - COALESCE(cal_counts.tlh_hrs, 0)
            ) / 8.0,
            0
        )
    END AS "Total_Task_Mins_WorkDays_WITHOUT_Leave",
    -- Remaining_Allocated_Task_Mins = Task_Allocated_Mins - Total_Task_Mins_WorkDays_WITHOUT_Leave
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN b."Task_Allocated_Mins" - cal_counts.ttl_cnt::float * b."Task_Allocated_Mins"::float / NULLIF(
            (
                cal_counts.wdb_cnt * 8 - COALESCE(cal_counts.tlh_hrs, 0)
            ) / 8.0,
            0
        )
    END AS "Remaining_Allocated_Task_Mins",
    -- WorkDays_WITH_Leaves_between_Task: workable days with partial (not full-day) leave
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN cal_counts.wdl_cnt
    END AS "WorkDays_WITH_Leaves_between_Task",
    -- Avg_Mins_perWorkDay_WITH_Leaves = DIVIDE(Remaining_Allocated_Task_Mins, WorkDays_WITH_Leaves_between_Task, BLANK())
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN (
            b."Task_Allocated_Mins" - cal_counts.ttl_cnt::float * b."Task_Allocated_Mins"::float / NULLIF(
                (
                    cal_counts.wdb_cnt * 8 - COALESCE(cal_counts.tlh_hrs, 0)
                ) / 8.0,
                0
            )
        ) / NULLIF(cal_counts.wdl_cnt, 0)
    END AS "Avg_Mins_perWorkDay_WITH_Leaves",
    -- Task_Mins_Worked_Till_Date = SUM(Recorded_Minutes) from 4_Timesheet_Table for this Job_Task_Staff_ID
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN tmt.recorded_mins
    END AS "Task_Mins_Worked_Till_Date",
    -- IS_Task_Mins_Worked_>_Allocated = Task_Mins_Worked_Till_Date > Task_Allocated_Mins
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN tmt.recorded_mins > b."Task_Allocated_Mins"
    END AS "IS_Task_Mins_Worked_>_Allocated",
    -- Task_Mins_Remain_until_Due = IF(IS_Task_Mins_Worked_>_Allocated, 0, Task_Allocated_Mins - Task_Mins_Worked_Till_Date)
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN GREATEST(0, b."Task_Allocated_Mins" - tmt.recorded_mins)
    END AS "Task_Mins_Remain_until_Due",
    -- Allo_Mins_during_Remaining_workDays_WITH_leave
    --   = SUM(Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04)*60 for this Job_Task_Staff_ID
    --   = COUNT(remaining days with partial leave) * Avg_Mins_perWorkDay_WITH_Leaves
    --   RETURN condition: Is_Date_between_Today&Due=TRUE AND Task_Category="Billable Tasks"
    --   Is_Date_between_Today&Due = Date >= TODAY AND Is_Date_Between_Task_Days
    CASE
        WHEN NOT b."Is_Task_a_Leave"
        AND NOT (
            b."Task_Name" ILIKE '%Admin - Non-billable%'
            OR b."Client_Name" = 'Dinniss Admin'
        ) THEN cal_counts.arwd_cnt::float * (
            b."Task_Allocated_Mins" - cal_counts.ttl_cnt::float * b."Task_Allocated_Mins"::float / NULLIF(
                (
                    cal_counts.wdb_cnt * 8 - COALESCE(cal_counts.tlh_hrs, 0)
                ) / 8.0,
                0
            )
        ) / NULLIF(cal_counts.wdl_cnt, 0)
    END AS "Allo_Mins_during_Remaining_workDays_WITH_leave",
    -- Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave = Task_Mins_Remain_until_Due - Allo_Mins_during_Remaining_workDays_WITH_leave
    --   DAX treats BLANK as 0 in arithmetic, so Allo_Mins is 0 for admin/non-billable tasks
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN rmn.remain_mins
    END AS "Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave",
    -- Remain_WorkDays_WITHOUT_Leave: remaining workable days without any leave (TODAY to DueDateAdjusted)
    --   DAX: DATESBETWEEN(Date, TODAY(), DueDateAdjusted) AND Is_Day_With_a_Leave=FALSE AND Is_Workable_Day AND Is_Staff_Workable_DayOfWeek
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN cal_counts.rttl_cnt
    END AS "Remain_WorkDays_WITHOUT_Leave",
    -- Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave = DIVIDE(Remain_Mins_Allo, Remain_WorkDays_WITHOUT_Leave, BLANK())
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN rmn.remain_mins / NULLIF(cal_counts.rttl_cnt, 0)
    END AS "Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave",
    -- Is_Task_WITHIN_Allo_Time_IMP:
    --   IF(Remain_Mins > 0, IF(Remain_WorkDays >= 1 AND Avg_Remain_Mins <= 480, TRUE, FALSE), TRUE)
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN CASE
            WHEN rmn.remain_mins > 0 THEN cal_counts.rttl_cnt >= 1
            AND rmn.remain_mins / NULLIF(cal_counts.rttl_cnt, 0) <= 480
            ELSE TRUE
        END
    END AS "Is_Task_WITHIN_Allo_Time_IMP",
    -- Is_Task_DueDate_Over = DueDateAdjusted < TODAY()
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN b."DueDateAdjusted" < CURRENT_DATE
    END AS "Is_Task_DueDate_Over",
    -- Task_Mins_Worked_Adjusted = IF(IS_Task_Mins_Worked_>_Allocated OR Is_Task_DueDate_Over, Task_Allocated_Mins, Task_Mins_Worked_Till_Date)
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN CASE
            WHEN tmt.recorded_mins > b."Task_Allocated_Mins"
            OR b."DueDateAdjusted" < CURRENT_DATE THEN b."Task_Allocated_Mins"
            ELSE tmt.recorded_mins
        END
    END AS "Task_Mins_Worked_Adjusted",
    -- Prior_WorkDays_WITH_Leave: workable days with partial leave from StartDateAdjusted to TODAY
    --   DAX: Is_Date_Between_Task_Days AND Is_Date_between_Start&Today (Date<=TODAY) AND Is_Workable_Day
    --        AND Is_Day_With_a_Leave=TRUE AND Is_Staff_Workable_DayOfWeek AND Is_Full_Day_Leave=FALSE
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN cal_counts.pwdl_cnt
    END AS "Prior_WorkDays_WITH_Leave",
    -- Prior_WorkDays_WITHOUT_Leave: workable days without any leave from StartDateAdjusted to TODAY
    --   Same filters as Prior_WorkDays_WITH_Leave but Is_Day_With_a_Leave=FALSE
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN cal_counts.pttl_cnt
    END AS "Prior_WorkDays_WITHOUT_Leave",
    -- Allo_Mins_during_PriorWorkDays_WITH_leave = SUM(Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE)*60
    --   = cal_counts.pwdl_cnt * Avg_Mins_perWorkDay_WITH_Leaves (billable tasks only)
    --   Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE = Avg_Mins_perWorkDay_WITH_Leaves/60
    --     when Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek
    --          AND Is_Day_With_a_Leave=TRUE AND Is_Full_Day_Leave=FALSE
    --     RETURN: Is_Date_between_Start&Today=TRUE AND Task_Category="Billable Tasks"
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN pam.allo_mins
    END AS "Allo_Mins_during_PriorWorkDays_WITH_leave",
    -- Is_Mins_PriorWorkDays_WITH_Leave_>_Task_Mins_Worked = Allo_Mins > Task_Mins_Worked_Adjusted
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN pam.allo_mins > twa.adj_mins
    END AS "Is_Mins_PriorWorkDays_WITH_Leave_>_Task_Mins_Worked",
    -- Adj_Worked_Mins_PriorWorkDays_WITH_Leave = IF(Is_Mins_Prior_>_Worked, Task_Mins_Worked_Adjusted, Allo_Mins_during_PriorWorkDays_WITH_leave)
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN CASE
            WHEN pam.allo_mins > twa.adj_mins THEN twa.adj_mins
            ELSE pam.allo_mins
        END
    END AS "Adj_Worked_Mins_PriorWorkDays_WITH_Leave",
    -- Adj_Worked_Mins_PriorWorkDays_WITHOUT_Leave = Task_Mins_Worked_Adjusted - Adj_Worked_Mins_PriorWorkDays_WITH_Leave
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN twa.adj_mins - CASE
            WHEN pam.allo_mins > twa.adj_mins THEN twa.adj_mins
            ELSE pam.allo_mins
        END
    END AS "Adj_Worked_Mins_PriorWorkDays_WITHOUT_Leave",
    -- Avg_Worked_Mins_perPriorDays_WITH_Leave = DIVIDE(Adj_Worked_Mins_PriorWorkDays_WITH_Leave, Prior_WorkDays_WITH_Leave, BLANK())
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN CASE
            WHEN pam.allo_mins > twa.adj_mins THEN twa.adj_mins
            ELSE pam.allo_mins
        END / NULLIF(cal_counts.pwdl_cnt, 0)
    END AS "Avg_Worked_Mins_perPriorDays_WITH_Leave",
    -- Avg_Worked_Mins_perPriorDays_WITHOUT_Leave = DIVIDE(Adj_Worked_Mins_PriorWorkDays_WITHOUT_Leave, Prior_WorkDays_WITHOUT_Leave, BLANK())
    CASE
        WHEN NOT b."Is_Task_a_Leave" THEN (
            twa.adj_mins - CASE
                WHEN pam.allo_mins > twa.adj_mins THEN twa.adj_mins
                ELSE pam.allo_mins
            END
        ) / NULLIF(cal_counts.pttl_cnt, 0)
    END AS "Avg_Worked_Mins_perPriorDays_WITHOUT_Leave"
FROM
    (
        SELECT
            (
                jt."JobDetailsRemoteID"::text || jt."UUID"::text || jta."UUID"::text
            ) AS "Job_Task_Staff_ID",
            jt."JobDetailsRemoteID" AS "Job_ID",
            jt."UUID" AS "Task_UUID",
            jt."Name" AS "Task_Name",
            jt."EstimatedMinutes",
            jt."ActualMinutes",
            jt."Billable" AS "Task_Billable",
            jt."Completed" AS "Task_Completed",
            jta."UUID" AS "Staff_UUID",
            jta."Name" AS "Staff_Name",
            cd."Name" AS "Client_Name",
            jt."StartDate",
            jt."DueDate",
            jta."AllocatedMinutes" AS "Task_Allocated_Mins",
            -- Task_Type: LOOKUPVALUE(key04_task_name[Task_Type], key04_task_name[Task_Name], [Task_Name])
            ktn."Task_Type",
            -- StartDateAdjusted: IF(ISBLANK([StartDate]), LOOKUPVALUE(key06_job_table[StartDate], ..., [Job_ID]), [StartDate])
            COALESCE(jt."StartDate", jd."StartDate") AS "StartDateAdjusted",
            -- DueDateAdjusted: IF(ISBLANK([DueDate]), LOOKUPVALUE(key06_job_table[EarlierDate], ..., [Job_ID]), [DueDate])
            --   EarlierDate = IF(ISBLANK([CompletedDate]), [DueDate], IF([CompletedDate]<[DueDate], [CompletedDate], [DueDate]))
            COALESCE(
                jt."DueDate",
                CASE
                    WHEN jd."CompletedDate" IS NULL THEN jd."DueDate"
                    ELSE LEAST(jd."CompletedDate", jd."DueDate")
                END
            ) AS "DueDateAdjusted",
            -- Is_Task_a_Leave: OR(CONTAINSSTRING([Task_Name],"Holiday"), CONTAINSSTRING(...,"Sick leave"), CONTAINSSTRING(...,"Other leave"))
            (
                jt."Name" ILIKE '%Holiday%'
                OR jt."Name" ILIKE '%Sick leave%'
                OR jt."Name" ILIKE '%Other leave%'
            ) AS "Is_Task_a_Leave"
        FROM
            jobtask jt
            LEFT JOIN jobtaskassignee jta ON jta."JobTaskID" = jt."RemoteID"::uuid
            LEFT JOIN jobdetails jd ON jd."RemoteID" = jt."JobDetailsRemoteID"
            LEFT JOIN clientdetails cd ON cd."UUID" = jd."ClientUUID"
            LEFT JOIN key04_task_name ktn ON ktn."Task_Name" = jt."Name"
        WHERE
            jt."IsDeleted" = FALSE
            AND jta."Name" IS NOT NULL
            AND jta."Name" NOT IN (
                'Anna Williams',
                'Conor Cameron',
                'Conor O''Brien',
                'Dinniss',
                'Sahar Sedaghat',
                'The OLD - Dani Millar'
            )
    ) b
    CROSS JOIN LATERAL (
        -- cal_counts: single calendar scan replacing 8 separate laterals (wdb/tlh/ttl/wdl/arwd/rttl/pwdl/pttl).
        -- One pass over key01_calendar_date for this task's date range; FILTER clauses partition by leave status.
        SELECT
            COUNT(*) FILTER (
                WHERE
                    NOT COALESCE(lv.is_full_day, FALSE)
            ) AS wdb_cnt,
            COALESCE(
                SUM(lv.partial_leave_hrs_per_day) FILTER (
                    WHERE
                        NOT COALESCE(lv.is_full_day, FALSE)
                ),
                0
            ) AS tlh_hrs,
            COUNT(*) FILTER (
                WHERE
                    NOT COALESCE(lv.has_partial_leave, FALSE)
                    AND NOT COALESCE(lv.is_full_day, FALSE)
                    AND NOT (
                        b."Task_Name" = 'Admin - Non-billable'
                        AND cal."Date" >= DATE '2021-02-01'
                    )
            ) AS ttl_cnt,
            COUNT(*) FILTER (
                WHERE
                    COALESCE(lv.has_partial_leave, FALSE)
                    AND NOT COALESCE(lv.is_full_day, FALSE)
            ) AS wdl_cnt,
            COUNT(*) FILTER (
                WHERE
                    COALESCE(lv.has_partial_leave, FALSE)
                    AND NOT COALESCE(lv.is_full_day, FALSE)
                    AND cal."Date" >= CURRENT_DATE
            ) AS arwd_cnt,
            COUNT(*) FILTER (
                WHERE
                    NOT COALESCE(lv.has_partial_leave, FALSE)
                    AND NOT COALESCE(lv.is_full_day, FALSE)
                    AND cal."Date" >= CURRENT_DATE
                    AND NOT (
                        b."Task_Name" = 'Admin - Non-billable'
                        AND cal."Date" >= DATE '2021-02-01'
                    )
            ) AS rttl_cnt,
            COUNT(*) FILTER (
                WHERE
                    COALESCE(lv.has_partial_leave, FALSE)
                    AND NOT COALESCE(lv.is_full_day, FALSE)
                    AND cal."Date" <= CURRENT_DATE
            ) AS pwdl_cnt,
            COUNT(*) FILTER (
                WHERE
                    NOT COALESCE(lv.has_partial_leave, FALSE)
                    AND NOT COALESCE(lv.is_full_day, FALSE)
                    AND cal."Date" <= CURRENT_DATE
                    AND NOT (
                        b."Task_Name" = 'Admin - Non-billable'
                        AND cal."Date" >= DATE '2021-02-01'
                    )
            ) AS pttl_cnt
        FROM
            key01_calendar_date cal
            LEFT JOIN leave_status_by_staff_date lv ON lv.staff_name = b."Staff_Name"
            AND lv."Date" = cal."Date"
        WHERE
            cal."Date" >= b."StartDateAdjusted"
            AND cal."Date" <= b."DueDateAdjusted"
            AND cal."WeekEnd" = FALSE
            AND cal."PublicHoliday" = FALSE
            AND EXISTS (
                SELECT
                    1
                FROM
                    excel_workable_days ewd
                WHERE
                    ewd.staffname = b."Staff_Name"
                    AND ewd.day_of_week = cal."Weekday"
                    AND ewd.working_day = TRUE
            )
    ) cal_counts
    CROSS JOIN LATERAL (
        -- tmt: total recorded minutes from the time table for this task (Optimization 4)
        --   DAX: CALCULATE(SUM([Recorded_Minutes]), FILTER([Job_Task_Staff_ID] = current))
        --   Uses column comparisons (not string concatenation) so indexes on the
        --   underlying time base table can be used.
        SELECT
            COALESCE(SUM(t."Minutes"), 0) AS recorded_mins
        FROM
            "time" t
        WHERE
            t."JobID"::text = b."Job_ID"::text
            AND t."TaskUUID"::text = b."Task_UUID"::text
            AND t."StaffMemberUUID"::text = b."Staff_UUID"::text
            AND t."Date" >= '2020-01-01'
    ) tmt
    CROSS JOIN LATERAL (
        -- rmn: pre-compute Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave for reuse across columns
        --   = Task_Mins_Remain_until_Due - Allo_Mins_during_Remaining_workDays_WITH_leave
        --   Allo_Mins is 0 (not NULL) for admin/non-billable tasks, matching DAX BLANK-as-0 arithmetic
        SELECT
            GREATEST(0, b."Task_Allocated_Mins" - tmt.recorded_mins) - CASE
                WHEN NOT (
                    b."Task_Name" ILIKE '%Admin - Non-billable%'
                    OR b."Client_Name" = 'Dinniss Admin'
                ) THEN COALESCE(
                    cal_counts.arwd_cnt::float * (
                        b."Task_Allocated_Mins" - cal_counts.ttl_cnt::float * b."Task_Allocated_Mins"::float / NULLIF(
                            (
                                cal_counts.wdb_cnt * 8 - COALESCE(cal_counts.tlh_hrs, 0)
                            ) / 8.0,
                            0
                        )
                    ) / NULLIF(cal_counts.wdl_cnt, 0),
                    0
                )
                ELSE 0
            END AS remain_mins
    ) rmn
    CROSS JOIN LATERAL (
        -- twa: pre-compute Task_Mins_Worked_Adjusted for reuse
        --   = IF(IS_Task_Mins_Worked_>_Allocated OR Is_Task_DueDate_Over, Task_Allocated_Mins, Task_Mins_Worked_Till_Date)
        SELECT
            CASE
                WHEN tmt.recorded_mins > b."Task_Allocated_Mins"
                OR b."DueDateAdjusted" < CURRENT_DATE THEN b."Task_Allocated_Mins"
                ELSE tmt.recorded_mins
            END AS adj_mins
    ) twa
    CROSS JOIN LATERAL (
        -- pam: pre-compute Allo_Mins_during_PriorWorkDays_WITH_leave for reuse
        --   = cal_counts.pwdl_cnt * Avg_Mins_perWorkDay_WITH_Leaves (billable tasks only, NULL for admin)
        SELECT
            CASE
                WHEN NOT (
                    b."Task_Name" ILIKE '%Admin - Non-billable%'
                    OR b."Client_Name" = 'Dinniss Admin'
                ) THEN cal_counts.pwdl_cnt::float * (
                    b."Task_Allocated_Mins" - cal_counts.ttl_cnt::float * b."Task_Allocated_Mins"::float / NULLIF(
                        (
                            cal_counts.wdb_cnt * 8 - COALESCE(cal_counts.tlh_hrs, 0)
                        ) / 8.0,
                        0
                    )
                ) / NULLIF(cal_counts.wdl_cnt, 0)
                ELSE NULL
            END AS allo_mins
    ) pam;


CREATE UNIQUE INDEX ON "1_Job_Task_Details_Table" ("Job_Task_Staff_ID");


CREATE INDEX ON "1_Job_Task_Details_Table" ("Staff_Name");


-- 4_Timesheet_Table (Optimizations A, B)
-- A: Converted to MATERIALIZED VIEW — eliminates re-execution of all joins on every downstream query.
-- B: Lookup LATERALs (jt_lkp, e_lkp, inv_lkp, k2_lkp) replaced with regular LEFT JOINs — allows
--    the planner to use hash/merge joins across the full dataset instead of one nested-loop
--    lookup per row. jt_lkp and e_lkp use DISTINCT ON subqueries to match the LIMIT 1 behaviour
--    from the original (prevents row multiplication if the source has duplicate keys).
--    Computation-only laterals (ic, ibt, mto) remain as CROSS JOIN LATERAL.
-- Refresh order: 1_Job_Task_Details_Table → 4_Timesheet_Table → keys_time → key02_job_task_staff_id
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table" CASCADE;


CREATE MATERIALIZED VIEW "4_Timesheet_Table" AS
SELECT
    ROW_NUMBER() OVER () AS "row_id",
    (t."JobID" || t."TaskUUID" || t."StaffMemberUUID") AS "Job_Task_Staff_ID",
    t."UUID" AS "Timesheet_UUID",
    t."JobID" AS "Job_ID",
    t."TaskUUID" AS "Task_ID",
    t."StaffMemberUUID" AS "Staff_ID",
    s."Staff_Name",
    t."Note" AS "Timesheet_Notes",
    t."Billable",
    t."InvoiceUUID" AS "Invoice_ID",
    t."InvoiceTaskUUID" AS "Invoice_Task_ID",
    t."Date",
    t."Minutes" AS "Recorded_Minutes",
    -- Task_Name: LOOKUPVALUE from 1_Job_Task_Details_Table by Job_Task_Staff_ID, fallback to excel_recorded_invoiced_hours[Task] by Timesheet_UUID
    COALESCE(jt_lkp."Task_Name", e_lkp.task) AS "Task_Name",
    -- Client_Name: LOOKUPVALUE from 1_Job_Task_Details_Table by Job_Task_Staff_ID, fallback to excel_recorded_invoiced_hours[Client] by Timesheet_UUID
    COALESCE(jt_lkp."Client_Name", e_lkp.client) AS "Client_Name",
    -- Task_Type: LOOKUPVALUE from 1_Job_Task_Details_Table by Job_Task_Staff_ID, fallback to excel_recorded_invoiced_hours[Task] by Timesheet_UUID
    COALESCE(jt_lkp."Task_Type", e_lkp.task) AS "Task_Type",
    -- Task_Completed: LOOKUPVALUE from 1_Job_Task_Details_Table by Job_Task_Staff_ID
    jt_lkp."Task_Completed" AS "Task_Completed",
    -- Is_Client = NOT (Client_Name="Dinniss Admin" OR Task_Type="Admin - Non-billable")
    ic.is_client AS "Is_Client",
    -- Invoiced_Time: IF(AND(ISBLANK(Invoice_Task_ID), Is_Client), "Un-Invoiced", IF(ISBLANK(Invoice_Task_ID), "Dinniss Time", "Invoiced"))
    CASE
        WHEN t."InvoiceTaskUUID" IS NULL
        AND ic.is_client THEN 'Un-Invoiced'
        WHEN t."InvoiceTaskUUID" IS NULL THEN 'Dinniss Time'
        ELSE 'Invoiced'
    END AS "Invoiced_Time",
    -- Is_Billable = NOT (Is_Client=FALSE OR Task_Type="Coaching")
    (
        ic.is_client
        AND COALESCE(jt_lkp."Task_Type", e_lkp.task) <> 'Coaching'
    ) AS "Is_Billable",
    -- Billable_Selector = IF(Is_Billable, "Billable", "Not Billable")
    CASE
        WHEN ic.is_client
        AND COALESCE(jt_lkp."Task_Type", e_lkp.task) <> 'Coaching' THEN 'Billable'
        ELSE 'Not Billable'
    END AS "Billable_Selector",
    -- Task_Category: Leave/Admin/Billable based on Task_Name and Client_Name
    CASE
        WHEN COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Holiday%'
        OR COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Other leave%'
        OR COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Sick leave%' THEN 'Leave Tasks'
        WHEN COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Admin - Non-billable%'
        OR COALESCE(jt_lkp."Client_Name", e_lkp.client) = 'Dinniss Admin' THEN 'Admin Tasks'
        ELSE 'Billable Tasks'
    END AS "Task_Category",
    -- Invoice_Number: LOOKUPVALUE(invoicetask[InvoiceID], invoicetask[UUID], Invoice_Task_ID)
    it."InvoiceID" AS "Invoice_Number",
    -- Month_Time_Recorded = DATE(YEAR(Date), MONTH(Date), 1) = first day of recorded month
    DATE_TRUNC('month', t."Date")::DATE AS "Month_Time_Recorded",
    -- Month_Time_Invoiced: first day of month the invoice was raised
    DATE_TRUNC('month', inv."Date")::DATE AS "Month_Time_Invoiced",
    -- Invoiced_Minutes: LOOKUPVALUE(excel_recorded_invoiced_hours[Invoiced_Mins], ..., Timesheet_UUID)
    e_lkp.invoiced_mins AS "Invoiced_Minutes",
    -- Month_Invoiced_On: DATEDIFF(Month_Time_Recorded, Month_Time_Invoiced, MONTH); -1→1, else diff+1; NULL if no invoice
    mto.val AS "Month_Invoiced_On",
    -- Recorded_Hours_invoiced: billing timing category (Billable Tasks only)
    CASE
        WHEN ibt.is_billable_task THEN CASE
            WHEN t."InvoiceTaskUUID" IS NULL
            AND ic.is_client THEN '5_Un-Invoiced'
            WHEN mto.val = 1 THEN '1_Same Month'
            WHEN mto.val = 2 THEN '2_Following Month'
            WHEN mto.val = 3 THEN '3_Third Month'
            ELSE '4_Fourth Month +'
        END
    END AS "Recorded_Hours_invoiced",
    -- Job_Name: RELATED(key02_job_task_staff_id[Job_Name])
    k2_lkp."Job_Name" AS "Job_Name"
FROM
    TIME t
    LEFT JOIN "key03_staff_table" s ON s."Staff_UUID" = t."StaffMemberUUID"
    -- jt_lkp: DISTINCT ON matches LIMIT 1 behaviour; regular JOIN allows hash/merge join (Opt B)
    LEFT JOIN (
        SELECT DISTINCT
            ON ("Job_Task_Staff_ID") "Job_Task_Staff_ID",
            "Task_Name",
            "Client_Name",
            "Task_Type",
            "Task_Completed"
        FROM
            "1_Job_Task_Details_Table"
        ORDER BY
            "Job_Task_Staff_ID"
    ) jt_lkp ON jt_lkp."Job_Task_Staff_ID" = (t."JobID" || t."TaskUUID" || t."StaffMemberUUID")
    -- e_lkp: DISTINCT ON matches LIMIT 1 behaviour; regular JOIN allows hash/merge join (Opt B)
    LEFT JOIN (
        SELECT DISTINCT
            ON (timesheet_uuid) timesheet_uuid,
            task,
            client,
            invoiced_mins
        FROM
            excel_recorded_invoiced_hours
        ORDER BY
            timesheet_uuid
    ) e_lkp ON e_lkp.timesheet_uuid = t."UUID"::text
    CROSS JOIN LATERAL (
        -- ic: pre-compute Is_Client to reuse across Invoiced_Time, Is_Billable, Billable_Selector
        SELECT
            NOT (
                COALESCE(jt_lkp."Client_Name", e_lkp.client) = 'Dinniss Admin'
                OR COALESCE(jt_lkp."Task_Type", e_lkp.task) = 'Admin - Non-billable'
            ) AS is_client
    ) ic
    CROSS JOIN LATERAL (
        -- ibt: pre-compute is_billable_task for Recorded_Hours_invoiced
        SELECT
            NOT (
                COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Holiday%'
                OR COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Other leave%'
                OR COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Sick leave%'
                OR COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Admin - Non-billable%'
                OR COALESCE(jt_lkp."Client_Name", e_lkp.client) = 'Dinniss Admin'
            ) AS is_billable_task
    ) ibt
    -- inv_lkp split into two regular JOINs; invoicetask.UUID is unique so no row multiplication (Opt B)
    LEFT JOIN invoicetask it ON it."UUID" = t."InvoiceTaskUUID"
    AND it."IsDeleted" = FALSE
    LEFT JOIN invoice inv ON inv."ID" = it."InvoiceID"
    AND inv."IsDeleted" = FALSE
    CROSS JOIN LATERAL (
        -- mto: Month_Invoiced_On = IF(ISBLANK(Invoice_Number), BLANK(), IF(Logic=-1, 1, Logic+1))
        --   Logic = DATEDIFF(Month_Time_Recorded, Month_Time_Invoiced, MONTH)
        SELECT
            CASE
                WHEN it."InvoiceID" IS NULL THEN NULL
                ELSE (
                    CASE
                        WHEN (
                            (
                                EXTRACT(
                                    YEAR
                                    FROM
                                        DATE_TRUNC('month', inv."Date")::DATE
                                ) - EXTRACT(
                                    YEAR
                                    FROM
                                        DATE_TRUNC('month', t."Date")::DATE
                                )
                            ) * 12 + EXTRACT(
                                MONTH
                                FROM
                                    DATE_TRUNC('month', inv."Date")::DATE
                            ) - EXTRACT(
                                MONTH
                                FROM
                                    DATE_TRUNC('month', t."Date")::DATE
                            )
                        )::int = -1 THEN 1
                        ELSE (
                            (
                                EXTRACT(
                                    YEAR
                                    FROM
                                        DATE_TRUNC('month', inv."Date")::DATE
                                ) - EXTRACT(
                                    YEAR
                                    FROM
                                        DATE_TRUNC('month', t."Date")::DATE
                                )
                            ) * 12 + EXTRACT(
                                MONTH
                                FROM
                                    DATE_TRUNC('month', inv."Date")::DATE
                            ) - EXTRACT(
                                MONTH
                                FROM
                                    DATE_TRUNC('month', t."Date")::DATE
                            )
                        )::int + 1
                    END
                )
            END AS val
    ) mto
    -- k2_lkp: Job_ID is unique in key06_job_table; regular JOIN allows hash/merge join (Opt B)
    LEFT JOIN key06_job_table k2_lkp ON k2_lkp."Job_ID" = t."JobID"
WHERE
    t."Date" >= '2020-01-01'
    AND s."Staff_Name" IS NOT NULL;


CREATE INDEX ON "4_Timesheet_Table" ("Job_Task_Staff_ID");


CREATE INDEX ON "4_Timesheet_Table" ("Staff_Name");


CREATE INDEX ON "4_Timesheet_Table" ("Date");


CREATE INDEX ON "4_Timesheet_Table" ("Staff_Name", "Date");


CREATE UNIQUE INDEX ON "4_Timesheet_Table" ("row_id");


DROP VIEW IF EXISTS keys_time CASCADE;


CREATE OR REPLACE VIEW keys_time AS
SELECT
    "Job_Task_Staff_ID",
    "Job_ID",
    "Task_ID",
    "Staff_Name",
    "Staff_ID" AS "Staff_UUID"
FROM
    "4_Timesheet_Table";


-- key02_job_task_staff_id (Optimization I)
-- I: Converted to MATERIALIZED VIEW — eliminates re-execution of the UNION ALL + DISTINCT +
--    per-row ILIKE scalar subquery (Task_Type1) on every build of 2_Staff_Task_Allocation_byDay.
--    Both drop forms handle first-run transition from VIEW → MATERIALIZED VIEW.
DROP VIEW IF EXISTS key02_job_task_staff_id CASCADE;


DROP MATERIALIZED VIEW IF EXISTS key02_job_task_staff_id CASCADE;


CREATE MATERIALIZED VIEW key02_job_task_staff_id AS
SELECT DISTINCT
    combined."Job_Task_Staff_ID",
    combined."Job_ID",
    combined."Staff_Name",
    -- Task_Name: LOOKUPVALUE from 1_Job_Task_Details_Table
    jt."Task_Name",
    -- Client_Name: LOOKUPVALUE from key06_job_table
    j."Client_Name",
    -- Job_Name: LOOKUPVALUE from key06_job_table
    j."Job_Name",
    -- Task_Category
    CASE
        WHEN jt."Task_Name" ILIKE '%Holiday%'
        OR jt."Task_Name" ILIKE '%Other leave%'
        OR jt."Task_Name" ILIKE '%Sick leave%' THEN 'Leave Tasks'
        WHEN jt."Task_Name" ILIKE '%Admin - Non-billable%'
        OR j."Client_Name" = 'Dinniss Admin' THEN 'Admin Tasks'
        ELSE 'Billable Tasks'
    END AS "Task_Category",
    -- Task_Type1: match Task_Name against key05_task_type Names
    COALESCE(
        (
            SELECT
                kt."Name"
            FROM
                "key05_task_type" kt
            WHERE
                jt."Task_Name" ILIKE ('%' || kt."Name" || '%')
            LIMIT
                1
        ),
        jt."Task_Name"
    ) AS "Task_Type1"
FROM
    (
        SELECT
            "Job_Task_Staff_ID",
            "Job_ID",
            "Task_UUID" AS "Task_ID",
            "Staff_UUID",
            "Staff_Name"
        FROM
            "1_Job_Task_Details_Table"
        UNION ALL
        SELECT
            "Job_Task_Staff_ID",
            "Job_ID",
            "Task_ID",
            "Staff_UUID",
            "Staff_Name"
        FROM
            keys_time
    ) combined
    LEFT JOIN "1_Job_Task_Details_Table" jt ON jt."Job_Task_Staff_ID" = combined."Job_Task_Staff_ID"
    LEFT JOIN "key06_job_table" j ON j."Job_ID" = combined."Job_ID";


CREATE UNIQUE INDEX ON key02_job_task_staff_id ("Job_Task_Staff_ID");


CREATE INDEX ON key02_job_task_staff_id ("Staff_Name");


-- 2_Staff_Task_Allocation_byDay (Optimizations C, J)
-- C: Converted to MATERIALIZED VIEW with (Staff_Name, Date) and (Job_Task_Staff_ID) indexes.
--    lv lateral (O(n⁴) inline jobtask × nested COUNT scan) replaced with a JOIN to
--    leave_status_by_staff_date — the same pre-computed materialized view used by
--    1_Job_Task_Details_Table. wkd lateral replaced with a regular LEFT JOIN.
-- Refresh order: leave_status_by_staff_date → 1_Job_Task_Details_Table
--   → key02_job_task_staff_id → 2_Staff_Task_Allocation_byDay
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay" CASCADE;


CREATE MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay" AS
SELECT
    c.*,
    k.*,
    -- Is_Client
    CASE
        WHEN k."Client_Name" = 'Dinniss Admin'
        OR k."Task_Type1" = 'Admin - Non-billable' THEN FALSE
        ELSE TRUE
    END AS "Is_Client",
    -- Is_Billable
    CASE
        WHEN k."Client_Name" = 'Dinniss Admin'
        OR k."Task_Type1" = 'Admin - Non-billable'
        OR k."Task_Type1" = 'Coaching' THEN FALSE
        ELSE TRUE
    END AS "Is_Billable",
    -- Billable_Selector
    CASE
        WHEN k."Client_Name" = 'Dinniss Admin'
        OR k."Task_Type1" = 'Admin - Non-billable'
        OR k."Task_Type1" = 'Coaching' THEN 'Not Billable'
        ELSE 'Billable'
    END AS "Billable_Selector",
    -- StartDateAdjusted: adjusted start date for this task (LOOKUPVALUE via jt join)
    jt."StartDateAdjusted" AS "StartDateAdjusted",
    -- DueDateAdjusted: adjusted due date for this task (LOOKUPVALUE via jt join)
    jt."DueDateAdjusted" AS "DueDateAdjusted",
    -- Is_Workable_Day: date is not a weekend and not a public holiday
    (
        NOT c."WeekEnd"
        AND NOT c."PublicHoliday"
    ) AS "Is_Workable_Day",
    -- Is_Date_Between_Task_Days: c.Date falls within this task's adjusted start/due dates
    (
        c."Date" >= jt."StartDateAdjusted"
        AND c."Date" <= jt."DueDateAdjusted"
    ) AS "Is_Date_Between_Task_Days",
    -- Is_Date_between_Today&Due: date >= today AND Is_Date_Between_Task_Days = TRUE
    -- (WHERE filter guarantees Is_Date_Between_Task_Days is always TRUE for rows here)
    (c."Date" >= CURRENT_DATE) AS "Is_Date_between_Today&Due",
    -- Is_Date_between_Start&Today: date < today AND Is_Date_Between_Task_Days = TRUE
    (c."Date" < CURRENT_DATE) AS "Is_Date_between_Start&Today",
    -- Is_Task_a_Leave: this task is a leave task (Holiday / Sick leave / Other leave)
    COALESCE(jt."Is_Task_a_Leave", FALSE) AS "Is_Task_a_Leave",
    -- Is_Full_Day_Leave: on this date, this staff has ANY full-day leave task (480 mins/workday)
    COALESCE(lv.is_full_day, FALSE) AS "Is_Full_Day_Leave",
    -- Admin_Task_To_Be_Removed: Task_Name = 'Admin - Non-billable' AND Date >= 2021-02-01
    (
        k."Task_Name" = 'Admin - Non-billable'
        AND c."Date" >= DATE '2021-02-01'
    ) AS "Admin_Task_To_Be_Removed",
    -- Is_Staff_Workable_DayOfWeek: staff works on this weekday per excel_workable_days
    COALESCE(wkd.working_day, FALSE) AS "Is_Staff_Workable_DayOfWeek",
    -- Is_Day_With_a_Leave: on this date, this staff has ANY partial (non-full-day) leave task
    COALESCE(lv.has_partial_leave, FALSE) AS "Is_Day_With_a_Leave",
    -- Initial_Allo_Hrs_perWorkDay_KPI01:
    --   IF(Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek
    --      AND Is_Full_Day_Leave=FALSE AND Admin_Task_To_Be_Removed=FALSE,
    --      Initial_Avg_Mins_perWorkDay/60, BLANK())
    CASE
        WHEN NOT c."WeekEnd"
        AND NOT c."PublicHoliday"
        AND c."Date" >= jt."StartDateAdjusted"
        AND c."Date" <= jt."DueDateAdjusted"
        AND COALESCE(wkd.working_day, FALSE)
        AND NOT COALESCE(lv.is_full_day, FALSE)
        AND NOT (
            k."Task_Type1" = 'Admin - Non-billable'
            OR k."Client_Name" = 'Dinniss Admin'
        ) THEN jt."Initial_Avg_Mins_perWorkDay" / 60.0
    END AS "Initial_Allo_Hrs_perWorkDay_KPI01",
    -- Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02:
    --   IF(Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek
    --      AND Is_Day_With_a_Leave=FALSE AND Is_Task_a_Leave=FALSE AND Is_Full_Day_Leave=FALSE
    --      AND Admin_Task_To_Be_Removed=FALSE,
    --      Avg_Mins_perWorkDay_WITHOUT_Leave/60, BLANK())
    CASE
        WHEN NOT c."WeekEnd"
        AND NOT c."PublicHoliday"
        AND c."Date" >= jt."StartDateAdjusted"
        AND c."Date" <= jt."DueDateAdjusted"
        AND COALESCE(wkd.working_day, FALSE)
        AND NOT COALESCE(lv.has_partial_leave, FALSE)
        AND NOT COALESCE(jt."Is_Task_a_Leave", FALSE)
        AND NOT COALESCE(lv.is_full_day, FALSE)
        AND NOT (
            k."Task_Type1" = 'Admin - Non-billable'
            OR k."Client_Name" = 'Dinniss Admin'
        ) THEN jt."Avg_Mins_perWorkDay_WITHOUT_Leave" / 60.0
    END AS "Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02",
    -- Allo_Hrs_perWorkday_WITH_Leave_KPI03:
    --   IF(Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek
    --      AND Is_Day_With_a_Leave=TRUE AND Is_Task_a_Leave=FALSE AND Is_Full_Day_Leave=FALSE
    --      AND Admin_Task_To_Be_Removed=FALSE,
    --      Avg_Mins_perWorkDay_WITH_Leaves/60, BLANK())
    CASE
        WHEN NOT c."WeekEnd"
        AND NOT c."PublicHoliday"
        AND c."Date" >= jt."StartDateAdjusted"
        AND c."Date" <= jt."DueDateAdjusted"
        AND COALESCE(wkd.working_day, FALSE)
        AND COALESCE(lv.has_partial_leave, FALSE)
        AND NOT COALESCE(jt."Is_Task_a_Leave", FALSE)
        AND NOT COALESCE(lv.is_full_day, FALSE)
        AND NOT (
            k."Task_Type1" = 'Admin - Non-billable'
            OR k."Client_Name" = 'Dinniss Admin'
        ) THEN jt."Avg_Mins_perWorkDay_WITH_Leaves" / 60.0
    END AS "Allo_Hrs_perWorkday_WITH_Leave_KPI03",
    -- Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04:
    --   RETURN IF(Is_Date_between_Today&Due AND Task_Category="Billable Tasks",
    --     IF(Is_Workable_Day AND Is_Date_Between_Task_Days,
    --       IF(Is_Staff_Workable_DayOfWeek AND Is_Day_With_a_Leave AND NOT Is_Task_a_Leave AND NOT Is_Full_Day_Leave,
    --          Avg_Mins_perWorkDay_WITH_Leaves/60, BLANK()), BLANK()), BLANK())
    CASE
        WHEN c."Date" >= CURRENT_DATE
        AND k."Task_Category" = 'Billable Tasks'
        AND NOT c."WeekEnd"
        AND NOT c."PublicHoliday"
        AND COALESCE(wkd.working_day, FALSE)
        AND COALESCE(lv.has_partial_leave, FALSE)
        AND NOT COALESCE(jt."Is_Task_a_Leave", FALSE)
        AND NOT COALESCE(lv.is_full_day, FALSE) THEN jt."Avg_Mins_perWorkDay_WITH_Leaves" / 60.0
    END AS "Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04",
    -- Allo_Hrs_perRemainingWorkDay_WITHOUT_LEAVE_KPI05:
    --   RETURN IF(Is_Date_between_Today&Due AND Task_Category="Billable Tasks",
    --     IF(Is_Workable_Day AND Is_Date_Between_Task_Days,
    --       IF(Is_Staff_Workable_DayOfWeek AND NOT Is_Day_With_a_Leave AND NOT Is_Task_a_Leave AND NOT Is_Full_Day_Leave,
    --          Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave/60, BLANK()), BLANK()), BLANK())
    CASE
        WHEN c."Date" >= CURRENT_DATE
        AND k."Task_Category" = 'Billable Tasks'
        AND NOT c."WeekEnd"
        AND NOT c."PublicHoliday"
        AND COALESCE(wkd.working_day, FALSE)
        AND NOT COALESCE(lv.has_partial_leave, FALSE)
        AND NOT COALESCE(jt."Is_Task_a_Leave", FALSE)
        AND NOT COALESCE(lv.is_full_day, FALSE) THEN jt."Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave" / 60.0
    END AS "Allo_Hrs_perRemainingWorkDay_WITHOUT_LEAVE_KPI05",
    -- Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE:
    --   RETURN IF(Is_Date_between_Start&Today AND Task_Category="Billable Tasks",
    --     IF(Is_Workable_Day AND Is_Date_Between_Task_Days,
    --       IF(Is_Staff_Workable_DayOfWeek AND Is_Day_With_a_Leave AND NOT Is_Task_a_Leave AND NOT Is_Full_Day_Leave,
    --          Avg_Mins_perWorkDay_WITH_Leaves/60, BLANK()), BLANK()), BLANK())
    CASE
        WHEN c."Date" < CURRENT_DATE
        AND k."Task_Category" = 'Billable Tasks'
        AND NOT c."WeekEnd"
        AND NOT c."PublicHoliday"
        AND COALESCE(wkd.working_day, FALSE)
        AND COALESCE(lv.has_partial_leave, FALSE)
        AND NOT COALESCE(jt."Is_Task_a_Leave", FALSE)
        AND NOT COALESCE(lv.is_full_day, FALSE) THEN jt."Avg_Mins_perWorkDay_WITH_Leaves" / 60.0
    END AS "Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE",
    -- Act_Allo_Hrs_perPriorWorkDays_WITH_LEAVE_KPI06:
    --   RETURN IF(Is_Date_between_Start&Today AND Task_Category="Billable Tasks",
    --     IF(Is_Workable_Day AND Is_Date_Between_Task_Days,
    --       IF(Is_Staff_Workable_DayOfWeek AND Is_Day_With_a_Leave AND NOT Is_Task_a_Leave AND NOT Is_Full_Day_Leave,
    --          Avg_Worked_Mins_perPriorDays_WITH_Leave/60, BLANK()), BLANK()), BLANK())
    CASE
        WHEN c."Date" < CURRENT_DATE
        AND k."Task_Category" = 'Billable Tasks'
        AND NOT c."WeekEnd"
        AND NOT c."PublicHoliday"
        AND COALESCE(wkd.working_day, FALSE)
        AND COALESCE(lv.has_partial_leave, FALSE)
        AND NOT COALESCE(jt."Is_Task_a_Leave", FALSE)
        AND NOT COALESCE(lv.is_full_day, FALSE) THEN jt."Avg_Worked_Mins_perPriorDays_WITH_Leave" / 60.0
    END AS "Act_Allo_Hrs_perPriorWorkDays_WITH_LEAVE_KPI06",
    -- Act_Allo_Hrs_perPriorWorkDays_WITHOUT_LEAVE_KPI07:
    --   RETURN IF(Is_Date_between_Start&Today AND Task_Category="Billable Tasks",
    --     IF(Is_Workable_Day AND Is_Date_Between_Task_Days,
    --       IF(Is_Staff_Workable_DayOfWeek AND NOT Is_Day_With_a_Leave AND NOT Is_Task_a_Leave AND NOT Is_Full_Day_Leave,
    --          Avg_Worked_Mins_perPriorDays_WITHOUT_Leave/60, BLANK()), BLANK()), BLANK())
    CASE
        WHEN c."Date" < CURRENT_DATE
        AND k."Task_Category" = 'Billable Tasks'
        AND NOT c."WeekEnd"
        AND NOT c."PublicHoliday"
        AND COALESCE(wkd.working_day, FALSE)
        AND NOT COALESCE(lv.has_partial_leave, FALSE)
        AND NOT COALESCE(jt."Is_Task_a_Leave", FALSE)
        AND NOT COALESCE(lv.is_full_day, FALSE) THEN jt."Avg_Worked_Mins_perPriorDays_WITHOUT_Leave" / 60.0
    END AS "Act_Allo_Hrs_perPriorWorkDays_WITHOUT_LEAVE_KPI07",
    -- Is_Task_WITHIN_Allo_Time: LOOKUPVALUE of Is_Task_WITHIN_Allo_Time_IMP from 1_Job_Task_Details_Table
    jt."Is_Task_WITHIN_Allo_Time_IMP" AS "Is_Task_WITHIN_Allo_Time",
    -- Is_Job_Complete: TRUE when the job's state is 'Completed' (from key06_job_table)
    (COALESCE(j6."Job_State", '') = 'Completed') AS "Is_Job_Complete",
    -- Is_Final_Invoice_Raised: TRUE when TOCHECK_JobWithFinalInvoice[Type] = 'Final Invoice' for this Job_ID
    -- DISABLED: relation "TOCHECK_JobWithFinalInvoice" does not exist yet
    -- (COALESCE(fi."Type", '') = 'Final Invoice')                       AS "Is_Final_Invoice_Raised",
    -- Recorded_Task_Hours: SUM(Recorded_Minutes)/60 from 4_Timesheet_Table for this Job_Task_Staff_ID + Date
    --   DAX: CALCULATE(SUM(Recorded_Minutes)/60, FILTER by Job_Task_Staff_ID AND Date)
    ts_task.recorded_hrs AS "Recorded_Task_Hours",
    -- Allo_Hrs_perWorkDay_Leave: hours for leave tasks (Holiday / Sick / Other) per workable day
    --   Same as KPI01 but WITHOUT the NOT is_full_day guard: leave tasks cause full-day leave for the
    --   staff on those dates, so KPI01 = NULL for all leave task rows. This column captures the
    --   allocation by using Initial_Avg_Mins_perWorkDay / 60, gated only on workday + working_day.
    --   Used by 3_Staff_Performance_Table alloc_agg for Allocated_Holiday_Hours / Allocated_Other_Leave_Hours.
    --   Date-range guard is redundant (WHERE clause guarantees it) but included for clarity.
    CASE
        WHEN COALESCE(jt."Is_Task_a_Leave", FALSE)
        AND NOT c."WeekEnd"
        AND NOT c."PublicHoliday"
        AND COALESCE(wkd.working_day, FALSE) THEN jt."Initial_Avg_Mins_perWorkDay" / 60.0
    END AS "Allo_Hrs_perWorkDay_Leave",
    -- Allo_Hrs_perWorkDay_AdjLeavesRemainDays_FIN02 = KPI04 + KPI05 (pending FIN02 DAX)
    NULL::double precision AS "Allo_Hrs_perWorkDay_AdjLeavesRemainDays_FIN02",
    -- Allo_Hrs_perWorkDay_AdjLeavesPriorDays_FIN03 = KPI06 + KPI07 (pending FIN03 DAX)
    NULL::double precision AS "Allo_Hrs_perWorkDay_AdjLeavesPriorDays_FIN03",
    -- Allo_Hrs_perWorkDay_AdjLeaves_FIN01 = FIN02 + FIN03 (pending FIN02, FIN03)
    NULL::double precision AS "Allo_Hrs_perWorkDay_AdjLeaves_FIN01",
    -- Allo_Hrs_perWorkableDay_Final_Output (pending FIN01)
    NULL::double precision AS "Allo_Hrs_perWorkableDay_Final_Output"
FROM
    key01_calendar_date c
    CROSS JOIN key02_job_task_staff_id k
    LEFT JOIN "1_Job_Task_Details_Table" jt ON jt."Job_Task_Staff_ID" = k."Job_Task_Staff_ID"
    -- wkd: DISTINCT ON matches LIMIT 1 behaviour; regular JOIN allows hash/merge join (Opt C)
    LEFT JOIN (
        SELECT DISTINCT
            ON (staffname, day_of_week) staffname,
            day_of_week,
            working_day
        FROM
            excel_workable_days
        ORDER BY
            staffname,
            day_of_week
    ) wkd ON wkd.staffname = k."Staff_Name"
    AND wkd.day_of_week = c."Weekday"
    -- lv: leave_status_by_staff_date replaces the O(n⁴) inline jobtask × nested COUNT scan (Opt C)
    LEFT JOIN leave_status_by_staff_date lv ON lv.staff_name = k."Staff_Name"
    AND lv."Date" = c."Date"
    -- j6: job state lookup for Is_Job_Complete
    -- DISTINCT ON ("Job_ID") because key06_job_table has one row per staff assignee per job
    LEFT JOIN (
        SELECT DISTINCT
            ON ("Job_ID") "Job_ID",
            "Job_State"
        FROM
            key06_job_table
        ORDER BY
            "Job_ID"
    ) j6 ON j6."Job_ID" = k."Job_ID"
    -- fi: final invoice lookup for Is_Final_Invoice_Raised
    -- DISABLED: relation "TOCHECK_JobWithFinalInvoice" does not exist yet
    -- LEFT JOIN "TOCHECK_JobWithFinalInvoice" fi ON fi."JobText" = k."Job_ID"
    -- ts_task: pre-aggregate 4_Timesheet_Table once by (Job_Task_Staff_ID, Date) for Recorded_Task_Hours
    LEFT JOIN (
        SELECT
            "Job_Task_Staff_ID",
            "Date"::date AS ts_date,
            SUM("Recorded_Minutes") / 60.0 AS recorded_hrs
        FROM
            "4_Timesheet_Table"
        GROUP BY
            "Job_Task_Staff_ID",
            "Date"::date
    ) ts_task ON ts_task."Job_Task_Staff_ID" = k."Job_Task_Staff_ID"
    AND ts_task.ts_date = c."Date"
    -- Filter to only calendar dates within each task's adjusted date range (Optimization J)
    -- Without this, the CROSS JOIN produces N_dates × N_tasks rows; the vast majority have
    -- dates outside any task's window and evaluate to all-NULL KPI columns — wasted storage
    -- and scan time. With this filter the row count drops to SUM(task_duration_in_workdays).
    -- Is_Date_Between_Task_Days is always TRUE for retained rows (semantics unchanged for
    -- downstream aggregations in 3_Staff_Performance_Table).
WHERE
    jt."StartDateAdjusted" IS NOT NULL
    AND c."Date" >= jt."StartDateAdjusted"
    AND c."Date" <= jt."DueDateAdjusted";


CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Staff_Name", "Date");


CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Job_Task_Staff_ID");


CREATE UNIQUE INDEX ON "2_Staff_Task_Allocation_byDay" ("Job_Task_Staff_ID", "Date");


DROP VIEW IF EXISTS key07_is_billable CASCADE;


CREATE OR REPLACE VIEW key07_is_billable AS
SELECT DISTINCT
    "Billable_Selector"
FROM
    "2_Staff_Task_Allocation_byDay";


-- 3_Staff_Performance_Table (Optimizations D, E, F, G, H, K)
-- D: Converted to MATERIALIZED VIEW with (Staff_Name, Date) index.
-- E: Is_Workable_Day NOT EXISTS on 1_Job_Task_Details_Table replaced with
--    leave_status_by_staff_date JOIN (lv_spt) — same pre-computed materialized view.
-- F: Two excel_workable_days scalar subqueries merged into one LEFT JOIN (wd_lkp).
--    excel_staff_adjustment_sheet scalar subquery merged into one LEFT JOIN (adj_lkp).
-- G: itgt and pitgt LATERAL+LIMIT 1 replaced with regular LEFT JOINs — allows hash joins.
-- H: 6 separate 2_Staff_Task_Allocation_byDay scans → 1 alloc_counts FILTER lateral (6× reduction).
--    6 separate 4_Timesheet_Table scans → 1 ts_counts FILTER lateral (6× reduction).
--    Note: original alr/olr used Staff_ID=Staff_UUID; unified to Staff_Name for consistency
--    since 4_Timesheet_Table requires Staff_Name IS NOT NULL.
-- Refresh order: 2_Staff_Task_Allocation_byDay → 4_Timesheet_Table → 3_Staff_Performance_Table
DROP MATERIALIZED VIEW IF EXISTS "3_Staff_Performance_Table" CASCADE;


CREATE MATERIALIZED VIEW "3_Staff_Performance_Table" AS
SELECT
    c.*,
    s.*,
    -- Is_Workable_Day
    -- DAX: IF(AND(Is_PublicHoliday=FALSE,Is_WeekEnd=FALSE), IF(Is_Full_Day_Leave=FALSE,TRUE,FALSE))
    -- DAX outer IF has no else → BLANK on holiday/weekend, but correct value is FALSE
    (
        NOT c."PublicHoliday"
        AND NOT c."WeekEnd"
        AND NOT COALESCE(lv_spt.is_full_day, FALSE)
    ) AS "Is_Workable_Day",
    -- Is_Staff_Workable_DayOfWeek: merged from scalar subquery into wd_lkp JOIN (Opt F)
    wd_lkp.working_day AS "Is_Staff_Workable_DayOfWeek",
    -- Adjustment_Factor_by_Month: merged from scalar subquery into adj_lkp JOIN (Opt F)
    COALESCE(adj_lkp.adjustmentfactor, 0) AS "Adjustment_Factor_by_Month",
    -- Week_Of_Month: 1 + WEEKNUM(Date) - WEEKNUM(STARTOFMONTH(Date))
    -- DAX WEEKNUM(date, 1): week starts Sunday; week 1 = partial week containing Jan 1.
    -- Formula: CEIL((DOY + DOW_of_Jan1) / 7) where DOW uses EXTRACT(DOW) (0=Sun … 6=Sat).
    -- TO_CHAR('WW') was wrong: it treats Jan 1–7 always as week 1, ignoring the start-day.
    -- Example: Jan 2022 — Jan 1=Sat, so DAX week 1 = just Jan 1; Jan 2 (Sun) starts week 2.
    --          TO_CHAR gives Jan 5 = WW 1 (wrong); this formula gives CEIL((5+6)/7)=2 (correct).
    1 + CEIL(
        (
            EXTRACT(
                DOY
                FROM
                    c."Date"
            )::float + EXTRACT(
                DOW
                FROM
                    DATE_TRUNC('year', c."Date")
            )::float
        ) / 7.0
    )::int - CEIL(
        (
            EXTRACT(
                DOY
                FROM
                    DATE_TRUNC('month', c."Date")::date
            )::float + EXTRACT(
                DOW
                FROM
                    DATE_TRUNC('year', c."Date")
            )::float
        ) / 7.0
    )::int AS "Week_Of_Month",
    -- Overall_Recordable_Hours: placeholder — update orh LATERAL when DAX is provided
    orh.val AS "Overall_Recordable_Hours",
    -- Allocated_Holiday_Hours: SUM(Allo_Hrs_perWorkableDay_Final_Output) for Holiday tasks * -1
    --   Guard: IF(Overall_Recordable_Hours=0, BLANK(), ...)
    --   COALESCE hol_hrs to 0: no holiday rows → LEFT JOIN gives NULL → -NULL = NULL; DAX BLANK() = 0 in arithmetic
    CASE
        WHEN orh.val = 0 THEN NULL
        ELSE - COALESCE(alloc_agg.hol_hrs, 0)
    END AS "Allocated_Holiday_Hours",
    -- Allocated_Other_Leave_Hours: SUM for Sick leave / Other leave tasks * -1
    --   Same COALESCE fix as Allocated_Holiday_Hours
    CASE
        WHEN orh.val = 0 THEN NULL
        ELSE - COALESCE(alloc_agg.lvl_hrs, 0)
    END AS "Allocated_Other_Leave_Hours",
    -- Non_Leave_Recordable_Hours: IF(Overall=0, 0, Overall + Holiday + OtherLeave)
    --   Holiday and OtherLeave are already negative, BLANK()→0 in DAX addition
    CASE
        WHEN orh.val = 0 THEN 0
        ELSE orh.val + COALESCE(- alloc_agg.hol_hrs, 0) + COALESCE(- alloc_agg.lvl_hrs, 0)
    END AS "Non_Leave_Recordable_Hours",
    -- Allocated_Dinniss_Admin_Hours: SUM for Task_Category="Admin Tasks" (no * -1)
    CASE
        WHEN orh.val = 0 THEN NULL
        ELSE alloc_agg.adm_hrs
    END AS "Allocated_Dinniss_Admin_Hours",
    -- Available_Billable_Hours = Non_Leave_Recordable_Hours - Allocated_Dinniss_Admin_Hours
    --   Guard: IF(Overall=0, 0, Logic)
    CASE
        WHEN orh.val = 0 THEN 0
        ELSE orh.val + COALESCE(- alloc_agg.hol_hrs, 0) + COALESCE(- alloc_agg.lvl_hrs, 0) - COALESCE(alloc_agg.adm_hrs, 0)
    END AS "Available_Billable_Hours",
    -- Allocated_Billable_Hours_Original: SUM(Allo_Hrs_perWorkDay_AdjLeaves_FIN01) for Billable Tasks
    --   Guard: IF(Overall=0, BLANK(), ...)
    CASE
        WHEN orh.val = 0 THEN NULL
        ELSE alloc_agg.bil_orig_hrs
    END AS "Allocated_Billable_Hours_Original",
    -- Allocated_Billable_Hours_Capacity_Planning: SUM(Allo_Hrs_perWorkableDay_Final_Output) for Billable Tasks
    --   Adjusted for task hours worked/remaining
    CASE
        WHEN orh.val = 0 THEN NULL
        ELSE alloc_agg.bil_cap_hrs
    END AS "Allocated_Billable_Hours_Capacity_Planning",
    -- Target_Billable_Hours: Available_Billable_Hours * LOOKUPVALUE(excel_incentive_targets[target_billable_hours], staff_name, StartOfMonth=month_year)
    CASE
        WHEN orh.val = 0 THEN 0
        ELSE (
            orh.val + COALESCE(- alloc_agg.hol_hrs, 0) + COALESCE(- alloc_agg.lvl_hrs, 0) - COALESCE(alloc_agg.adm_hrs, 0)
        ) * itgt.target_billable_hours
    END AS "Target_Billable_Hours",
    -- Target_Non_Leave_Hours: Non_Leave_Recordable_Hours * target_billable_hours; IF(Logic<=0, BLANK(), Logic)
    CASE
        WHEN tnl.val <= 0 THEN NULL
        ELSE tnl.val
    END AS "Target_Non_Leave_Hours",
    -- M2Date_Overall_Recordable_Hours: BLANK() if Date >= TODAY (future/today), else Overall_Recordable_Hours
    CASE
        WHEN c."Date" >= CURRENT_DATE THEN NULL
        ELSE orh.val
    END AS "M2Date_Overall_Recordable_Hours",
    -- M2Date_Target_Billable_Hours: BLANK() if Date >= TODAY, else Target_Billable_Hours
    CASE
        WHEN c."Date" >= CURRENT_DATE THEN NULL
        WHEN orh.val = 0 THEN 0
        ELSE (
            orh.val + COALESCE(- alloc_agg.hol_hrs, 0) + COALESCE(- alloc_agg.lvl_hrs, 0) - COALESCE(alloc_agg.adm_hrs, 0)
        ) * itgt.target_billable_hours
    END AS "M2Date_Target_Billable_Hours",
    -- Annual_Leave_Hours_Recorded: SUM(Recorded_Minutes/60) from 4_Timesheet_Table for Holiday tasks * -1
    - ts_agg.alr_mins / 60.0 AS "Annual_Leave_Hours_Recorded",
    -- Other_Leave_Hours_Recorded: SUM(Recorded_Minutes/60) for Sick leave / Other leave tasks * -1
    - ts_agg.olr_mins / 60.0 AS "Other_Leave_Hours_Recorded",
    -- Dinniss_Admin_Hours_Recorded: SUM(Recorded_Minutes/60) for Admin Tasks (positive, no * -1)
    ts_agg.dar_mins / 60.0 AS "Dinniss_Admin_Hours_Recorded",
    -- Billable_Hours_Recorded: SUM(Recorded_Minutes/60) for Billable Tasks (positive, no * -1)
    ts_agg.bhr_mins / 60.0 AS "Billable_Hours_Recorded",
    -- Overall_Hours_Recorded: (Annual + Other) * -1 + Billable + Admin
    --   Annual and Other are already negative, so * -1 makes them positive: = |Annual| + |Other| + Billable + Admin
    COALESCE(ts_agg.alr_mins, 0) / 60.0 + COALESCE(ts_agg.olr_mins, 0) / 60.0 + COALESCE(ts_agg.bhr_mins, 0) / 60.0 + COALESCE(ts_agg.dar_mins, 0) / 60.0 AS "Overall_Hours_Recorded",
    -- Billable_Hours_Invoiced: SUM(Invoiced_Minutes/60) where Invoiced_Time="Invoiced" AND Billable Tasks
    ts_agg.bhi_mins / 60.0 AS "Billable_Hours_Invoiced",
    -- Billable_Hours_To_Be_Invoiced: SUM(Recorded_Minutes/60) where Invoiced_Time="Un-Invoiced" AND Billable Tasks
    ts_agg.btu_mins / 60.0 AS "Billable_Hours_To_Be_Invoiced",
    -- M2Date_Overall_Hours_Recorded: BLANK() if Date >= TODAY, else Overall_Hours_Recorded
    CASE
        WHEN c."Date" >= CURRENT_DATE THEN NULL
        ELSE COALESCE(ts_agg.alr_mins, 0) / 60.0 + COALESCE(ts_agg.olr_mins, 0) / 60.0 + COALESCE(ts_agg.bhr_mins, 0) / 60.0 + COALESCE(ts_agg.dar_mins, 0) / 60.0
    END AS "M2Date_Overall_Hours_Recorded",
    -- Allocated_Non_Leave_Hours_Capacity_Planning_CHECK_VARIABLES:
    --   SUM(Allo_Hrs_perWorkableDay_Final_Output) for Admin OR Billable Tasks
    --   Guard: IF(Overall_Recordable_Hours=0, BLANK(), ...)
    CASE
        WHEN orh.val = 0 THEN NULL
        ELSE alloc_agg.anl_hrs
    END AS "Allocated_Non_Leave_Hours_Capacity_Planning_CHECK_VARIABLES",
    -- Adjustment_Factor_by_Day: merged from scalar subquery into wd_lkp JOIN (Opt F)
    COALESCE(wd_lkp.adjustment_factor, 0) AS "Adjustment_Factor_by_Day",
    -- Is_Current_Month: Date falls in the same year+month as TODAY
    (
        EXTRACT(
            YEAR
            FROM
                c."Date"
        ) = EXTRACT(
            YEAR
            FROM
                CURRENT_DATE
        )
        AND EXTRACT(
            MONTH
            FROM
                c."Date"
        ) = EXTRACT(
            MONTH
            FROM
                CURRENT_DATE
        )
    ) AS "Is_Current_Month",
    -- Target_Hours_to_be_Recorded: Target_Billable_Hours * target_recorded_2_billable_hrs; IF(Logic<=0, BLANK(), Logic)
    CASE
        WHEN trec.val <= 0 THEN NULL
        ELSE trec.val
    END AS "Target_Hours_to_be_Recorded",
    -- Target_Hours_to_be_Allocated: Target_Billable_Hours * target_allocated_2_billable_hrs; IF(Logic<=0, BLANK(), Logic)
    CASE
        WHEN talloc.val <= 0 THEN NULL
        ELSE talloc.val
    END AS "Target_Hours_to_be_Allocated",
    -- Target_Hours_to_be_Invoiced: Target_Billable_Hours * target_invoiced_2_billable_hrs; IF(Logic<=0, BLANK(), Logic)
    CASE
        WHEN tinv.val <= 0 THEN NULL
        ELSE tinv.val
    END AS "Target_Hours_to_be_Invoiced",
    -- Target_Billable_Percent: LOOKUPVALUE(target_billable_hours, month_year=Date); IF(=0, BLANK(), val)
    --   Note: month_year is first-of-month, so only matches when Date = 1st of month
    NULLIF(pitgt.target_billable_hours, 0) AS "Target_Billable_Percent",
    -- Target_Recordable_Percent: LOOKUPVALUE(target_recorded_2_billable_hrs, month_year=Date)
    NULLIF(pitgt.target_recorded_2_billable_hrs, 0) AS "Target_Recordable Percent",
    -- Target_Allocation_Percent: LOOKUPVALUE(target_allocated_2_billable_hrs, month_year=Date)
    NULLIF(pitgt.target_allocated_2_billable_hrs, 0) AS "Target_Allocation_Percent",
    -- Target_Invoice_Percent: LOOKUPVALUE(target_invoiced_2_billable_hrs, month_year=Date)
    NULLIF(pitgt.target_invoiced_2_billable_hrs, 0) AS "Target_Invoice_Percent"
FROM
    key01_calendar_date c
    CROSS JOIN key03_staff_table s
    -- lv_spt: Is_Workable_Day full-day leave flag; replaces NOT EXISTS on 1_Job_Task_Details_Table (Opt E)
    LEFT JOIN leave_status_by_staff_date lv_spt ON lv_spt.staff_name = s."Staff_Name"
    AND lv_spt."Date" = c."Date"
    -- wd_lkp: merges Is_Staff_Workable_DayOfWeek + Adjustment_Factor_by_Day into one JOIN (Opt F)
    LEFT JOIN (
        SELECT DISTINCT
            ON (staffname, day_of_week) staffname,
            day_of_week,
            working_day,
            adjustment_factor
        FROM
            excel_workable_days
        ORDER BY
            staffname,
            day_of_week
    ) wd_lkp ON wd_lkp.staffname = s."Staff_Name"
    AND wd_lkp.day_of_week = c."Weekday"
    -- adj_lkp: merges Adjustment_Factor_by_Month scalar subquery into one JOIN (Opt F)
    LEFT JOIN (
        SELECT DISTINCT
            ON (staffname, MONTH::date) staffname,
            MONTH::date AS month_date,
            adjustmentfactor
        FROM
            excel_staff_adjustment_sheet
        ORDER BY
            staffname,
            MONTH::date
    ) adj_lkp ON adj_lkp.staffname = s."Staff_Name"
    AND adj_lkp.month_date = c."StartOfMonth"
    CROSS JOIN LATERAL (
        -- orh: Overall_Recordable_Hours
        -- DAX: IF(AND(Is_Workable_Day=TRUE(), Is_Staff_Workable_DayOfWeek=TRUE()), 8*Adjustment_Factor_by_Month, 0)
        SELECT
            CASE
                WHEN NOT c."PublicHoliday"
                AND NOT c."WeekEnd"
                AND NOT COALESCE(lv_spt.is_full_day, FALSE)
                AND COALESCE(wd_lkp.working_day, FALSE) THEN ROUND(8.0 * COALESCE(adj_lkp.adjustmentfactor, 0))
                ELSE 0
            END AS val
    ) orh
    -- alloc_agg: pre-aggregate 2_Staff_Task_Allocation_byDay once by (Staff_Name, Date),
    -- then hash-join to the outer loop — replaces ~44,000 individual nested-loop aggregate
    -- scans from the previous CROSS JOIN LATERAL alloc_counts pattern (Opt K)
    LEFT JOIN (
        SELECT
            a."Staff_Name",
            a."Date",
            -- hol_hrs / lvl_hrs: leave tasks have NULL for all KPI columns (KPI01 guards NOT is_full_day,
            -- KPI02/03 guard Is_Task_a_Leave=FALSE, KPI04-07 guard Billable Tasks only).
            -- Use Allo_Hrs_perWorkDay_Leave which is KPI01 without the is_full_day guard.
            SUM(a."Allo_Hrs_perWorkDay_Leave") FILTER (
                WHERE
                    a."Task_Name" ILIKE '%Holiday%'
            ) AS hol_hrs,
            SUM(a."Allo_Hrs_perWorkDay_Leave") FILTER (
                WHERE
                    a."Task_Name" ILIKE '%Sick leave%'
                    OR a."Task_Name" ILIKE '%Other leave%'
            ) AS lvl_hrs,
            SUM(a."Allo_Hrs_perWorkableDay_Final_Output") FILTER (
                WHERE
                    a."Task_Category" = 'Admin Tasks'
            ) AS adm_hrs,
            SUM(a."Allo_Hrs_perWorkDay_AdjLeaves_FIN01") FILTER (
                WHERE
                    a."Task_Category" = 'Billable Tasks'
            ) AS bil_orig_hrs,
            SUM(a."Allo_Hrs_perWorkableDay_Final_Output") FILTER (
                WHERE
                    a."Task_Category" = 'Billable Tasks'
            ) AS bil_cap_hrs,
            SUM(a."Allo_Hrs_perWorkableDay_Final_Output") FILTER (
                WHERE
                    a."Task_Category" IN ('Admin Tasks', 'Billable Tasks')
            ) AS anl_hrs
        FROM
            "2_Staff_Task_Allocation_byDay" a
        GROUP BY
            a."Staff_Name",
            a."Date"
    ) alloc_agg ON alloc_agg."Staff_Name" = s."Staff_Name"
    AND alloc_agg."Date" = c."Date"
    -- itgt: regular LEFT JOIN replaces LATERAL+LIMIT 1 — allows hash/merge join (Opt G)
    LEFT JOIN (
        SELECT DISTINCT
            ON (staff_name, month_year::date) staff_name,
            month_year::date AS month_date,
            target_billable_hours,
            target_recorded_2_billable_hrs,
            target_allocated_2_billable_hrs,
            target_invoiced_2_billable_hrs
        FROM
            excel_incentive_targets
        ORDER BY
            staff_name,
            month_year::date
    ) itgt ON itgt.staff_name = s."Staff_Name"
    AND itgt.month_date = c."StartOfMonth"
    CROSS JOIN LATERAL (
        -- tnl: Non_Leave_Recordable_Hours * target_billable_hours (pre-computed for Target_Non_Leave_Hours guard)
        SELECT
            (
                CASE
                    WHEN orh.val = 0 THEN 0
                    ELSE orh.val + COALESCE(- alloc_agg.hol_hrs, 0) + COALESCE(- alloc_agg.lvl_hrs, 0)
                END
            ) * itgt.target_billable_hours AS val
    ) tnl
    CROSS JOIN LATERAL (
        -- trec: Target_Billable_Hours * target_recorded_2_billable_hrs (for Target_Hours_to_be_Recorded guard)
        SELECT
            (
                CASE
                    WHEN orh.val = 0 THEN 0
                    ELSE (
                        orh.val + COALESCE(- alloc_agg.hol_hrs, 0) + COALESCE(- alloc_agg.lvl_hrs, 0) - COALESCE(alloc_agg.adm_hrs, 0)
                    ) * itgt.target_billable_hours
                END
            ) * itgt.target_recorded_2_billable_hrs AS val
    ) trec
    CROSS JOIN LATERAL (
        -- talloc: Target_Billable_Hours * target_allocated_2_billable_hrs (for Target_Hours_to_be_Allocated guard)
        SELECT
            (
                CASE
                    WHEN orh.val = 0 THEN 0
                    ELSE (
                        orh.val + COALESCE(- alloc_agg.hol_hrs, 0) + COALESCE(- alloc_agg.lvl_hrs, 0) - COALESCE(alloc_agg.adm_hrs, 0)
                    ) * itgt.target_billable_hours
                END
            ) * itgt.target_allocated_2_billable_hrs AS val
    ) talloc
    CROSS JOIN LATERAL (
        -- tinv: Target_Billable_Hours * target_invoiced_2_billable_hrs (for Target_Hours_to_be_Invoiced guard)
        SELECT
            (
                CASE
                    WHEN orh.val = 0 THEN 0
                    ELSE (
                        orh.val + COALESCE(- alloc_agg.hol_hrs, 0) + COALESCE(- alloc_agg.lvl_hrs, 0) - COALESCE(alloc_agg.adm_hrs, 0)
                    ) * itgt.target_billable_hours
                END
            ) * itgt.target_invoiced_2_billable_hrs AS val
    ) tinv
    -- pitgt: regular LEFT JOIN replaces LATERAL+LIMIT 1 — allows hash/merge join (Opt G)
    --   Matches only when Date = first of month (month_year is always first-of-month)
    LEFT JOIN (
        SELECT DISTINCT
            ON (staff_name, month_year::date) staff_name,
            month_year::date AS month_date,
            target_billable_hours,
            target_recorded_2_billable_hrs,
            target_allocated_2_billable_hrs,
            target_invoiced_2_billable_hrs
        FROM
            excel_incentive_targets
        ORDER BY
            staff_name,
            month_year::date
    ) pitgt ON pitgt.staff_name = s."Staff_Name"
    AND pitgt.month_date = c."Date"
    -- ts_agg: pre-aggregate 4_Timesheet_Table once by (Staff_Name, Date),
    -- then hash-join to the outer loop — replaces ~44,000 individual nested-loop aggregate
    -- scans from the previous CROSS JOIN LATERAL ts_counts pattern (Opt K)
    LEFT JOIN (
        SELECT
            ts."Staff_Name",
            ts."Date"::date AS ts_date,
            SUM(ts."Recorded_Minutes") FILTER (
                WHERE
                    ts."Task_Name" ILIKE '%Holiday%'
            ) AS alr_mins,
            SUM(ts."Recorded_Minutes") FILTER (
                WHERE
                    ts."Task_Name" ILIKE '%Sick leave%'
                    OR ts."Task_Name" ILIKE '%Other leave%'
            ) AS olr_mins,
            SUM(ts."Recorded_Minutes") FILTER (
                WHERE
                    ts."Task_Category" = 'Admin Tasks'
            ) AS dar_mins,
            SUM(ts."Recorded_Minutes") FILTER (
                WHERE
                    ts."Task_Category" = 'Billable Tasks'
            ) AS bhr_mins,
            SUM(ts."Invoiced_Minutes") FILTER (
                WHERE
                    ts."Invoiced_Time" = 'Invoiced'
                    AND ts."Task_Category" = 'Billable Tasks'
            ) AS bhi_mins,
            SUM(ts."Recorded_Minutes") FILTER (
                WHERE
                    ts."Invoiced_Time" = 'Un-Invoiced'
                    AND ts."Task_Category" = 'Billable Tasks'
            ) AS btu_mins
        FROM
            "4_Timesheet_Table" ts
        GROUP BY
            ts."Staff_Name",
            ts."Date"::date
    ) ts_agg ON ts_agg."Staff_Name" = s."Staff_Name"
    AND ts_agg.ts_date = c."Date";


CREATE UNIQUE INDEX ON "3_Staff_Performance_Table" ("Staff_Name", "Date");