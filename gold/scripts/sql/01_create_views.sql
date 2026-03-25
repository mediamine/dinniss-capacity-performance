-- independent key views
DROP VIEW IF EXISTS key01_calendar_date CASCADE;


CREATE OR REPLACE VIEW key01_calendar_date AS
SELECT
    gs::DATE AS "Date",
    EXISTS (
        SELECT
            1
        FROM
            excel_public_holidays eph
        WHERE
            eph.DATE = gs::DATE
    ) AS "PublicHoliday",
    EXTRACT(
        DOW
        FROM
            gs
    )::INT + 1 AS "Weekday",
    (
        EXTRACT(
            DOW
            FROM
                gs
        )::INT + 1
    ) < 2
    OR (
        EXTRACT(
            DOW
            FROM
                gs
        )::INT + 1
    ) > 6 AS "WeekEnd",
    DATE_TRUNC('month', gs)::DATE AS "StartOfMonth",
    (
        DATE_TRUNC('month', gs) + INTERVAL '1 month' - INTERVAL '1 day'
    )::DATE AS "EndOfMonth",
    (
        (
            DATE_TRUNC('month', gs) + INTERVAL '1 month' - INTERVAL '1 day'
        )::DATE <= (
            DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day'
        )::DATE
        AND (
            DATE_TRUNC('month', gs) + INTERVAL '1 month' - INTERVAL '1 day'
        )::DATE > '2020-04-30'::DATE
    ) AS "Is_Range_for_Invoicing"
FROM
    generate_series(
        '2020-01-01'::DATE,
        (
            DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '4 months' - INTERVAL '1 day'
        )::DATE,
        '1 day'::INTERVAL
    ) gs;


DROP VIEW IF EXISTS key08_incentive_table_display_measure CASCADE;


CREATE OR REPLACE VIEW key08_incentive_table_display_measure AS
SELECT
    metric AS "Metric",
    'Selected Metric: ' || SUBSTRING(
        metric
        FROM
            4
    ) AS "Name",
    CASE metric
        WHEN '1. Allocated Billable Hours' THEN 'Target Metric: Hours to be Allocated'
        WHEN '2. Recorded Billable Hours' THEN 'Target Metric: Hours to be Recorded'
        WHEN '3. Invoiced Billable Hours' THEN 'Target Metric: Hours to be Invoiced'
        WHEN '4. % Allocated to Billable Hours' THEN 'Target Metric: Hours to be Allocated'
        WHEN '5. % Recorded to Billable Hours' THEN 'Target Metric: Hours to be Recorded'
        ELSE 'Target Metric: Hours to be Invoiced'
    END AS "Target_Metric"
FROM
    unnest(
        ARRAY[
            '1. Allocated Billable Hours',
            '2. Recorded Billable Hours',
            '3. Invoiced Billable Hours',
            '4. % Allocated to Billable Hours',
            '5. % Recorded to Billable Hours',
            '6. % Invoiced to Billable Hours'
        ]
    ) AS metric;


-- key views dependent on another table or view
DROP VIEW IF EXISTS key03_staff_table CASCADE;


CREATE OR REPLACE VIEW key03_staff_table AS
SELECT DISTINCT
    "StaffID" AS "Staff_UUID",
    "Name" AS "Staff_Name"
FROM
    jobtaskassignee
WHERE
    "Name" NOT IN (
        'Anna Williams',
        'Caroline Dinniss',
        'Conor Cameron',
        'Conor O''Brien',
        'Dinniss',
        'Sahar Sedaghat',
        'The OLD - Dani Millar',
        'Vicky Jones'
    )
    AND "Name" IS NOT NULL;


DROP VIEW IF EXISTS key05_task_type CASCADE;


CREATE OR REPLACE VIEW key05_task_type AS
SELECT
    "UUID" AS "TaskType_UUID",
    "Name"
FROM
    task;


DROP VIEW IF EXISTS key04_task_name CASCADE;


CREATE OR REPLACE VIEW key04_task_name AS
SELECT
    "Task_Name",
    "Task_Type1",
    -- Task_Type: IF Task_Name is a special online content task keep the name, else use Task_Type1
    --   DAX: IF([Task_Name]="Online content creation",[Task_Name], IF([Task_Name]="Online content management",[Task_Name],[Task_Type1]))
    CASE
        WHEN "Task_Name" = 'Online content creation' THEN "Task_Name"
        WHEN "Task_Name" = 'Online content management' THEN "Task_Name"
        ELSE "Task_Type1"
    END AS "Task_Type"
FROM (
    SELECT DISTINCT
        jt."Name" AS "Task_Name",
        -- Task_Type1: CONCATENATEX over key05_task_type, collecting type names found (via SEARCH/ILIKE) in the task name
        --   DAX: CONCATENATEX(KEY05_Task_Type, IF(SEARCH(key05[Name], task_name,,999)<>999, key05[Name]," "))
        COALESCE(
            NULLIF(
                TRIM(
                    (
                        SELECT STRING_AGG(kt."Name", ' ')
                        FROM key05_task_type kt
                        WHERE jt."Name" ILIKE ('%' || kt."Name" || '%')
                    )
                ),
                ''
            ),
            jt."Name"
        ) AS "Task_Type1"
    FROM
        jobtask jt
) sub;


DROP VIEW IF EXISTS key06_job_table CASCADE;


CREATE OR REPLACE VIEW key06_job_table AS
SELECT
    jd."UUID" AS "Job_UUID",
    jd."ID" AS "Job_ID",
    jd."Name" AS "Job_Name",
    jd."Description" AS "Job_Description",
    jd."ClientUUID" AS "Client_UUID",
    cd."Name" AS "Client_Name",
    ja."UUID" AS "Staff_UUID",
    ja."Name" AS "Staff_Name",
    jd."State" AS "Job_State",
    jd."StartDate",
    jd."DueDate",
    jd."CompletedDate"
FROM
    jobdetails jd
    LEFT JOIN clientdetails cd ON cd."UUID" = jd."ClientUUID"::uuid
    LEFT JOIN jobassignee ja ON ja."JobID" = jd."RemoteID";


-- views dependent on another table or view
DROP VIEW IF EXISTS "1_Job_Task_Details_Table" CASCADE;


CREATE OR REPLACE VIEW "1_Job_Task_Details_Table" AS
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
    wdb.cnt                                                          AS "Workable_Days_Between_Task",
    -- Workable_Hrs_Between_Task = Workable_Days_Between_Task * 8
    wdb.cnt * 8                                                      AS "Workable_Hrs_Between_Task",
    -- Initial_Avg_Mins_perWorkDay = DIVIDE(Task_Allocated_Mins, Workable_Days_Between_Task, BLANK())
    b."Task_Allocated_Mins"::float / NULLIF(wdb.cnt, 0)              AS "Initial_Avg_Mins_perWorkDay",
    -- Total_Leave_Hrs_between_Workable_Days: only for non-leave tasks
    --   DAX: IF(Is_Task_a_Leave=FALSE, CALCULATE(SUM([Initial_Allo_Hrs_perWorkDay_KPI01]),
    --            FILTER(Staff_Name matches), FILTER(Task_Category="Leave Tasks"),
    --            FILTER(Is_Full_Day_Leave=FALSE), DATESBETWEEN(Date, StartDateAdjusted, DueDateAdjusted)), BLANK())
    CASE WHEN NOT b."Is_Task_a_Leave" THEN tlh.hrs END               AS "Total_Leave_Hrs_between_Workable_Days",
    -- Rev_Workable_Days_Between_Task = (Workable_Hrs - Total_Leave_Hrs) / 8 (only for non-leave tasks)
    --   DAX: IF(Is_Task_a_Leave=FALSE, (Workable_Hrs_Between_Task - Total_Leave_Hrs_between_Workable_Days) / 8, BLANK())
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN (wdb.cnt * 8 - COALESCE(tlh.hrs, 0)) / 8.0
    END                                                               AS "Rev_Workable_Days_Between_Task",
    -- Avg_Mins_perWorkDay_WITHOUT_Leave = DIVIDE(Task_Allocated_Mins, Rev_Workable_Days_Between_Task, BLANK()) when not leave
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN b."Task_Allocated_Mins"::float / NULLIF((wdb.cnt * 8 - COALESCE(tlh.hrs, 0)) / 8.0, 0)
    END                                                               AS "Avg_Mins_perWorkDay_WITHOUT_Leave",
    -- Total_Task_Mins_WorkDays_WITHOUT_Leave = SUM(Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02)*60
    --   = COUNT(non-leave workable days) * Avg_Mins_perWorkDay_WITHOUT_Leave
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN ttl.cnt::float * b."Task_Allocated_Mins"::float / NULLIF((wdb.cnt * 8 - COALESCE(tlh.hrs, 0)) / 8.0, 0)
    END                                                               AS "Total_Task_Mins_WorkDays_WITHOUT_Leave",
    -- Remaining_Allocated_Task_Mins = Task_Allocated_Mins - Total_Task_Mins_WorkDays_WITHOUT_Leave
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN b."Task_Allocated_Mins" - ttl.cnt::float * b."Task_Allocated_Mins"::float / NULLIF((wdb.cnt * 8 - COALESCE(tlh.hrs, 0)) / 8.0, 0)
    END                                                               AS "Remaining_Allocated_Task_Mins",
    -- WorkDays_WITH_Leaves_between_Task: workable days with partial (not full-day) leave
    CASE WHEN NOT b."Is_Task_a_Leave" THEN wdl.cnt END                AS "WorkDays_WITH_Leaves_between_Task",
    -- Avg_Mins_perWorkDay_WITH_Leaves = DIVIDE(Remaining_Allocated_Task_Mins, WorkDays_WITH_Leaves_between_Task, BLANK())
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN (b."Task_Allocated_Mins" - ttl.cnt::float * b."Task_Allocated_Mins"::float / NULLIF((wdb.cnt * 8 - COALESCE(tlh.hrs, 0)) / 8.0, 0))
              / NULLIF(wdl.cnt, 0)
    END                                                               AS "Avg_Mins_perWorkDay_WITH_Leaves",
    -- Task_Mins_Worked_Till_Date = SUM(Recorded_Minutes) from 4_Timesheet_Table for this Job_Task_Staff_ID
    CASE WHEN NOT b."Is_Task_a_Leave" THEN tmt.recorded_mins END      AS "Task_Mins_Worked_Till_Date",
    -- IS_Task_Mins_Worked_>_Allocated = Task_Mins_Worked_Till_Date > Task_Allocated_Mins
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN tmt.recorded_mins > b."Task_Allocated_Mins"
    END                                                               AS "IS_Task_Mins_Worked_>_Allocated",
    -- Task_Mins_Remain_until_Due = IF(IS_Task_Mins_Worked_>_Allocated, 0, Task_Allocated_Mins - Task_Mins_Worked_Till_Date)
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN GREATEST(0, b."Task_Allocated_Mins" - tmt.recorded_mins)
    END                                                               AS "Task_Mins_Remain_until_Due",
    -- Allo_Mins_during_Remaining_workDays_WITH_leave
    --   = SUM(Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04)*60 for this Job_Task_Staff_ID
    --   = COUNT(remaining days with partial leave) * Avg_Mins_perWorkDay_WITH_Leaves
    --   RETURN condition: Is_Date_between_Today&Due=TRUE AND Task_Category="Billable Tasks"
    --   Is_Date_between_Today&Due = Date >= TODAY AND Is_Date_Between_Task_Days
    CASE WHEN NOT b."Is_Task_a_Leave"
              AND NOT (b."Task_Name" ILIKE '%Admin - Non-billable%' OR b."Client_Name" = 'Dinniss Admin')
         THEN arwd.cnt::float
              * (b."Task_Allocated_Mins" - ttl.cnt::float * b."Task_Allocated_Mins"::float / NULLIF((wdb.cnt * 8 - COALESCE(tlh.hrs, 0)) / 8.0, 0))
              / NULLIF(wdl.cnt, 0)
    END                                                               AS "Allo_Mins_during_Remaining_workDays_WITH_leave",
    -- Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave = Task_Mins_Remain_until_Due - Allo_Mins_during_Remaining_workDays_WITH_leave
    --   DAX treats BLANK as 0 in arithmetic, so Allo_Mins is 0 for admin/non-billable tasks
    CASE WHEN NOT b."Is_Task_a_Leave" THEN rmn.remain_mins END        AS "Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave",
    -- Remain_WorkDays_WITHOUT_Leave: remaining workable days without any leave (TODAY to DueDateAdjusted)
    --   DAX: DATESBETWEEN(Date, TODAY(), DueDateAdjusted) AND Is_Day_With_a_Leave=FALSE AND Is_Workable_Day AND Is_Staff_Workable_DayOfWeek
    CASE WHEN NOT b."Is_Task_a_Leave" THEN rttl.cnt END               AS "Remain_WorkDays_WITHOUT_Leave",
    -- Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave = DIVIDE(Remain_Mins_Allo, Remain_WorkDays_WITHOUT_Leave, BLANK())
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN rmn.remain_mins / NULLIF(rttl.cnt, 0)
    END                                                               AS "Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave",
    -- Is_Task_WITHIN_Allo_Time_IMP:
    --   IF(Remain_Mins > 0, IF(Remain_WorkDays >= 1 AND Avg_Remain_Mins <= 480, TRUE, FALSE), TRUE)
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN CASE
                  WHEN rmn.remain_mins > 0
                  THEN rttl.cnt >= 1
                       AND rmn.remain_mins / NULLIF(rttl.cnt, 0) <= 480
                  ELSE TRUE
              END
    END                                                               AS "Is_Task_WITHIN_Allo_Time_IMP",
    -- Is_Task_DueDate_Over = DueDateAdjusted < TODAY()
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN b."DueDateAdjusted" < CURRENT_DATE
    END                                                               AS "Is_Task_DueDate_Over",
    -- Task_Mins_Worked_Adjusted = IF(IS_Task_Mins_Worked_>_Allocated OR Is_Task_DueDate_Over, Task_Allocated_Mins, Task_Mins_Worked_Till_Date)
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN CASE WHEN tmt.recorded_mins > b."Task_Allocated_Mins"
                        OR b."DueDateAdjusted" < CURRENT_DATE
                   THEN b."Task_Allocated_Mins"
                   ELSE tmt.recorded_mins
              END
    END                                                               AS "Task_Mins_Worked_Adjusted",
    -- Prior_WorkDays_WITH_Leave: workable days with partial leave from StartDateAdjusted to TODAY
    --   DAX: Is_Date_Between_Task_Days AND Is_Date_between_Start&Today (Date<=TODAY) AND Is_Workable_Day
    --        AND Is_Day_With_a_Leave=TRUE AND Is_Staff_Workable_DayOfWeek AND Is_Full_Day_Leave=FALSE
    CASE WHEN NOT b."Is_Task_a_Leave" THEN pwdl.cnt END               AS "Prior_WorkDays_WITH_Leave",
    -- Prior_WorkDays_WITHOUT_Leave: workable days without any leave from StartDateAdjusted to TODAY
    --   Same filters as Prior_WorkDays_WITH_Leave but Is_Day_With_a_Leave=FALSE
    CASE WHEN NOT b."Is_Task_a_Leave" THEN pttl.cnt END               AS "Prior_WorkDays_WITHOUT_Leave",
    -- Allo_Mins_during_PriorWorkDays_WITH_leave = SUM(Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE)*60
    --   = pwdl.cnt * Avg_Mins_perWorkDay_WITH_Leaves (billable tasks only)
    --   Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE = Avg_Mins_perWorkDay_WITH_Leaves/60
    --     when Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek
    --          AND Is_Day_With_a_Leave=TRUE AND Is_Full_Day_Leave=FALSE
    --     RETURN: Is_Date_between_Start&Today=TRUE AND Task_Category="Billable Tasks"
    CASE WHEN NOT b."Is_Task_a_Leave" THEN pam.allo_mins END          AS "Allo_Mins_during_PriorWorkDays_WITH_leave",
    -- Is_Mins_PriorWorkDays_WITH_Leave_>_Task_Mins_Worked = Allo_Mins > Task_Mins_Worked_Adjusted
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN pam.allo_mins > twa.adj_mins
    END                                                               AS "Is_Mins_PriorWorkDays_WITH_Leave_>_Task_Mins_Worked",
    -- Adj_Worked_Mins_PriorWorkDays_WITH_Leave = IF(Is_Mins_Prior_>_Worked, Task_Mins_Worked_Adjusted, Allo_Mins_during_PriorWorkDays_WITH_leave)
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN CASE WHEN pam.allo_mins > twa.adj_mins THEN twa.adj_mins ELSE pam.allo_mins END
    END                                                               AS "Adj_Worked_Mins_PriorWorkDays_WITH_Leave",
    -- Adj_Worked_Mins_PriorWorkDays_WITHOUT_Leave = Task_Mins_Worked_Adjusted - Adj_Worked_Mins_PriorWorkDays_WITH_Leave
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN twa.adj_mins
              - CASE WHEN pam.allo_mins > twa.adj_mins THEN twa.adj_mins ELSE pam.allo_mins END
    END                                                               AS "Adj_Worked_Mins_PriorWorkDays_WITHOUT_Leave",
    -- Avg_Worked_Mins_perPriorDays_WITH_Leave = DIVIDE(Adj_Worked_Mins_PriorWorkDays_WITH_Leave, Prior_WorkDays_WITH_Leave, BLANK())
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN CASE WHEN pam.allo_mins > twa.adj_mins THEN twa.adj_mins ELSE pam.allo_mins END
              / NULLIF(pwdl.cnt, 0)
    END                                                               AS "Avg_Worked_Mins_perPriorDays_WITH_Leave",
    -- Avg_Worked_Mins_perPriorDays_WITHOUT_Leave = DIVIDE(Adj_Worked_Mins_PriorWorkDays_WITHOUT_Leave, Prior_WorkDays_WITHOUT_Leave, BLANK())
    CASE WHEN NOT b."Is_Task_a_Leave"
         THEN (twa.adj_mins - CASE WHEN pam.allo_mins > twa.adj_mins THEN twa.adj_mins ELSE pam.allo_mins END)
              / NULLIF(pttl.cnt, 0)
    END                                                               AS "Avg_Worked_Mins_perPriorDays_WITHOUT_Leave"
FROM (
    SELECT
        (jt."JobDetailsRemoteID"::text || jt."UUID"::text || jta."UUID"::text) AS "Job_Task_Staff_ID",
        jt."JobDetailsRemoteID"                      AS "Job_ID",
        jt."UUID"                                    AS "Task_UUID",
        jt."Name"                                    AS "Task_Name",
        jt."EstimatedMinutes",
        jt."ActualMinutes",
        jt."Billable"                                AS "Task_Billable",
        jt."Completed"                               AS "Task_Completed",
        jta."UUID"                                   AS "Staff_UUID",
        jta."Name"                                   AS "Staff_Name",
        cd."Name"                                    AS "Client_Name",
        jt."StartDate",
        jt."DueDate",
        jta."AllocatedMinutes"                       AS "Task_Allocated_Mins",
        -- Task_Type: LOOKUPVALUE(key04_task_name[Task_Type], key04_task_name[Task_Name], [Task_Name])
        ktn."Task_Type",
        -- StartDateAdjusted: IF(ISBLANK([StartDate]), LOOKUPVALUE(key06_job_table[StartDate], ..., [Job_ID]), [StartDate])
        COALESCE(jt."StartDate", jd."StartDate")     AS "StartDateAdjusted",
        -- DueDateAdjusted: IF(ISBLANK([DueDate]), LOOKUPVALUE(key06_job_table[EarlierDate], ..., [Job_ID]), [DueDate])
        --   EarlierDate = IF(ISBLANK([CompletedDate]), [DueDate], IF([CompletedDate]<[DueDate], [CompletedDate], [DueDate]))
        COALESCE(
            jt."DueDate",
            CASE WHEN jd."CompletedDate" IS NULL THEN jd."DueDate"
                 ELSE LEAST(jd."CompletedDate", jd."DueDate") END
        )                                            AS "DueDateAdjusted",
        -- Is_Task_a_Leave: OR(CONTAINSSTRING([Task_Name],"Holiday"), CONTAINSSTRING(...,"Sick leave"), CONTAINSSTRING(...,"Other leave"))
        (
            jt."Name" ILIKE '%Holiday%'
            OR jt."Name" ILIKE '%Sick leave%'
            OR jt."Name" ILIKE '%Other leave%'
        )                                            AS "Is_Task_a_Leave"
    FROM jobtask jt
        LEFT JOIN jobtaskassignee jta ON jta."JobTaskID" = jt."RemoteID"::uuid
        LEFT JOIN jobdetails jd ON jd."RemoteID" = jt."JobDetailsRemoteID"
        LEFT JOIN clientdetails cd ON cd."UUID" = jd."ClientUUID"
        LEFT JOIN key04_task_name ktn ON ktn."Task_Name" = jt."Name"
    WHERE jt."IsDeleted" = FALSE
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
    -- Workable_Days_Between_Task
    --   DAX: CALCULATE(COUNTROWS('2_Staff_Task_Allocation_byDay'),
    --          FILTER([Job_Task_Staff_ID]=current), FILTER([Is_Date_Between_Task_Days]),
    --          FILTER([Is_Workable_Day]), FILTER([Is_Staff_Workable_DayOfWeek]), FILTER([Is_Full_Day_Leave]=FALSE))
    SELECT COUNT(*) AS cnt
    FROM key01_calendar_date cal
    WHERE cal."Date" >= b."StartDateAdjusted"
      AND cal."Date" <= b."DueDateAdjusted"
      AND cal."WeekEnd" = FALSE        -- Is_Workable_Day
      AND cal."PublicHoliday" = FALSE  -- Is_Workable_Day
      -- Is_Staff_Workable_DayOfWeek
      AND EXISTS (
          SELECT 1 FROM excel_workable_days ewd
          WHERE ewd.staffname = b."Staff_Name"
            AND ewd.day_of_week = cal."Weekday"
            AND ewd.working_day = TRUE
      )
      -- Is_Full_Day_Leave = FALSE: AllocatedMins / WorkableDays(leave task, no leave exclusion) = 480
      AND NOT EXISTS (
          SELECT 1
          FROM jobtask lt
          JOIN jobtaskassignee lta ON lta."JobTaskID" = lt."RemoteID"::uuid
          LEFT JOIN jobdetails ljd ON ljd."RemoteID"::text = lt."JobDetailsRemoteID"::text
          WHERE lt."IsDeleted" = FALSE
            AND lta."Name" = b."Staff_Name"
            AND lta."AllocatedMinutes" > 0
            AND (lt."Name" ILIKE '%Holiday%' OR lt."Name" ILIKE '%Sick leave%' OR lt."Name" ILIKE '%Other leave%')
            AND cal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
            AND cal."Date" <= COALESCE(
                    lt."DueDate",
                    CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                         ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END
                )
            AND lta."AllocatedMinutes"::float / NULLIF((
                SELECT COUNT(*)
                FROM key01_calendar_date wcal
                WHERE wcal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
                  AND wcal."Date" <= COALESCE(
                          lt."DueDate",
                          CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                               ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END
                      )
                  AND wcal."WeekEnd" = FALSE
                  AND wcal."PublicHoliday" = FALSE
                  AND EXISTS (
                      SELECT 1 FROM excel_workable_days lewd
                      WHERE lewd.staffname = lta."Name"
                        AND lewd.day_of_week = wcal."Weekday"
                        AND lewd.working_day = TRUE
                  )
            ), 0) = 480
      )
) wdb
CROSS JOIN LATERAL (
    -- Total_Leave_Hrs_between_Workable_Days
    -- Sums Initial_Allo_Hrs_perWorkDay_KPI01 for leave tasks overlapping this task's date range
    --   Initial_Allo_Hrs_perWorkDay_KPI01 = Initial_Avg_Mins_perWorkDay(leave_task) / 60
    --     when Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek
    --          AND Is_Full_Day_Leave=FALSE AND Admin_Task_To_Be_Removed=FALSE
    --   Initial_Avg_Mins_perWorkDay(leave_task) = AllocatedMins / WorkableDays(leave_task, no leave exclusion)
    SELECT COALESCE(SUM(
        lta."AllocatedMinutes"::float
        / NULLIF((
            SELECT COUNT(*)
            FROM key01_calendar_date wcal
            WHERE wcal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
              AND wcal."Date" <= COALESCE(
                      lt."DueDate",
                      CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                           ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END
                  )
              AND wcal."WeekEnd" = FALSE
              AND wcal."PublicHoliday" = FALSE
              AND EXISTS (
                  SELECT 1 FROM excel_workable_days lewd
                  WHERE lewd.staffname = lta."Name"
                    AND lewd.day_of_week = wcal."Weekday"
                    AND lewd.working_day = TRUE
              )
        ), 0)
        / 60.0
    ), 0) AS hrs
    FROM key01_calendar_date cal
    JOIN jobtask lt ON (
        lt."Name" ILIKE '%Holiday%'
        OR lt."Name" ILIKE '%Sick leave%'
        OR lt."Name" ILIKE '%Other leave%'
    )
    JOIN jobtaskassignee lta ON lta."JobTaskID" = lt."RemoteID"::uuid
    LEFT JOIN jobdetails ljd ON ljd."RemoteID"::text = lt."JobDetailsRemoteID"::text
    WHERE lt."IsDeleted" = FALSE
      AND lta."Name" = b."Staff_Name"
      AND lta."AllocatedMinutes" > 0
      -- DATESBETWEEN: cal date within current task's adjusted date range
      AND cal."Date" >= b."StartDateAdjusted"
      AND cal."Date" <= b."DueDateAdjusted"
      -- Is_Date_Between_Task_Days: cal date within leave task's adjusted date range
      AND cal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
      AND cal."Date" <= COALESCE(
              lt."DueDate",
              CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                   ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END
          )
      -- Is_Workable_Day
      AND cal."WeekEnd" = FALSE
      AND cal."PublicHoliday" = FALSE
      -- Is_Staff_Workable_DayOfWeek
      AND EXISTS (
          SELECT 1 FROM excel_workable_days ewd
          WHERE ewd.staffname = lta."Name"
            AND ewd.day_of_week = cal."Weekday"
            AND ewd.working_day = TRUE
      )
      -- Is_Full_Day_Leave = FALSE
      AND NOT EXISTS (
          SELECT 1
          FROM jobtask flt
          JOIN jobtaskassignee flta ON flta."JobTaskID" = flt."RemoteID"::uuid
          LEFT JOIN jobdetails fljd ON fljd."RemoteID"::text = flt."JobDetailsRemoteID"::text
          WHERE flt."IsDeleted" = FALSE
            AND flta."Name" = lta."Name"
            AND flta."AllocatedMinutes" > 0
            AND (flt."Name" ILIKE '%Holiday%' OR flt."Name" ILIKE '%Sick leave%' OR flt."Name" ILIKE '%Other leave%')
            AND cal."Date" >= COALESCE(flt."StartDate", fljd."StartDate")
            AND cal."Date" <= COALESCE(
                    flt."DueDate",
                    CASE WHEN fljd."CompletedDate" IS NULL THEN fljd."DueDate"
                         ELSE LEAST(fljd."CompletedDate", fljd."DueDate") END
                )
            AND flta."AllocatedMinutes"::float / NULLIF((
                SELECT COUNT(*)
                FROM key01_calendar_date wcal2
                WHERE wcal2."Date" >= COALESCE(flt."StartDate", fljd."StartDate")
                  AND wcal2."Date" <= COALESCE(
                          flt."DueDate",
                          CASE WHEN fljd."CompletedDate" IS NULL THEN fljd."DueDate"
                               ELSE LEAST(fljd."CompletedDate", fljd."DueDate") END
                      )
                  AND wcal2."WeekEnd" = FALSE
                  AND wcal2."PublicHoliday" = FALSE
                  AND EXISTS (
                      SELECT 1 FROM excel_workable_days lewd2
                      WHERE lewd2.staffname = flta."Name"
                        AND lewd2.day_of_week = wcal2."Weekday"
                        AND lewd2.working_day = TRUE
                  )
            ), 0) = 480
      )
      -- Admin_Task_To_Be_Removed = FALSE: Task_Name='Admin - Non-billable' AND Date >= 2021-02-01
      -- Leave tasks (Holiday/Sick/Other leave) are never 'Admin - Non-billable', condition always met
      AND NOT (lt."Name" = 'Admin - Non-billable' AND cal."Date" >= DATE '2021-02-01')
) tlh
CROSS JOIN LATERAL (
    -- ttl: count of workable days WITHOUT any leave, for Total_Task_Mins_WorkDays_WITHOUT_Leave
    --   Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02 conditions:
    --     Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek
    --     AND Is_Day_With_a_Leave=FALSE AND Admin_Task_To_Be_Removed=FALSE
    --   (Is_Full_Day_Leave=FALSE implied by Is_Day_With_a_Leave=FALSE)
    SELECT COUNT(*) AS cnt
    FROM key01_calendar_date cal
    WHERE cal."Date" >= b."StartDateAdjusted"
      AND cal."Date" <= b."DueDateAdjusted"
      AND cal."WeekEnd" = FALSE
      AND cal."PublicHoliday" = FALSE
      AND EXISTS (
          SELECT 1 FROM excel_workable_days ewd
          WHERE ewd.staffname = b."Staff_Name"
            AND ewd.day_of_week = cal."Weekday"
            AND ewd.working_day = TRUE
      )
      -- Is_Day_With_a_Leave = FALSE: no active leave task covers this date for this staff
      AND NOT EXISTS (
          SELECT 1
          FROM jobtask lt
          JOIN jobtaskassignee lta ON lta."JobTaskID" = lt."RemoteID"::uuid
          LEFT JOIN jobdetails ljd ON ljd."RemoteID"::text = lt."JobDetailsRemoteID"::text
          WHERE lt."IsDeleted" = FALSE
            AND lta."Name" = b."Staff_Name"
            AND lta."AllocatedMinutes" > 0
            AND (lt."Name" ILIKE '%Holiday%' OR lt."Name" ILIKE '%Sick leave%' OR lt."Name" ILIKE '%Other leave%')
            AND cal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
            AND cal."Date" <= COALESCE(
                    lt."DueDate",
                    CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                         ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END
                )
            AND EXISTS (
                SELECT 1 FROM excel_workable_days lewd
                WHERE lewd.staffname = lta."Name"
                  AND lewd.day_of_week = cal."Weekday"
                  AND lewd.working_day = TRUE
            )
      )
      -- Admin_Task_To_Be_Removed = FALSE
      AND NOT (b."Task_Name" = 'Admin - Non-billable' AND cal."Date" >= DATE '2021-02-01')
) ttl
CROSS JOIN LATERAL (
    -- wdl: count of workable days WITH partial leave, for WorkDays_WITH_Leaves_between_Task
    --   Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek
    --   AND Is_Day_With_a_Leave=TRUE AND Is_Full_Day_Leave=FALSE
    SELECT COUNT(*) AS cnt
    FROM key01_calendar_date cal
    WHERE cal."Date" >= b."StartDateAdjusted"
      AND cal."Date" <= b."DueDateAdjusted"
      AND cal."WeekEnd" = FALSE
      AND cal."PublicHoliday" = FALSE
      AND EXISTS (
          SELECT 1 FROM excel_workable_days ewd
          WHERE ewd.staffname = b."Staff_Name"
            AND ewd.day_of_week = cal."Weekday"
            AND ewd.working_day = TRUE
      )
      -- Is_Day_With_a_Leave = TRUE: staff has at least one active leave task on this date
      AND EXISTS (
          SELECT 1
          FROM jobtask lt
          JOIN jobtaskassignee lta ON lta."JobTaskID" = lt."RemoteID"::uuid
          LEFT JOIN jobdetails ljd ON ljd."RemoteID"::text = lt."JobDetailsRemoteID"::text
          WHERE lt."IsDeleted" = FALSE
            AND lta."Name" = b."Staff_Name"
            AND lta."AllocatedMinutes" > 0
            AND (lt."Name" ILIKE '%Holiday%' OR lt."Name" ILIKE '%Sick leave%' OR lt."Name" ILIKE '%Other leave%')
            AND cal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
            AND cal."Date" <= COALESCE(
                    lt."DueDate",
                    CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                         ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END
                )
            AND EXISTS (
                SELECT 1 FROM excel_workable_days lewd
                WHERE lewd.staffname = lta."Name"
                  AND lewd.day_of_week = cal."Weekday"
                  AND lewd.working_day = TRUE
            )
      )
      -- Is_Full_Day_Leave = FALSE: not a full 8-hr leave day
      AND NOT EXISTS (
          SELECT 1
          FROM jobtask flt
          JOIN jobtaskassignee flta ON flta."JobTaskID" = flt."RemoteID"::uuid
          LEFT JOIN jobdetails fljd ON fljd."RemoteID"::text = flt."JobDetailsRemoteID"::text
          WHERE flt."IsDeleted" = FALSE
            AND flta."Name" = b."Staff_Name"
            AND flta."AllocatedMinutes" > 0
            AND (flt."Name" ILIKE '%Holiday%' OR flt."Name" ILIKE '%Sick leave%' OR flt."Name" ILIKE '%Other leave%')
            AND cal."Date" >= COALESCE(flt."StartDate", fljd."StartDate")
            AND cal."Date" <= COALESCE(
                    flt."DueDate",
                    CASE WHEN fljd."CompletedDate" IS NULL THEN fljd."DueDate"
                         ELSE LEAST(fljd."CompletedDate", fljd."DueDate") END
                )
            AND flta."AllocatedMinutes"::float / NULLIF((
                SELECT COUNT(*)
                FROM key01_calendar_date wcal
                WHERE wcal."Date" >= COALESCE(flt."StartDate", fljd."StartDate")
                  AND wcal."Date" <= COALESCE(
                          flt."DueDate",
                          CASE WHEN fljd."CompletedDate" IS NULL THEN fljd."DueDate"
                               ELSE LEAST(fljd."CompletedDate", fljd."DueDate") END
                      )
                  AND wcal."WeekEnd" = FALSE
                  AND wcal."PublicHoliday" = FALSE
                  AND EXISTS (
                      SELECT 1 FROM excel_workable_days lewd
                      WHERE lewd.staffname = flta."Name"
                        AND lewd.day_of_week = wcal."Weekday"
                        AND lewd.working_day = TRUE
                  )
            ), 0) = 480
      )
) wdl
CROSS JOIN LATERAL (
    -- tmt: total recorded minutes from 4_Timesheet_Table for this task
    --   DAX: CALCULATE(SUM([Recorded_Minutes]), FILTER([Job_Task_Staff_ID] = current))
    SELECT COALESCE(SUM(t."Minutes"), 0) AS recorded_mins
    FROM "time" t
    WHERE (t."JobID"::text || t."TaskUUID"::text || t."StaffMemberUUID"::text) = b."Job_Task_Staff_ID"
      AND t."Date" >= '2020-01-01'
) tmt
CROSS JOIN LATERAL (
    -- arwd: count of remaining workable days with partial leave, for Allo_Mins_during_Remaining_workDays_WITH_leave
    --   Is_Date_between_Today&Due: Date >= TODAY AND Date in task range
    --   AND Is_Workable_Day AND Is_Staff_Workable_DayOfWeek AND Is_Day_With_a_Leave=TRUE AND Is_Full_Day_Leave=FALSE
    SELECT COUNT(*) AS cnt
    FROM key01_calendar_date cal
    WHERE cal."Date" >= b."StartDateAdjusted"
      AND cal."Date" <= b."DueDateAdjusted"
      AND cal."Date" >= CURRENT_DATE          -- Is_Date_between_Today&Due
      AND cal."WeekEnd" = FALSE
      AND cal."PublicHoliday" = FALSE
      AND EXISTS (
          SELECT 1 FROM excel_workable_days ewd
          WHERE ewd.staffname = b."Staff_Name"
            AND ewd.day_of_week = cal."Weekday"
            AND ewd.working_day = TRUE
      )
      -- Is_Day_With_a_Leave = TRUE
      AND EXISTS (
          SELECT 1
          FROM jobtask lt
          JOIN jobtaskassignee lta ON lta."JobTaskID" = lt."RemoteID"::uuid
          LEFT JOIN jobdetails ljd ON ljd."RemoteID"::text = lt."JobDetailsRemoteID"::text
          WHERE lt."IsDeleted" = FALSE
            AND lta."Name" = b."Staff_Name"
            AND lta."AllocatedMinutes" > 0
            AND (lt."Name" ILIKE '%Holiday%' OR lt."Name" ILIKE '%Sick leave%' OR lt."Name" ILIKE '%Other leave%')
            AND cal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
            AND cal."Date" <= COALESCE(
                    lt."DueDate",
                    CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                         ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END
                )
            AND EXISTS (
                SELECT 1 FROM excel_workable_days lewd
                WHERE lewd.staffname = lta."Name"
                  AND lewd.day_of_week = cal."Weekday"
                  AND lewd.working_day = TRUE
            )
      )
      -- Is_Full_Day_Leave = FALSE
      AND NOT EXISTS (
          SELECT 1
          FROM jobtask flt
          JOIN jobtaskassignee flta ON flta."JobTaskID" = flt."RemoteID"::uuid
          LEFT JOIN jobdetails fljd ON fljd."RemoteID"::text = flt."JobDetailsRemoteID"::text
          WHERE flt."IsDeleted" = FALSE
            AND flta."Name" = b."Staff_Name"
            AND flta."AllocatedMinutes" > 0
            AND (flt."Name" ILIKE '%Holiday%' OR flt."Name" ILIKE '%Sick leave%' OR flt."Name" ILIKE '%Other leave%')
            AND cal."Date" >= COALESCE(flt."StartDate", fljd."StartDate")
            AND cal."Date" <= COALESCE(
                    flt."DueDate",
                    CASE WHEN fljd."CompletedDate" IS NULL THEN fljd."DueDate"
                         ELSE LEAST(fljd."CompletedDate", fljd."DueDate") END
                )
            AND flta."AllocatedMinutes"::float / NULLIF((
                SELECT COUNT(*)
                FROM key01_calendar_date wcal
                WHERE wcal."Date" >= COALESCE(flt."StartDate", fljd."StartDate")
                  AND wcal."Date" <= COALESCE(
                          flt."DueDate",
                          CASE WHEN fljd."CompletedDate" IS NULL THEN fljd."DueDate"
                               ELSE LEAST(fljd."CompletedDate", fljd."DueDate") END
                      )
                  AND wcal."WeekEnd" = FALSE
                  AND wcal."PublicHoliday" = FALSE
                  AND EXISTS (
                      SELECT 1 FROM excel_workable_days lewd
                      WHERE lewd.staffname = flta."Name"
                        AND lewd.day_of_week = wcal."Weekday"
                        AND lewd.working_day = TRUE
                  )
            ), 0) = 480
      )
) arwd
CROSS JOIN LATERAL (
    -- rmn: pre-compute Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave for reuse across columns
    --   = Task_Mins_Remain_until_Due - Allo_Mins_during_Remaining_workDays_WITH_leave
    --   Allo_Mins is 0 (not NULL) for admin/non-billable tasks, matching DAX BLANK-as-0 arithmetic
    SELECT GREATEST(0, b."Task_Allocated_Mins" - tmt.recorded_mins)
           - CASE WHEN NOT (b."Task_Name" ILIKE '%Admin - Non-billable%' OR b."Client_Name" = 'Dinniss Admin')
                  THEN COALESCE(
                           arwd.cnt::float
                           * (b."Task_Allocated_Mins" - ttl.cnt::float * b."Task_Allocated_Mins"::float / NULLIF((wdb.cnt * 8 - COALESCE(tlh.hrs, 0)) / 8.0, 0))
                           / NULLIF(wdl.cnt, 0),
                           0)
                  ELSE 0
             END AS remain_mins
) rmn
CROSS JOIN LATERAL (
    -- rttl: remaining workable days WITHOUT any leave (TODAY to DueDateAdjusted), for Remain_WorkDays_WITHOUT_Leave
    --   Same filters as ttl but with Date >= CURRENT_DATE (DATESBETWEEN TODAY to DueDateAdjusted)
    SELECT COUNT(*) AS cnt
    FROM key01_calendar_date cal
    WHERE cal."Date" >= b."StartDateAdjusted"
      AND cal."Date" <= b."DueDateAdjusted"
      AND cal."Date" >= CURRENT_DATE
      AND cal."WeekEnd" = FALSE
      AND cal."PublicHoliday" = FALSE
      AND EXISTS (
          SELECT 1 FROM excel_workable_days ewd
          WHERE ewd.staffname = b."Staff_Name"
            AND ewd.day_of_week = cal."Weekday"
            AND ewd.working_day = TRUE
      )
      -- Is_Day_With_a_Leave = FALSE
      AND NOT EXISTS (
          SELECT 1
          FROM jobtask lt
          JOIN jobtaskassignee lta ON lta."JobTaskID" = lt."RemoteID"::uuid
          LEFT JOIN jobdetails ljd ON ljd."RemoteID"::text = lt."JobDetailsRemoteID"::text
          WHERE lt."IsDeleted" = FALSE
            AND lta."Name" = b."Staff_Name"
            AND lta."AllocatedMinutes" > 0
            AND (lt."Name" ILIKE '%Holiday%' OR lt."Name" ILIKE '%Sick leave%' OR lt."Name" ILIKE '%Other leave%')
            AND cal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
            AND cal."Date" <= COALESCE(
                    lt."DueDate",
                    CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                         ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END
                )
            AND EXISTS (
                SELECT 1 FROM excel_workable_days lewd
                WHERE lewd.staffname = lta."Name"
                  AND lewd.day_of_week = cal."Weekday"
                  AND lewd.working_day = TRUE
            )
      )
      AND NOT (b."Task_Name" = 'Admin - Non-billable' AND cal."Date" >= DATE '2021-02-01')
) rttl
CROSS JOIN LATERAL (
    -- pwdl: workable days WITH partial leave from StartDateAdjusted up to TODAY (Prior_WorkDays_WITH_Leave)
    --   Same as wdl but Date also <= CURRENT_DATE (Is_Date_between_Start&Today combined with Is_Date_Between_Task_Days)
    SELECT COUNT(*) AS cnt
    FROM key01_calendar_date cal
    WHERE cal."Date" >= b."StartDateAdjusted"
      AND cal."Date" <= b."DueDateAdjusted"
      AND cal."Date" <= CURRENT_DATE          -- Is_Date_between_Start&Today
      AND cal."WeekEnd" = FALSE
      AND cal."PublicHoliday" = FALSE
      AND EXISTS (
          SELECT 1 FROM excel_workable_days ewd
          WHERE ewd.staffname = b."Staff_Name"
            AND ewd.day_of_week = cal."Weekday"
            AND ewd.working_day = TRUE
      )
      -- Is_Day_With_a_Leave = TRUE
      AND EXISTS (
          SELECT 1
          FROM jobtask lt
          JOIN jobtaskassignee lta ON lta."JobTaskID" = lt."RemoteID"::uuid
          LEFT JOIN jobdetails ljd ON ljd."RemoteID"::text = lt."JobDetailsRemoteID"::text
          WHERE lt."IsDeleted" = FALSE
            AND lta."Name" = b."Staff_Name"
            AND lta."AllocatedMinutes" > 0
            AND (lt."Name" ILIKE '%Holiday%' OR lt."Name" ILIKE '%Sick leave%' OR lt."Name" ILIKE '%Other leave%')
            AND cal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
            AND cal."Date" <= COALESCE(
                    lt."DueDate",
                    CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                         ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END
                )
            AND EXISTS (
                SELECT 1 FROM excel_workable_days lewd
                WHERE lewd.staffname = lta."Name"
                  AND lewd.day_of_week = cal."Weekday"
                  AND lewd.working_day = TRUE
            )
      )
      -- Is_Full_Day_Leave = FALSE
      AND NOT EXISTS (
          SELECT 1
          FROM jobtask flt
          JOIN jobtaskassignee flta ON flta."JobTaskID" = flt."RemoteID"::uuid
          LEFT JOIN jobdetails fljd ON fljd."RemoteID"::text = flt."JobDetailsRemoteID"::text
          WHERE flt."IsDeleted" = FALSE
            AND flta."Name" = b."Staff_Name"
            AND flta."AllocatedMinutes" > 0
            AND (flt."Name" ILIKE '%Holiday%' OR flt."Name" ILIKE '%Sick leave%' OR flt."Name" ILIKE '%Other leave%')
            AND cal."Date" >= COALESCE(flt."StartDate", fljd."StartDate")
            AND cal."Date" <= COALESCE(
                    flt."DueDate",
                    CASE WHEN fljd."CompletedDate" IS NULL THEN fljd."DueDate"
                         ELSE LEAST(fljd."CompletedDate", fljd."DueDate") END
                )
            AND flta."AllocatedMinutes"::float / NULLIF((
                SELECT COUNT(*)
                FROM key01_calendar_date wcal
                WHERE wcal."Date" >= COALESCE(flt."StartDate", fljd."StartDate")
                  AND wcal."Date" <= COALESCE(
                          flt."DueDate",
                          CASE WHEN fljd."CompletedDate" IS NULL THEN fljd."DueDate"
                               ELSE LEAST(fljd."CompletedDate", fljd."DueDate") END
                      )
                  AND wcal."WeekEnd" = FALSE
                  AND wcal."PublicHoliday" = FALSE
                  AND EXISTS (
                      SELECT 1 FROM excel_workable_days lewd
                      WHERE lewd.staffname = flta."Name"
                        AND lewd.day_of_week = wcal."Weekday"
                        AND lewd.working_day = TRUE
                  )
            ), 0) = 480
      )
) pwdl
CROSS JOIN LATERAL (
    -- pttl: prior workable days WITHOUT any leave (StartDateAdjusted to TODAY), for Prior_WorkDays_WITHOUT_Leave
    --   Same as ttl but with Date <= CURRENT_DATE (Is_Date_between_Start&Today)
    SELECT COUNT(*) AS cnt
    FROM key01_calendar_date cal
    WHERE cal."Date" >= b."StartDateAdjusted"
      AND cal."Date" <= b."DueDateAdjusted"
      AND cal."Date" <= CURRENT_DATE
      AND cal."WeekEnd" = FALSE
      AND cal."PublicHoliday" = FALSE
      AND EXISTS (
          SELECT 1 FROM excel_workable_days ewd
          WHERE ewd.staffname = b."Staff_Name"
            AND ewd.day_of_week = cal."Weekday"
            AND ewd.working_day = TRUE
      )
      AND NOT EXISTS (
          SELECT 1
          FROM jobtask lt
          JOIN jobtaskassignee lta ON lta."JobTaskID" = lt."RemoteID"::uuid
          LEFT JOIN jobdetails ljd ON ljd."RemoteID"::text = lt."JobDetailsRemoteID"::text
          WHERE lt."IsDeleted" = FALSE
            AND lta."Name" = b."Staff_Name"
            AND lta."AllocatedMinutes" > 0
            AND (lt."Name" ILIKE '%Holiday%' OR lt."Name" ILIKE '%Sick leave%' OR lt."Name" ILIKE '%Other leave%')
            AND cal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
            AND cal."Date" <= COALESCE(
                    lt."DueDate",
                    CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                         ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END
                )
            AND EXISTS (
                SELECT 1 FROM excel_workable_days lewd
                WHERE lewd.staffname = lta."Name"
                  AND lewd.day_of_week = cal."Weekday"
                  AND lewd.working_day = TRUE
            )
      )
      AND NOT (b."Task_Name" = 'Admin - Non-billable' AND cal."Date" >= DATE '2021-02-01')
) pttl
CROSS JOIN LATERAL (
    -- twa: pre-compute Task_Mins_Worked_Adjusted for reuse
    --   = IF(IS_Task_Mins_Worked_>_Allocated OR Is_Task_DueDate_Over, Task_Allocated_Mins, Task_Mins_Worked_Till_Date)
    SELECT CASE WHEN tmt.recorded_mins > b."Task_Allocated_Mins"
                     OR b."DueDateAdjusted" < CURRENT_DATE
                THEN b."Task_Allocated_Mins"
                ELSE tmt.recorded_mins
           END AS adj_mins
) twa
CROSS JOIN LATERAL (
    -- pam: pre-compute Allo_Mins_during_PriorWorkDays_WITH_leave for reuse
    --   = pwdl.cnt * Avg_Mins_perWorkDay_WITH_Leaves (billable tasks only, NULL for admin)
    SELECT CASE WHEN NOT (b."Task_Name" ILIKE '%Admin - Non-billable%' OR b."Client_Name" = 'Dinniss Admin')
                THEN pwdl.cnt::float
                     * (b."Task_Allocated_Mins" - ttl.cnt::float * b."Task_Allocated_Mins"::float / NULLIF((wdb.cnt * 8 - COALESCE(tlh.hrs, 0)) / 8.0, 0))
                     / NULLIF(wdl.cnt, 0)
                ELSE NULL
           END AS allo_mins
) pam;


DROP VIEW IF EXISTS "4_Timesheet_Table" CASCADE;


CREATE OR REPLACE VIEW "4_Timesheet_Table" AS
SELECT
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
    t."Minutes" AS "Recorded_Minutes"
FROM
    TIME t
    LEFT JOIN "key03_staff_table" s ON s."Staff_UUID" = t."StaffMemberUUID"
WHERE
    t."Date" >= '2020-01-01'
    AND s."Staff_Name" IS NOT NULL;


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


DROP VIEW IF EXISTS key02_job_task_staff_id CASCADE;


CREATE OR REPLACE VIEW key02_job_task_staff_id AS
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


DROP VIEW IF EXISTS "2_Staff_Task_Allocation_byDay" CASCADE;


CREATE OR REPLACE VIEW "2_Staff_Task_Allocation_byDay" AS
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
    END AS "Billable_Selector"
FROM
    key01_calendar_date c
    CROSS JOIN key02_job_task_staff_id k;


DROP VIEW IF EXISTS key07_is_billable CASCADE;


CREATE OR REPLACE VIEW key07_is_billable AS
SELECT DISTINCT
    "Billable_Selector"
FROM
    "2_Staff_Task_Allocation_byDay";


DROP VIEW IF EXISTS "3_Staff_Performance_Table" CASCADE;


CREATE OR REPLACE VIEW "3_Staff_Performance_Table" AS
SELECT
    c.*,
    s.*
FROM
    key01_calendar_date c
    CROSS JOIN key03_staff_table s;