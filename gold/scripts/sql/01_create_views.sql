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
    t."Minutes" AS "Recorded_Minutes",
    -- Task_Name: LOOKUPVALUE from 1_Job_Task_Details_Table by Job_Task_Staff_ID, fallback to excel_recorded_invoiced_hours[Task] by Timesheet_UUID
    COALESCE(jt_lkp."Task_Name", e_lkp.task)                         AS "Task_Name",
    -- Client_Name: LOOKUPVALUE from 1_Job_Task_Details_Table by Job_Task_Staff_ID, fallback to excel_recorded_invoiced_hours[Client] by Timesheet_UUID
    COALESCE(jt_lkp."Client_Name", e_lkp.client)                     AS "Client_Name",
    -- Task_Type: LOOKUPVALUE from 1_Job_Task_Details_Table by Job_Task_Staff_ID, fallback to excel_recorded_invoiced_hours[Task] by Timesheet_UUID
    COALESCE(jt_lkp."Task_Type", e_lkp.task)                         AS "Task_Type",
    -- Task_Completed: LOOKUPVALUE from 1_Job_Task_Details_Table by Job_Task_Staff_ID
    jt_lkp."Task_Completed"                                           AS "Task_Completed",
    -- Is_Client = NOT (Client_Name="Dinniss Admin" OR Task_Type="Admin - Non-billable")
    ic.is_client                                                      AS "Is_Client",
    -- Invoiced_Time: IF(AND(ISBLANK(Invoice_Task_ID), Is_Client), "Un-Invoiced", IF(ISBLANK(Invoice_Task_ID), "Dinniss Time", "Invoiced"))
    CASE
        WHEN t."InvoiceTaskUUID" IS NULL AND ic.is_client THEN 'Un-Invoiced'
        WHEN t."InvoiceTaskUUID" IS NULL                  THEN 'Dinniss Time'
        ELSE 'Invoiced'
    END                                                               AS "Invoiced_Time",
    -- Is_Billable = NOT (Is_Client=FALSE OR Task_Type="Coaching")
    (ic.is_client AND COALESCE(jt_lkp."Task_Type", e_lkp.task) <> 'Coaching')
                                                                      AS "Is_Billable",
    -- Billable_Selector = IF(Is_Billable, "Billable", "Not Billable")
    CASE
        WHEN ic.is_client AND COALESCE(jt_lkp."Task_Type", e_lkp.task) <> 'Coaching'
            THEN 'Billable'
        ELSE 'Not Billable'
    END                                                               AS "Billable_Selector",
    -- Task_Category: Leave/Admin/Billable based on Task_Name and Client_Name
    CASE
        WHEN COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Holiday%'
          OR COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Other leave%'
          OR COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Sick leave%'
            THEN 'Leave Tasks'
        WHEN COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Admin - Non-billable%'
          OR COALESCE(jt_lkp."Client_Name", e_lkp.client) = 'Dinniss Admin'
            THEN 'Admin Tasks'
        ELSE 'Billable Tasks'
    END                                                               AS "Task_Category",
    -- Invoice_Number: LOOKUPVALUE(invoicetask[InvoiceID], invoicetask[UUID], Invoice_Task_ID)
    inv_lkp.invoice_number                                            AS "Invoice_Number",
    -- Month_Time_Recorded = DATE(YEAR(Date), MONTH(Date), 1) = first day of recorded month
    DATE_TRUNC('month', t."Date")::DATE                              AS "Month_Time_Recorded",
    -- Month_Time_Invoiced: first day of month the invoice was raised
    inv_lkp.invoiced_month                                           AS "Month_Time_Invoiced",
    -- Invoiced_Minutes: LOOKUPVALUE(excel_recorded_invoiced_hours[Invoiced_Mins], ..., Timesheet_UUID)
    e_lkp.invoiced_mins                                              AS "Invoiced_Minutes",
    -- Month_Invoiced_On: DATEDIFF(Month_Time_Recorded, Month_Time_Invoiced, MONTH); -1→1, else diff+1; NULL if no invoice
    mto.val                                                          AS "Month_Invoiced_On",
    -- Recorded_Hours_invoiced: billing timing category (Billable Tasks only)
    CASE WHEN ibt.is_billable_task THEN
        CASE
            WHEN t."InvoiceTaskUUID" IS NULL AND ic.is_client THEN '5_Un-Invoiced'
            WHEN mto.val = 1                                  THEN '1_Same Month'
            WHEN mto.val = 2                                  THEN '2_Following Month'
            WHEN mto.val = 3                                  THEN '3_Third Month'
            ELSE '4_Fourth Month +'
        END
    END                                                               AS "Recorded_Hours_invoiced",
    -- Job_Name: RELATED(key02_job_task_staff_id[Job_Name])
    k2_lkp."Job_Name"                                                AS "Job_Name"
FROM
    TIME t
    LEFT JOIN "key03_staff_table" s ON s."Staff_UUID" = t."StaffMemberUUID"
    LEFT JOIN LATERAL (
        SELECT jt."Task_Name", jt."Client_Name", jt."Task_Type", jt."Task_Completed"
        FROM "1_Job_Task_Details_Table" jt
        WHERE jt."Job_Task_Staff_ID" = (t."JobID" || t."TaskUUID" || t."StaffMemberUUID")
        LIMIT 1
    ) jt_lkp ON TRUE
    LEFT JOIN LATERAL (
        SELECT e.task, e.client, e.invoiced_mins
        FROM excel_recorded_invoiced_hours e
        WHERE e.timesheet_uuid = t."UUID"::text
        LIMIT 1
    ) e_lkp ON TRUE
    CROSS JOIN LATERAL (
        -- ic: pre-compute Is_Client to reuse across Invoiced_Time, Is_Billable, Billable_Selector
        SELECT NOT (
            COALESCE(jt_lkp."Client_Name", e_lkp.client) = 'Dinniss Admin'
            OR COALESCE(jt_lkp."Task_Type", e_lkp.task) = 'Admin - Non-billable'
        ) AS is_client
    ) ic
    CROSS JOIN LATERAL (
        -- ibt: pre-compute is_billable_task for Recorded_Hours_invoiced
        SELECT NOT (
            COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Holiday%'
            OR COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Other leave%'
            OR COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Sick leave%'
            OR COALESCE(jt_lkp."Task_Name", e_lkp.task) ILIKE '%Admin - Non-billable%'
            OR COALESCE(jt_lkp."Client_Name", e_lkp.client) = 'Dinniss Admin'
        ) AS is_billable_task
    ) ibt
    LEFT JOIN LATERAL (
        -- inv_lkp: Invoice_Number (= InvoiceID) and Month_Time_Invoiced from invoicetask + invoice
        SELECT it."InvoiceID"                            AS invoice_number,
               DATE_TRUNC('month', inv."Date")::DATE     AS invoiced_month
        FROM invoicetask it
        LEFT JOIN invoice inv ON inv."ID" = it."InvoiceID"
                              AND inv."IsDeleted" = FALSE
        WHERE it."UUID" = t."InvoiceTaskUUID"
          AND it."IsDeleted" = FALSE
        LIMIT 1
    ) inv_lkp ON TRUE
    CROSS JOIN LATERAL (
        -- mto: Month_Invoiced_On = IF(ISBLANK(Invoice_Number), BLANK(), IF(Logic=-1, 1, Logic+1))
        --   Logic = DATEDIFF(Month_Time_Recorded, Month_Time_Invoiced, MONTH)
        SELECT CASE
            WHEN inv_lkp.invoice_number IS NULL THEN NULL
            ELSE (
                CASE
                    WHEN (
                        (EXTRACT(YEAR  FROM inv_lkp.invoiced_month)
                         - EXTRACT(YEAR  FROM DATE_TRUNC('month', t."Date")::DATE)) * 12
                        + EXTRACT(MONTH FROM inv_lkp.invoiced_month)
                        - EXTRACT(MONTH FROM DATE_TRUNC('month', t."Date")::DATE)
                    )::int = -1 THEN 1
                    ELSE (
                        (EXTRACT(YEAR  FROM inv_lkp.invoiced_month)
                         - EXTRACT(YEAR  FROM DATE_TRUNC('month', t."Date")::DATE)) * 12
                        + EXTRACT(MONTH FROM inv_lkp.invoiced_month)
                        - EXTRACT(MONTH FROM DATE_TRUNC('month', t."Date")::DATE)
                    )::int + 1
                END
            )
        END AS val
    ) mto
    LEFT JOIN LATERAL (
        -- k2_lkp: Job_Name from key06_job_table by Job_ID (avoids circular dep via key02→keys_time→4_Timesheet)
        SELECT k."Job_Name"
        FROM key06_job_table k
        WHERE k."Job_ID" = t."JobID"
        LIMIT 1
    ) k2_lkp ON TRUE
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
    END AS "Billable_Selector",
    -- Is_Date_Between_Task_Days: c.Date falls within this task's adjusted start/due dates
    (c."Date" >= jt."StartDateAdjusted" AND c."Date" <= jt."DueDateAdjusted") AS "Is_Date_Between_Task_Days",
    -- Is_Task_a_Leave: this task is a leave task (Holiday / Sick leave / Other leave)
    COALESCE(jt."Is_Task_a_Leave", FALSE)                             AS "Is_Task_a_Leave",
    -- Is_Full_Day_Leave: on this date, this staff has ANY full-day leave task (480 mins/workday)
    COALESCE(lv.is_full_day, FALSE)                                   AS "Is_Full_Day_Leave",
    -- Admin_Task_To_Be_Removed: task is Admin Non-billable or belongs to Dinniss Admin client
    (k."Task_Type1" = 'Admin - Non-billable' OR k."Client_Name" = 'Dinniss Admin')
                                                                      AS "Admin_Task_To_Be_Removed",
    -- Is_Staff_Workable_DayOfWeek: staff works on this weekday per excel_workable_days
    COALESCE(wkd.working_day, FALSE)                                  AS "Is_Staff_Workable_DayOfWeek",
    -- Is_Day_With_a_Leave: on this date, this staff has ANY partial (non-full-day) leave task
    COALESCE(lv.has_partial_leave, FALSE)                             AS "Is_Day_With_a_Leave",
    -- Initial_Allo_Hrs_perWorkDay_KPI01:
    --   IF(Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek
    --      AND Is_Full_Day_Leave=FALSE AND Admin_Task_To_Be_Removed=FALSE,
    --      Initial_Avg_Mins_perWorkDay/60, BLANK())
    CASE WHEN NOT c."WeekEnd" AND NOT c."PublicHoliday"
              AND c."Date" >= jt."StartDateAdjusted" AND c."Date" <= jt."DueDateAdjusted"
              AND COALESCE(wkd.working_day, FALSE)
              AND NOT COALESCE(lv.is_full_day, FALSE)
              AND NOT (k."Task_Type1" = 'Admin - Non-billable' OR k."Client_Name" = 'Dinniss Admin')
         THEN jt."Initial_Avg_Mins_perWorkDay" / 60.0
    END                                                               AS "Initial_Allo_Hrs_perWorkDay_KPI01",
    -- Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02:
    --   IF(Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek
    --      AND Is_Day_With_a_Leave=FALSE AND Is_Task_a_Leave=FALSE AND Is_Full_Day_Leave=FALSE
    --      AND Admin_Task_To_Be_Removed=FALSE,
    --      Avg_Mins_perWorkDay_WITHOUT_Leave/60, BLANK())
    CASE WHEN NOT c."WeekEnd" AND NOT c."PublicHoliday"
              AND c."Date" >= jt."StartDateAdjusted" AND c."Date" <= jt."DueDateAdjusted"
              AND COALESCE(wkd.working_day, FALSE)
              AND NOT COALESCE(lv.has_partial_leave, FALSE)
              AND NOT COALESCE(jt."Is_Task_a_Leave", FALSE)
              AND NOT COALESCE(lv.is_full_day, FALSE)
              AND NOT (k."Task_Type1" = 'Admin - Non-billable' OR k."Client_Name" = 'Dinniss Admin')
         THEN jt."Avg_Mins_perWorkDay_WITHOUT_Leave" / 60.0
    END                                                               AS "Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02",
    -- Allo_Hrs_perWorkday_WITH_Leave_KPI03:
    --   IF(Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek
    --      AND Is_Day_With_a_Leave=TRUE AND Is_Task_a_Leave=FALSE AND Is_Full_Day_Leave=FALSE
    --      AND Admin_Task_To_Be_Removed=FALSE,
    --      Avg_Mins_perWorkDay_WITH_Leaves/60, BLANK())
    CASE WHEN NOT c."WeekEnd" AND NOT c."PublicHoliday"
              AND c."Date" >= jt."StartDateAdjusted" AND c."Date" <= jt."DueDateAdjusted"
              AND COALESCE(wkd.working_day, FALSE)
              AND COALESCE(lv.has_partial_leave, FALSE)
              AND NOT COALESCE(jt."Is_Task_a_Leave", FALSE)
              AND NOT COALESCE(lv.is_full_day, FALSE)
              AND NOT (k."Task_Type1" = 'Admin - Non-billable' OR k."Client_Name" = 'Dinniss Admin')
         THEN jt."Avg_Mins_perWorkDay_WITH_Leaves" / 60.0
    END                                                               AS "Allo_Hrs_perWorkday_WITH_Leave_KPI03",
    -- Allo_Hrs_perWorkDay_AdjLeavesRemainDays_FIN02 = KPI04 + KPI05 (pending DAX for KPI04, KPI05)
    NULL::double precision                                            AS "Allo_Hrs_perWorkDay_AdjLeavesRemainDays_FIN02",
    -- Allo_Hrs_perWorkDay_AdjLeavesPriorDays_FIN03 = KPI06 + KPI07 (pending DAX for KPI06, KPI07)
    NULL::double precision                                            AS "Allo_Hrs_perWorkDay_AdjLeavesPriorDays_FIN03",
    -- Allo_Hrs_perWorkDay_AdjLeaves_FIN01 = FIN02 + FIN03 (pending FIN02, FIN03)
    NULL::double precision                                            AS "Allo_Hrs_perWorkDay_AdjLeaves_FIN01",
    -- Allo_Hrs_perWorkableDay_Final_Output (pending FIN01)
    NULL::double precision                                            AS "Allo_Hrs_perWorkableDay_Final_Output"
FROM
    key01_calendar_date c
    CROSS JOIN key02_job_task_staff_id k
    LEFT JOIN "1_Job_Task_Details_Table" jt ON jt."Job_Task_Staff_ID" = k."Job_Task_Staff_ID"
    LEFT JOIN LATERAL (
        -- wkd: Is_Staff_Workable_DayOfWeek lookup
        SELECT wd.working_day
        FROM excel_workable_days wd
        WHERE wd.staffname = k."Staff_Name"
          AND wd.day_of_week = c."Weekday"
        LIMIT 1
    ) wkd ON TRUE
    LEFT JOIN LATERAL (
        -- lv: leave status for this staff on this date
        --   is_full_day    = any leave task covering this date with Initial_Avg_Mins_perWorkDay = 480
        --   has_partial_leave = any leave task covering this date with Initial_Avg_Mins_perWorkDay != 480
        SELECT
            COALESCE(BOOL_OR(mpd.mins_per_day =  480), FALSE) AS is_full_day,
            COALESCE(BOOL_OR(mpd.mins_per_day != 480), FALSE) AS has_partial_leave
        FROM jobtask lt
        JOIN jobtaskassignee lta ON lta."JobTaskID" = lt."RemoteID"::uuid
        LEFT JOIN jobdetails ljd ON ljd."RemoteID"::text = lt."JobDetailsRemoteID"::text
        CROSS JOIN LATERAL (
            SELECT lta."AllocatedMinutes"::float / NULLIF((
                SELECT COUNT(*)
                FROM key01_calendar_date wcal
                WHERE wcal."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
                  AND wcal."Date" <= COALESCE(lt."DueDate",
                          CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                               ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END)
                  AND wcal."WeekEnd" = FALSE
                  AND wcal."PublicHoliday" = FALSE
                  AND EXISTS (
                      SELECT 1 FROM excel_workable_days lewd
                      WHERE lewd.staffname = lta."Name"
                        AND lewd.day_of_week = wcal."Weekday"
                        AND lewd.working_day = TRUE
                  )
            ), 0) AS mins_per_day
        ) mpd
        WHERE lt."IsDeleted" = FALSE
          AND lta."Name" = k."Staff_Name"
          AND lta."AllocatedMinutes" > 0
          AND (lt."Name" ILIKE '%Holiday%' OR lt."Name" ILIKE '%Sick leave%' OR lt."Name" ILIKE '%Other leave%')
          AND c."Date" >= COALESCE(lt."StartDate", ljd."StartDate")
          AND c."Date" <= COALESCE(lt."DueDate",
                  CASE WHEN ljd."CompletedDate" IS NULL THEN ljd."DueDate"
                       ELSE LEAST(ljd."CompletedDate", ljd."DueDate") END)
    ) lv ON TRUE;


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
    s.*,
    -- Is_Workable_Day: IF(NOT PublicHoliday AND NOT WeekEnd,
    --   IF(LOOKUPVALUE(2_Staff_Task_Allocation_byDay[Is_Full_Day_Leave],...Date,Staff_Name)=FALSE, TRUE, FALSE))
    --   Is_Full_Day_Leave proxy: leave task for this staff covering this date with Initial_Avg_Mins_perWorkDay=480
    CASE WHEN NOT c."PublicHoliday" AND NOT c."WeekEnd" THEN
        NOT EXISTS (
            SELECT 1
            FROM "1_Job_Task_Details_Table" jt
            WHERE jt."Staff_Name" = s."Staff_Name"
              AND jt."Is_Task_a_Leave" = TRUE
              AND c."Date" >= jt."StartDateAdjusted"
              AND c."Date" <= jt."DueDateAdjusted"
              AND jt."Initial_Avg_Mins_perWorkDay" = 480
        )
    END                                                           AS "Is_Workable_Day",
    -- Is_Staff_Workable_DayOfWeek: LOOKUPVALUE(excel_workable_days[working_day], day_of_week, Weekday, staffname, Staff_Name)
    (SELECT wd.working_day
     FROM excel_workable_days wd
     WHERE wd.staffname = s."Staff_Name"
       AND wd.day_of_week = c."Weekday"
     LIMIT 1)                                                     AS "Is_Staff_Workable_DayOfWeek",
    -- Adjustment_Factor_by_Month: LOOKUPVALUE(excel_staff_adjustment_sheet[AdjustmentFactor], month, StartOfMonth, staffname, Staff_Name)
    --   Returns 0 if no match (DAX: IF(ISBLANK(AdjFac), 0, AdjFac))
    COALESCE(
        (SELECT adj.adjustmentfactor
         FROM excel_staff_adjustment_sheet adj
         WHERE adj.staffname = s."Staff_Name"
           AND adj.month::date = c."StartOfMonth"
         LIMIT 1),
        0
    )                                                             AS "Adjustment_Factor_by_Month",
    -- Week_Of_Month: 1 + WEEKNUM(Date) - WEEKNUM(STARTOFMONTH(Date))
    1 + EXTRACT(WEEK FROM c."Date")::int
      - EXTRACT(WEEK FROM DATE_TRUNC('month', c."Date")::date)::int
                                                                  AS "Week_Of_Month",
    -- Overall_Recordable_Hours: placeholder — update orh LATERAL when DAX is provided
    orh.val                                                       AS "Overall_Recordable_Hours",
    -- Allocated_Holiday_Hours: SUM(Allo_Hrs_perWorkableDay_Final_Output) for Holiday tasks * -1
    --   Guard: IF(Overall_Recordable_Hours=0, BLANK(), ...)
    CASE WHEN orh.val = 0 THEN NULL
         ELSE -hol.hrs
    END                                                           AS "Allocated_Holiday_Hours",
    -- Allocated_Other_Leave_Hours: SUM for Sick leave / Other leave tasks * -1
    CASE WHEN orh.val = 0 THEN NULL
         ELSE -lvl.hrs
    END                                                           AS "Allocated_Other_Leave_Hours",
    -- Non_Leave_Recordable_Hours: IF(Overall=0, 0, Overall + Holiday + OtherLeave)
    --   Holiday and OtherLeave are already negative, BLANK()→0 in DAX addition
    CASE WHEN orh.val = 0 THEN 0
         ELSE orh.val + COALESCE(-hol.hrs, 0) + COALESCE(-lvl.hrs, 0)
    END                                                           AS "Non_Leave_Recordable_Hours",
    -- Allocated_Dinniss_Admin_Hours: SUM for Task_Category="Admin Tasks" (no * -1)
    CASE WHEN orh.val = 0 THEN NULL
         ELSE adm.hrs
    END                                                           AS "Allocated_Dinniss_Admin_Hours",
    -- Available_Billable_Hours = Non_Leave_Recordable_Hours - Allocated_Dinniss_Admin_Hours
    --   Guard: IF(Overall=0, 0, Logic)
    CASE WHEN orh.val = 0 THEN 0
         ELSE orh.val + COALESCE(-hol.hrs, 0) + COALESCE(-lvl.hrs, 0) - COALESCE(adm.hrs, 0)
    END                                                           AS "Available_Billable_Hours",
    -- Allocated_Billable_Hours_Original: SUM(Allo_Hrs_perWorkDay_AdjLeaves_FIN01) for Billable Tasks
    --   Guard: IF(Overall=0, BLANK(), ...)
    CASE WHEN orh.val = 0 THEN NULL
         ELSE bil_orig.hrs
    END                                                           AS "Allocated_Billable_Hours_Original",
    -- Allocated_Billable_Hours_Capacity_Planning: SUM(Allo_Hrs_perWorkableDay_Final_Output) for Billable Tasks
    --   Adjusted for task hours worked/remaining
    CASE WHEN orh.val = 0 THEN NULL
         ELSE bil_cap.hrs
    END                                                           AS "Allocated_Billable_Hours_Capacity_Planning",
    -- Target_Billable_Hours: Available_Billable_Hours * LOOKUPVALUE(excel_incentive_targets[target_billable_hours], staff_name, StartOfMonth=month_year)
    CASE WHEN orh.val = 0 THEN 0
         ELSE (orh.val + COALESCE(-hol.hrs, 0) + COALESCE(-lvl.hrs, 0) - COALESCE(adm.hrs, 0))
              * itgt.target_billable_hours
    END                                                           AS "Target_Billable_Hours",
    -- Target_Non_Leave_Hours: Non_Leave_Recordable_Hours * target_billable_hours; IF(Logic<=0, BLANK(), Logic)
    CASE WHEN tnl.val <= 0 THEN NULL
         ELSE tnl.val
    END                                                           AS "Target_Non_Leave_Hours",
    -- M2Date_Overall_Recordable_Hours: BLANK() if Date >= TODAY (future/today), else Overall_Recordable_Hours
    CASE WHEN c."Date" >= CURRENT_DATE THEN NULL
         ELSE orh.val
    END                                                           AS "M2Date_Overall_Recordable_Hours",
    -- M2Date_Target_Billable_Hours: BLANK() if Date >= TODAY, else Target_Billable_Hours
    CASE WHEN c."Date" >= CURRENT_DATE THEN NULL
         WHEN orh.val = 0               THEN 0
         ELSE (orh.val + COALESCE(-hol.hrs, 0) + COALESCE(-lvl.hrs, 0) - COALESCE(adm.hrs, 0))
              * itgt.target_billable_hours
    END                                                           AS "M2Date_Target_Billable_Hours",
    -- Annual_Leave_Hours_Recorded: SUM(Recorded_Minutes/60) from 4_Timesheet_Table for Holiday tasks * -1
    -alr.hrs                                                      AS "Annual_Leave_Hours_Recorded",
    -- Other_Leave_Hours_Recorded: SUM(Recorded_Minutes/60) for Sick leave / Other leave tasks * -1
    -olr.hrs                                                      AS "Other_Leave_Hours_Recorded",
    -- Dinniss_Admin_Hours_Recorded: SUM(Recorded_Minutes/60) for Admin Tasks (positive, no * -1)
    dar.hrs                                                       AS "Dinniss_Admin_Hours_Recorded",
    -- Billable_Hours_Recorded: SUM(Recorded_Minutes/60) for Billable Tasks (positive, no * -1)
    bhr.hrs                                                       AS "Billable_Hours_Recorded",
    -- Overall_Hours_Recorded: (Annual + Other) * -1 + Billable + Admin
    --   Annual and Other are already negative, so * -1 makes them positive: = |Annual| + |Other| + Billable + Admin
    COALESCE(alr.hrs, 0) + COALESCE(olr.hrs, 0)
    + COALESCE(bhr.hrs, 0) + COALESCE(dar.hrs, 0)               AS "Overall_Hours_Recorded",
    -- Billable_Hours_Invoiced: SUM(Invoiced_Minutes/60) where Invoiced_Time="Invoiced" AND Billable Tasks
    bhi.hrs                                                       AS "Billable_Hours_Invoiced",
    -- Billable_Hours_To_Be_Invoiced: SUM(Recorded_Minutes/60) where Invoiced_Time="Un-Invoiced" AND Billable Tasks
    btu.hrs                                                       AS "Billable_Hours_To_Be_Invoiced",
    -- M2Date_Overall_Hours_Recorded: BLANK() if Date >= TODAY, else Overall_Hours_Recorded
    CASE WHEN c."Date" >= CURRENT_DATE THEN NULL
         ELSE COALESCE(alr.hrs, 0) + COALESCE(olr.hrs, 0)
              + COALESCE(bhr.hrs, 0) + COALESCE(dar.hrs, 0)
    END                                                           AS "M2Date_Overall_Hours_Recorded",
    -- Allocated_Non_Leave_Hours_Capacity_Planning_CHECK_VARIABLES:
    --   SUM(Allo_Hrs_perWorkableDay_Final_Output) for Admin OR Billable Tasks
    --   Guard: IF(Overall_Recordable_Hours=0, BLANK(), ...)
    CASE WHEN orh.val = 0 THEN NULL
         ELSE anl.hrs
    END                                                           AS "Allocated_Non_Leave_Hours_Capacity_Planning_CHECK_VARIABLES",
    -- Adjustment_Factor_by_Day: LOOKUPVALUE(excel_workable_days[adjustment_factor], day_of_week, staffname); default 0
    COALESCE(
        (SELECT wd.adjustment_factor
         FROM excel_workable_days wd
         WHERE wd.staffname = s."Staff_Name"
           AND wd.day_of_week = c."Weekday"
         LIMIT 1),
        0
    )                                                             AS "Adjustment_Factor_by_Day",
    -- Is_Current_Month: Date falls in the same year+month as TODAY
    (EXTRACT(YEAR  FROM c."Date") = EXTRACT(YEAR  FROM CURRENT_DATE)
     AND EXTRACT(MONTH FROM c."Date") = EXTRACT(MONTH FROM CURRENT_DATE))
                                                                  AS "Is_Current_Month",
    -- Target_Hours_to_be_Recorded: Target_Billable_Hours * target_recorded_2_billable_hrs; IF(Logic<=0, BLANK(), Logic)
    CASE WHEN trec.val <= 0 THEN NULL
         ELSE trec.val
    END                                                           AS "Target_Hours_to_be_Recorded",
    -- Target_Hours_to_be_Allocated: Target_Billable_Hours * target_allocated_2_billable_hrs; IF(Logic<=0, BLANK(), Logic)
    CASE WHEN talloc.val <= 0 THEN NULL
         ELSE talloc.val
    END                                                           AS "Target_Hours_to_be_Allocated",
    -- Target_Hours_to_be_Invoiced: Target_Billable_Hours * target_invoiced_2_billable_hrs; IF(Logic<=0, BLANK(), Logic)
    CASE WHEN tinv.val <= 0 THEN NULL
         ELSE tinv.val
    END                                                           AS "Target_Hours_to_be_Invoiced",
    -- Target_Billable_Percent: LOOKUPVALUE(target_billable_hours, month_year=Date); IF(=0, BLANK(), val)
    --   Note: month_year is first-of-month, so only matches when Date = 1st of month
    NULLIF(pitgt.target_billable_hours, 0)                        AS "Target_Billable_Percent",
    -- Target_Recordable_Percent: LOOKUPVALUE(target_recorded_2_billable_hrs, month_year=Date)
    NULLIF(pitgt.target_recorded_2_billable_hrs, 0)               AS "Target_Recordable Percent",
    -- Target_Allocation_Percent: LOOKUPVALUE(target_allocated_2_billable_hrs, month_year=Date)
    NULLIF(pitgt.target_allocated_2_billable_hrs, 0)              AS "Target_Allocation_Percent",
    -- Target_Invoice_Percent: LOOKUPVALUE(target_invoiced_2_billable_hrs, month_year=Date)
    NULLIF(pitgt.target_invoiced_2_billable_hrs, 0)               AS "Target_Invoice_Percent"
FROM
    key01_calendar_date c
    CROSS JOIN key03_staff_table s
    CROSS JOIN LATERAL (
        -- orh: placeholder for Overall_Recordable_Hours (TBD — update when DAX provided)
        SELECT NULL::double precision AS val
    ) orh
    CROSS JOIN LATERAL (
        -- hol: SUM of Allo_Hrs_perWorkableDay_Final_Output for Holiday tasks, this staff, this date
        SELECT SUM(a."Allo_Hrs_perWorkableDay_Final_Output") AS hrs
        FROM "2_Staff_Task_Allocation_byDay" a
        WHERE a."Staff_Name" = s."Staff_Name"
          AND a."Date" = c."Date"
          AND a."Task_Name" ILIKE '%Holiday%'
    ) hol
    CROSS JOIN LATERAL (
        -- lvl: SUM for Sick leave / Other leave tasks
        SELECT SUM(a."Allo_Hrs_perWorkableDay_Final_Output") AS hrs
        FROM "2_Staff_Task_Allocation_byDay" a
        WHERE a."Staff_Name" = s."Staff_Name"
          AND a."Date" = c."Date"
          AND (a."Task_Name" ILIKE '%Sick leave%' OR a."Task_Name" ILIKE '%Other leave%')
    ) lvl
    CROSS JOIN LATERAL (
        -- adm: SUM for Admin Tasks (Task_Category = 'Admin Tasks')
        SELECT SUM(a."Allo_Hrs_perWorkableDay_Final_Output") AS hrs
        FROM "2_Staff_Task_Allocation_byDay" a
        WHERE a."Staff_Name" = s."Staff_Name"
          AND a."Date" = c."Date"
          AND a."Task_Category" = 'Admin Tasks'
    ) adm
    CROSS JOIN LATERAL (
        -- bil_orig: SUM of Allo_Hrs_perWorkDay_AdjLeaves_FIN01 for Billable Tasks
        SELECT SUM(a."Allo_Hrs_perWorkDay_AdjLeaves_FIN01") AS hrs
        FROM "2_Staff_Task_Allocation_byDay" a
        WHERE a."Staff_Name" = s."Staff_Name"
          AND a."Date" = c."Date"
          AND a."Task_Category" = 'Billable Tasks'
    ) bil_orig
    CROSS JOIN LATERAL (
        -- bil_cap: SUM of Allo_Hrs_perWorkableDay_Final_Output for Billable Tasks
        SELECT SUM(a."Allo_Hrs_perWorkableDay_Final_Output") AS hrs
        FROM "2_Staff_Task_Allocation_byDay" a
        WHERE a."Staff_Name" = s."Staff_Name"
          AND a."Date" = c."Date"
          AND a."Task_Category" = 'Billable Tasks'
    ) bil_cap
    LEFT JOIN LATERAL (
        -- itgt: LOOKUPVALUE(excel_incentive_targets[target_*], staff_name, month_year=StartOfMonth)
        SELECT it.target_billable_hours,
               it.target_recorded_2_billable_hrs,
               it.target_allocated_2_billable_hrs,
               it.target_invoiced_2_billable_hrs
        FROM excel_incentive_targets it
        WHERE it.staff_name = s."Staff_Name"
          AND it.month_year::date = c."StartOfMonth"
        LIMIT 1
    ) itgt ON TRUE
    CROSS JOIN LATERAL (
        -- tnl: Non_Leave_Recordable_Hours * target_billable_hours (pre-computed for Target_Non_Leave_Hours guard)
        SELECT (CASE WHEN orh.val = 0 THEN 0
                     ELSE orh.val + COALESCE(-hol.hrs, 0) + COALESCE(-lvl.hrs, 0)
                END) * itgt.target_billable_hours AS val
    ) tnl
    CROSS JOIN LATERAL (
        -- trec: Target_Billable_Hours * target_recorded_2_billable_hrs (for Target_Hours_to_be_Recorded guard)
        SELECT (CASE WHEN orh.val = 0 THEN 0
                     ELSE (orh.val + COALESCE(-hol.hrs, 0) + COALESCE(-lvl.hrs, 0) - COALESCE(adm.hrs, 0))
                          * itgt.target_billable_hours
                END) * itgt.target_recorded_2_billable_hrs AS val
    ) trec
    CROSS JOIN LATERAL (
        -- talloc: Target_Billable_Hours * target_allocated_2_billable_hrs (for Target_Hours_to_be_Allocated guard)
        SELECT (CASE WHEN orh.val = 0 THEN 0
                     ELSE (orh.val + COALESCE(-hol.hrs, 0) + COALESCE(-lvl.hrs, 0) - COALESCE(adm.hrs, 0))
                          * itgt.target_billable_hours
                END) * itgt.target_allocated_2_billable_hrs AS val
    ) talloc
    CROSS JOIN LATERAL (
        -- tinv: Target_Billable_Hours * target_invoiced_2_billable_hrs (for Target_Hours_to_be_Invoiced guard)
        SELECT (CASE WHEN orh.val = 0 THEN 0
                     ELSE (orh.val + COALESCE(-hol.hrs, 0) + COALESCE(-lvl.hrs, 0) - COALESCE(adm.hrs, 0))
                          * itgt.target_billable_hours
                END) * itgt.target_invoiced_2_billable_hrs AS val
    ) tinv
    LEFT JOIN LATERAL (
        -- pitgt: percent targets looked up by month_year = Date (only rows where Date = first of month will match)
        SELECT it.target_billable_hours,
               it.target_recorded_2_billable_hrs,
               it.target_allocated_2_billable_hrs,
               it.target_invoiced_2_billable_hrs
        FROM excel_incentive_targets it
        WHERE it.staff_name = s."Staff_Name"
          AND it.month_year::date = c."Date"
        LIMIT 1
    ) pitgt ON TRUE
    CROSS JOIN LATERAL (
        -- alr: SUM(Recorded_Minutes)/60 from 4_Timesheet_Table for Holiday tasks, this staff, this date
        SELECT SUM(ts."Recorded_Minutes") / 60.0 AS hrs
        FROM "4_Timesheet_Table" ts
        WHERE ts."Staff_ID" = s."Staff_UUID"
          AND ts."Task_Name" ILIKE '%Holiday%'
          AND ts."Date"::date = c."Date"
    ) alr
    CROSS JOIN LATERAL (
        -- olr: SUM(Recorded_Minutes)/60 for Sick leave / Other leave tasks (matched by Staff_ID = Staff_UUID)
        SELECT SUM(ts."Recorded_Minutes") / 60.0 AS hrs
        FROM "4_Timesheet_Table" ts
        WHERE ts."Staff_ID" = s."Staff_UUID"
          AND (ts."Task_Name" ILIKE '%Sick leave%' OR ts."Task_Name" ILIKE '%Other leave%')
          AND ts."Date"::date = c."Date"
    ) olr
    CROSS JOIN LATERAL (
        -- dar: SUM(Recorded_Minutes)/60 for Admin Tasks (matched by Staff_Name)
        SELECT SUM(ts."Recorded_Minutes") / 60.0 AS hrs
        FROM "4_Timesheet_Table" ts
        WHERE ts."Staff_Name" = s."Staff_Name"
          AND ts."Task_Category" = 'Admin Tasks'
          AND ts."Date"::date = c."Date"
    ) dar
    CROSS JOIN LATERAL (
        -- bhr: SUM(Recorded_Minutes)/60 for Billable Tasks (matched by Staff_Name)
        SELECT SUM(ts."Recorded_Minutes") / 60.0 AS hrs
        FROM "4_Timesheet_Table" ts
        WHERE ts."Staff_Name" = s."Staff_Name"
          AND ts."Task_Category" = 'Billable Tasks'
          AND ts."Date"::date = c."Date"
    ) bhr
    CROSS JOIN LATERAL (
        -- bhi: SUM(Invoiced_Minutes/60) where Invoiced_Time="Invoiced" AND Billable Tasks
        SELECT SUM(ts."Invoiced_Minutes") / 60.0 AS hrs
        FROM "4_Timesheet_Table" ts
        WHERE ts."Staff_Name" = s."Staff_Name"
          AND ts."Invoiced_Time" = 'Invoiced'
          AND ts."Task_Category" = 'Billable Tasks'
          AND ts."Date"::date = c."Date"
    ) bhi
    CROSS JOIN LATERAL (
        -- btu: SUM(Recorded_Minutes/60) where Invoiced_Time="Un-Invoiced" AND Billable Tasks
        SELECT SUM(ts."Recorded_Minutes") / 60.0 AS hrs
        FROM "4_Timesheet_Table" ts
        WHERE ts."Staff_Name" = s."Staff_Name"
          AND ts."Invoiced_Time" = 'Un-Invoiced'
          AND ts."Task_Category" = 'Billable Tasks'
          AND ts."Date"::date = c."Date"
    ) btu
    CROSS JOIN LATERAL (
        -- anl: SUM(Allo_Hrs_perWorkableDay_Final_Output) for Admin Tasks OR Billable Tasks
        SELECT SUM(a."Allo_Hrs_perWorkableDay_Final_Output") AS hrs
        FROM "2_Staff_Task_Allocation_byDay" a
        WHERE a."Staff_Name" = s."Staff_Name"
          AND (a."Task_Category" = 'Admin Tasks' OR a."Task_Category" = 'Billable Tasks')
          AND a."Date" = c."Date"
    ) anl;