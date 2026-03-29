-- =============================================================================
-- PERFORMANCE OPTIMIZATION NOTES
-- =============================================================================
-- Problem: SELECT * FROM "1_Job_Task_Details_Table" LIMIT 10 was taking ~1 min.
--          CREATE MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay" was slow.
--          SELECT * FROM "3_Staff_Performance_Table" LIMIT 1 was slow.
--
-- Root cause: 1_Job_Task_Details_Table was a regular VIEW with 8 CROSS JOIN
--   LATERAL subqueries per task row, each independently scanning jobtask ×
--   jobtaskassignee × jobdetails and running a nested COUNT(*) over
--   key01_calendar_date to compute AllocatedMinutes / WorkableDays = 480
--   (Is_Full_Day_Leave). Complexity was effectively O(tasks × dates ×
--   leave_tasks × workable_days) — O(n⁴).
--
-- Optimizations applied:
--
--   1. leave_status_by_staff_date (MATERIALIZED VIEW, index on staff_name, date)
--      Pre-computes is_full_day / has_partial_leave / partial_leave_hrs_per_day
--      per (staff_name, date) once. Eliminates the inline jobtask × nested COUNT
--      subquery that previously ran for every calendar date × every task row.
--      Must be created before 1_Job_Task_Details_Table.
--
--   2. 1_Job_Task_Details_Table converted to MATERIALIZED VIEW
--      Downstream queries (4_Timesheet_Table, key02_job_task_staff_id,
--      2_Staff_Task_Allocation_byDay, 3_Staff_Performance_Table) read from
--      stored rows instead of recomputing on every query.
--      Build time: ~26s. Query time: 57ms for 1000 rows.
--
--   3. 8 calendar laterals → 1 cal_counts lateral (FILTER aggregation)
--      wdb/tlh/ttl/wdl/arwd/rttl/pwdl/pttl were 8 separate scans of
--      key01_calendar_date over the same date range per task row. Replaced
--      with one scan using COUNT(*) FILTER (WHERE ...) / SUM(...) FILTER.
--      ~8× reduction in calendar scans per task row.
--
--   4. tmt lateral: string concatenation → column comparisons
--      Changed from (t."JobID"::text || t."TaskUUID"::text || ...) = b."Job_Task_Staff_ID"
--      to t."JobID"::text = b."Job_ID"::text AND t."TaskUUID"::text = ... so
--      PostgreSQL can use indexes on the underlying time base table.
--
--   5. Base table indexes
--      excel_workable_days (staffname, day_of_week) — used in every
--      IS_Staff_Workable_DayOfWeek EXISTS check across all views.
--
--   A. 4_Timesheet_Table converted to MATERIALIZED VIEW
--      Eliminates re-execution of all joins on every downstream query.
--
--   B. 4_Timesheet_Table lookup LATERALs → regular LEFT JOINs
--      jt_lkp, e_lkp, inv_lkp, k2_lkp converted from LATERAL+LIMIT 1 to
--      regular LEFT JOINs (jt_lkp/e_lkp use DISTINCT ON subqueries to match
--      LIMIT 1 behaviour). Allows hash/merge joins instead of nested loops.
--
--   C. 2_Staff_Task_Allocation_byDay converted to MATERIALIZED VIEW
--      lv lateral (O(n⁴) inline jobtask × nested COUNT) replaced with a JOIN
--      to leave_status_by_staff_date. wkd lateral replaced with regular LEFT JOIN.
--      Indexed on (Staff_Name, Date) and (Job_Task_Staff_ID).
--
--   D. 3_Staff_Performance_Table converted to MATERIALIZED VIEW
--      Eliminates re-computation of 14+ lateral scans per calendar×staff row.
--
--   E. 3_Staff_Performance_Table Is_Workable_Day NOT EXISTS replaced with
--      leave_status_by_staff_date JOIN (lv_spt).
--
--   F. 3_Staff_Performance_Table scalar subqueries merged into regular LEFT JOINs:
--      Two excel_workable_days subqueries → wd_lkp (working_day + adjustment_factor).
--      excel_staff_adjustment_sheet subquery → adj_lkp.
--
--   G. 3_Staff_Performance_Table itgt and pitgt LATERAL+LIMIT 1 → regular LEFT JOINs.
--
--   H. 3_Staff_Performance_Table: 6 separate 2_Staff_Task_Allocation_byDay scans
--      → 1 alloc_counts FILTER lateral (6× reduction).
--      6 separate 4_Timesheet_Table scans → 1 ts_counts FILTER lateral (6× reduction).
--
--   I. key02_job_task_staff_id converted to MATERIALIZED VIEW
--      Eliminates re-executing UNION ALL + DISTINCT + per-row ILIKE scalar subquery
--      for Task_Type1 on every build of 2_Staff_Task_Allocation_byDay.
--
--   J. 2_Staff_Task_Allocation_byDay date-range WHERE filter
--      Restricts the CROSS JOIN output to rows where c.Date falls within each task's
--      StartDateAdjusted–DueDateAdjusted window. Reduces row count from
--      N_dates × N_tasks → SUM(task_duration_in_workdays).
--
--   K. 3_Staff_Performance_Table alloc_agg and ts_agg pre-aggregated LEFT JOINs
--      alloc_counts and ts_counts were CROSS JOIN LATERALs — each executed a separate
--      aggregate scan per (staff, date) row (~44,000 each). Replaced with subqueries
--      that GROUP BY (Staff_Name, Date) once, then hash-join to the outer loop:
--      2 single-pass scans + 2 hash joins replaces ~88,000 nested-loop aggregate scans.
--      Also added composite index ("Staff_Name", "Date") on 4_Timesheet_Table.
--
-- Refresh order:
--   leave_status_by_staff_date
--   → 1_Job_Task_Details_Table
--   → 4_Timesheet_Table → keys_time
--   → key02_job_task_staff_id
--   → 2_Staff_Task_Allocation_byDay
--   → 3_Staff_Performance_Table
-- =============================================================================
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
SELECT DISTINCT ON ("Name")
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
    AND "Name" IS NOT NULL
ORDER BY "Name";


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
FROM
    (
        SELECT DISTINCT
            jt."Name" AS "Task_Name",
            -- Task_Type1: CONCATENATEX over key05_task_type, collecting type names found (via SEARCH/ILIKE) in the task name
            --   DAX: CONCATENATEX(KEY05_Task_Type, IF(SEARCH(key05[Name], task_name,,999)<>999, key05[Name]," "))
            COALESCE(
                NULLIF(
                    TRIM(
                        (
                            SELECT
                                STRING_AGG(kt."Name", ' ')
                            FROM
                                key05_task_type kt
                            WHERE
                                jt."Name" ILIKE ('%' || kt."Name" || '%')
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


-- =============================================================================
-- Materialized views have been moved to 02_create_materialized_views.sql.
-- Run 01_create_views.sql first, then 02_create_materialized_views.sql.
-- =============================================================================
