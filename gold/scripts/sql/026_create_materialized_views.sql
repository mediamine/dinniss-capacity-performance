-- =============================================================================
-- MATERIALIZED VIEWS - Staff Task Allocation by Day
-- =============================================================================
-- Run AFTER 010_create_views.sql and 025_create_materialized_views.sql
-- This view creates the per-day task allocation matrix by cross-joining calendar dates
-- with unique job-task-staff combinations. It serves as the foundation for per-day
-- allocation calculations and staff performance metrics.
--
-- For daily refresh use 030_refresh_materialized_views.sql instead.
--
-- Creation order (dependency chain):
--   1. 2_Staff_Task_Allocation_byDay_base  (depends on key01_calendar_date from 010_create_views.sql, KEY02_Job_Task_Staff_ID from 025_create_materialized_views.sql)
--   2. 2_Staff_Task_Allocation_byDay_base_1 (depends on #1, 1_Job_Task_Details_Table_base_1 from 025_create_materialized_views.sql, EXCEL01_Staff_Workable_Days from 021_create_materialized_views.sql; uses CTE + LEFT JOIN to consolidate intermediate calculations into a single MV write)
--   3. 3_Staff_Performance_Table_base      (depends on key01_calendar_date, key03_staff_table from 010_create_views.sql)
--   4. SUPPORT_Job_Leave_Task_Details_Table_base_2 (depends on SUPPORT_Job_Leave_Task_Details_Table_base_1 from 025, #2)
--   5. SUPPORT_Job_Leave_Task_Details_Table (depends on #4)
--   6. SUPPORT_Staff_Leave_Allocation_byDay (depends on SUPPORT_Staff_Leave_Allocation_byDay_base_2 from 025, #5)
--   7. 2_Staff_Task_Allocation_byDay_base_2 (depends on #2, #6)
--   8. 1_Job_Task_Details_Table_base_2      (depends on 1_Job_Task_Details_Table_base_1 from 025, #7)
--   9. 2_Staff_Task_Allocation_byDay_base_3 (depends on #7, #8)
--  10. 1_Job_Task_Details_Table_base_3      (depends on #8, #9)
--  11. 2_Staff_Task_Allocation_byDay       (depends on #9, #10)
--  12. 1_Job_Task_Details_Table            (depends on #10, #11)
--
-- Note: SUPPORT_Staff_Leave_Allocation_byDay_base and SUPPORT_Staff_Leave_Allocation_byDay_base_2 are created in 025_create_materialized_views.sql
-- =============================================================================
-- 2_Staff_Task_Allocation_byDay_base
-- DAX equivalent: 2_Staff_Task_Allocation_byDay = CROSSJOIN(KEY01_CalendarDate, KEY02_Job_Task_Staff_ID)
-- Base view combining every calendar date with every unique job-task-staff combination.
-- This creates the foundation for per-day task allocation calculations.
-- Dependencies: key01_calendar_date (from 010_create_views.sql), KEY02_Job_Task_Staff_ID (from 025_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay_base" CASCADE;


CREATE MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay_base" AS
SELECT
    c."Date",
    c."PublicHoliday",
    c."Weekday",
    c."WeekEnd",
    c."StartOfMonth",
    c."EndOfMonth",
    c."Is_Range_for_Invoicing",
    k."Job_Task_Staff_ID",
    k."Job_ID",
    k."Staff_Name",
    k."StartDateAdjusted",
    k."DueDateAdjusted",
    k."Task_Name",
    k."Client_Name",
    k."Job_Name",
    k."Task_Category",
    k."Task_Type1",
    k."Task_Type"
FROM
    key01_calendar_date c
    CROSS JOIN KEY02_Job_Task_Staff_ID k;


CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base" ("Date");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base" ("Job_Task_Staff_ID");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base" ("Staff_Name");


-- 2_Staff_Task_Allocation_byDay_base_1
-- DAX equivalent: Daily task allocation with task date range, client, workability and billability flags
-- Extends 2_Staff_Task_Allocation_byDay_base with task date range validation, client classification,
-- workability, and billability calculations — all consolidated into a single MV write.
--
-- Previously built via two separate MVs (_base_1 + final) with per-row correlated subqueries.
-- Rewritten to use CTEs + LEFT JOIN so PostgreSQL can build a hash table once per lookup
-- and probe it O(1) per row, instead of doing a B-tree lookup for every single row.
-- The _r-suffixed columns inside the `enriched` CTE exist so the outer SELECT can reference
-- computed values (e.g., Is_Billable depends on Is_Client_r).
--
-- Dependencies: 2_Staff_Task_Allocation_byDay_base (#1), 1_Job_Task_Details_Table_base_1 (from 025_create_materialized_views.sql),
--               EXCEL01_Staff_Workable_Days (from 021_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay_base_3" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay_base_1" CASCADE;


CREATE MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay_base_1" AS
WITH jtd AS (
    -- Deduplicate 1_Job_Task_Details_Table by Job_Task_Staff_ID (preserves LIMIT 1 semantics)
    SELECT DISTINCT ON ("Job_Task_Staff_ID")
        "Job_Task_Staff_ID",
        "StartDateAdjusted",
        "DueDateAdjusted"
    FROM "1_Job_Task_Details_Table_base_1"
    ORDER BY "Job_Task_Staff_ID"
),
wd AS (
    -- Deduplicate EXCEL01_Staff_Workable_Days by (Day of Week, StaffName)
    SELECT DISTINCT ON ("Day of Week", "StaffName")
        "Day of Week",
        "StaffName",
        "Working Day"
    FROM EXCEL01_Staff_Workable_Days
    ORDER BY "Day of Week", "StaffName"
),
enriched AS (
    SELECT
        b.*,
        -- StartDateAdjusted: LOOKUPVALUE(1_Job_Task_Details_Table[StartDateAdjusted], Job_Task_Staff_ID)
        COALESCE(j."StartDateAdjusted", b."StartDateAdjusted") AS "StartDateAdjusted_r",
        -- DueDateAdjusted: LOOKUPVALUE(1_Job_Task_Details_Table[DueDateAdjusted], Job_Task_Staff_ID)
        COALESCE(j."DueDateAdjusted",   b."DueDateAdjusted")   AS "DueDateAdjusted_r",
        -- Is_Staff_Workable_DayOfWeek: LOOKUPVALUE(EXCEL01_Staff_Workable_Days[Working Day], Day of Week, Weekday, StaffName, Staff_Name)
        COALESCE(w."Working Day", FALSE)                        AS "Is_Staff_Workable_DayOfWeek_r",
        -- Is_Client: IF(OR(Client_Name="Dinniss Admin", Task_Type1="Admin - Non-billable"), FALSE, TRUE)
        CASE
            WHEN b."Client_Name" = 'Dinniss Admin'
              OR b."Task_Type1" ILIKE '%Admin - Non-billable%' THEN FALSE
            ELSE TRUE
        END AS "Is_Client_r"
    FROM "2_Staff_Task_Allocation_byDay_base" b
    LEFT JOIN jtd j ON j."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    LEFT JOIN wd  w ON w."Day of Week" = b."Weekday" AND w."StaffName" = b."Staff_Name"
)
SELECT
    "Date",
    "PublicHoliday",
    "Weekday",
    "WeekEnd",
    "StartOfMonth",
    "EndOfMonth",
    "Is_Range_for_Invoicing",
    "Job_Task_Staff_ID",
    "Job_ID",
    "Staff_Name",
    "StartDateAdjusted_r" AS "StartDateAdjusted",
    "DueDateAdjusted_r"   AS "DueDateAdjusted",
    "Task_Name",
    "Client_Name",
    "Job_Name",
    "Task_Category",
    "Task_Type1",
    "Task_Type",
    "Is_Client_r" AS "Is_Client",
    -- Is_Billable: IF(OR(Is_Client=FALSE, Task_Type1="Coaching"), FALSE, TRUE)
    CASE
        WHEN "Is_Client_r" = FALSE OR "Task_Type1" ILIKE '%Coaching%' THEN FALSE
        ELSE TRUE
    END AS "Is_Billable",
    -- Is_Workable_Day: IF(AND(WeekEnd=FALSE, PublicHoliday=FALSE), TRUE, FALSE)
    CASE
        WHEN "WeekEnd" = FALSE AND "PublicHoliday" = FALSE THEN TRUE
        ELSE FALSE
    END AS "Is_Workable_Day",
    -- Is_Date_Between_Task_Days: IF(AND(Date >= StartDateAdjusted, Date <= DueDateAdjusted), TRUE, FALSE)
    CASE
        WHEN "Date" >= "StartDateAdjusted_r" AND "Date" <= "DueDateAdjusted_r" THEN TRUE
        ELSE FALSE
    END AS "Is_Date_Between_Task_Days",
    "Is_Staff_Workable_DayOfWeek_r" AS "Is_Staff_Workable_DayOfWeek",
    -- Is_Task_a_Leave: IF(OR(CONTAINSSTRING(Task_Name, "Holiday"), CONTAINSSTRING(Task_Name, "Sick Leave")), TRUE, IF(CONTAINSSTRING(Task_Name, "Other leave"), TRUE, FALSE))
    CASE
        WHEN "Task_Name" ILIKE '%Holiday%' OR "Task_Name" ILIKE '%Sick Leave%' THEN TRUE
        WHEN "Task_Name" ILIKE '%Other leave%' THEN TRUE
        ELSE FALSE
    END AS "Is_Task_a_Leave"
FROM enriched;


CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_1" ("Date");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_1" ("Job_Task_Staff_ID");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_1" ("Staff_Name");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_1" ("Is_Billable");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_1" ("Is_Workable_Day");


-- 3_Staff_Performance_Table_base
-- DAX equivalent: 3_Staff_Performance_Table = CROSSJOIN(KEY01_CalendarDate, KEY03_Staff_Table)
-- Base view combining every calendar date with every unique staff member.
-- This creates the foundation for per-day staff performance metrics and utilization calculations.
-- Dependencies: key01_calendar_date, key03_staff_table (from 010_create_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "3_Staff_Performance_Table_base" CASCADE;


CREATE MATERIALIZED VIEW "3_Staff_Performance_Table_base" AS
SELECT
    c."Date",
    c."PublicHoliday",
    c."Weekday",
    c."WeekEnd",
    c."StartOfMonth",
    c."EndOfMonth",
    c."Is_Range_for_Invoicing",
    s."Staff_UUID",
    s."Staff_Name"
FROM
    key01_calendar_date c
    CROSS JOIN key03_staff_table s;


CREATE INDEX ON "3_Staff_Performance_Table_base" ("Date");
CREATE INDEX ON "3_Staff_Performance_Table_base" ("Staff_Name");
CREATE INDEX ON "3_Staff_Performance_Table_base" ("Staff_UUID");


-- SUPPORT_Job_Leave_Task_Details_Table_base_2
-- DAX equivalent: Leave task details with workable days between task calculation
-- Extends SUPPORT_Job_Leave_Task_Details_Table_base_1 with count of workable days within task date range.
-- Workable_Days_Between_Task: Counts rows from 2_Staff_Task_Allocation_byDay_base_1 where Job_Task_Staff_ID matches,
--   Is_Date_Between_Task_Days=TRUE, Is_Workable_Day=TRUE, and Is_Staff_Workable_DayOfWeek=TRUE.
-- Dependencies: SUPPORT_Job_Leave_Task_Details_Table_base_1 (from 025_create_materialized_views.sql), 2_Staff_Task_Allocation_byDay_base_1 (#2)
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Job_Leave_Task_Details_Table CASCADE;
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Job_Leave_Task_Details_Table_base_2 CASCADE;


CREATE MATERIALIZED VIEW SUPPORT_Job_Leave_Task_Details_Table_base_2 AS
SELECT
    b."Job_Task_Staff_ID",
    b."Job_ID",
    b."Task_Name",
    b."Task_UUID",
    b."Staff_Name",
    b."Staff_UUID",
    b."Client_Name",
    b."StartDate",
    b."DueDate",
    b."Task_Allocated_Mins",
    b."Initial_Avg_Mins_perWorkDay",
    b."StartDateAdjusted",
    b."DueDateAdjusted",
    -- Workable_Days_Between_Task: COUNT rows from 2_Staff_Task_Allocation_byDay matching conditions
    -- CALCULATE(COUNTROWS(2_Staff_Task_Allocation_byDay),
    --   FILTER(Job_Task_Staff_ID = match), FILTER(Is_Date_Between_Task_Days=TRUE),
    --   FILTER(Is_Workable_Day=TRUE), FILTER(Is_Staff_Workable_DayOfWeek=TRUE))
    (
        SELECT COUNT(*)
        FROM "2_Staff_Task_Allocation_byDay_base_1" d
        WHERE d."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
            AND d."Is_Date_Between_Task_Days" = TRUE
            AND d."Is_Workable_Day" = TRUE
            AND d."Is_Staff_Workable_DayOfWeek" = TRUE
    ) AS "Workable_Days_Between_Task"
FROM
    SUPPORT_Job_Leave_Task_Details_Table_base_1 b;


CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table_base_2 ("Job_Task_Staff_ID");
CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table_base_2 ("Staff_Name");
CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table_base_2 ("Job_ID");
CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table_base_2 ("Workable_Days_Between_Task");


-- SUPPORT_Job_Leave_Task_Details_Table
-- DAX equivalent: Leave task details with average daily hours calculation
-- Extends SUPPORT_Job_Leave_Task_Details_Table_base_2 with per-workday averages.
-- Initial_Avg_Mins_perWorkDay: DIVIDE(Task_Allocated_Mins, Workable_Days_Between_Task, BLANK())
-- Avg_Daily_Hours: Initial_Avg_Mins_perWorkDay / 60
-- Dependencies: SUPPORT_Job_Leave_Task_Details_Table_base_2
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Job_Leave_Task_Details_Table CASCADE;


CREATE MATERIALIZED VIEW SUPPORT_Job_Leave_Task_Details_Table AS
SELECT
    b."Job_Task_Staff_ID",
    b."Job_ID",
    b."Task_Name",
    b."Task_UUID",
    b."Staff_Name",
    b."Staff_UUID",
    b."Client_Name",
    b."StartDate",
    b."DueDate",
    b."Task_Allocated_Mins",
    b."StartDateAdjusted",
    b."DueDateAdjusted",
    b."Workable_Days_Between_Task",
    -- Initial_Avg_Mins_perWorkDay: DIVIDE(Task_Allocated_Mins, Workable_Days_Between_Task, BLANK())
    CASE
        WHEN b."Workable_Days_Between_Task" > 0 THEN
            b."Task_Allocated_Mins"::NUMERIC / b."Workable_Days_Between_Task"::NUMERIC
        ELSE NULL
    END AS "Initial_Avg_Mins_perWorkDay",
    -- Avg_Daily_Hours: Initial_Avg_Mins_perWorkDay / 60
    CASE
        WHEN b."Workable_Days_Between_Task" > 0 THEN
            (b."Task_Allocated_Mins"::NUMERIC / b."Workable_Days_Between_Task"::NUMERIC) / 60.0
        ELSE NULL
    END AS "Avg_Daily_Hours"
FROM
    SUPPORT_Job_Leave_Task_Details_Table_base_2 b;


CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table ("Job_Task_Staff_ID");
CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table ("Staff_Name");
CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table ("Job_ID");
CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table ("Workable_Days_Between_Task");


-- SUPPORT_Staff_Leave_Allocation_byDay
-- DAX equivalent: Leave allocation view with allocated leave hours per workday calculation
-- Extends SUPPORT_Staff_Leave_Allocation_byDay_base_2 with leave hours per workday lookup and calculation.
-- Allo_Leave_Hrs_perWorkday: VAR Logic = IF(AND(Is_WorkableDay=TRUE, Is_DateBetweenTask=TRUE),
--   LOOKUPVALUE(SUPPORT_Job_Leave_Task_Details_Table[Avg_Daily_Hours], Job_Task_Staff_ID), BLANK())
--   RETURN IF(Is_Staff_Workable_DayOfWeek=TRUE, Logic, BLANK())
-- Dependencies: SUPPORT_Staff_Leave_Allocation_byDay_base_2 (from 025_create_materialized_views.sql), SUPPORT_Job_Leave_Task_Details_Table (#6)
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Staff_Leave_Allocation_byDay CASCADE;


CREATE MATERIALIZED VIEW SUPPORT_Staff_Leave_Allocation_byDay AS
WITH leave_base AS (
    SELECT
        b."Job_Task_Staff_ID",
        b."Job_ID",
        b."Staff_Name",
        b."StartDateAdjusted",
        b."DueDateAdjusted",
        b."Task_Name",
        b."Client_Name",
        b."Job_Name",
        b."Task_Category",
        b."Task_Type1",
        b."Task_Type",
        b."Date",
        b."PublicHoliday",
        b."Weekday",
        b."WeekEnd",
        b."StartOfMonth",
        b."EndOfMonth",
        b."Is_Range_for_Invoicing",
        b."Is_WorkableDay",
        b."AdjustedStartDate",
        b."AdjustedDueDate",
        b."Is_Staff_Workable_DayOfWeek",
        b."Is_DateBetweenTask",
        -- Allo_Leave_Hrs_perWorkday:
        -- IF(AND(Is_WorkableDay=TRUE, Is_DateBetweenTask=TRUE), LOOKUPVALUE(SUPPORT_Job_Leave_Task_Details_Table[Avg_Daily_Hours], Job_Task_Staff_ID), BLANK())
        -- IF(Is_Staff_Workable_DayOfWeek=TRUE, result, BLANK())
        CASE
            WHEN b."Is_Staff_Workable_DayOfWeek" = TRUE THEN
                CASE
                    WHEN b."Is_WorkableDay" = TRUE AND b."Is_DateBetweenTask" = TRUE THEN
                        (
                            SELECT
                                sjl."Avg_Daily_Hours"
                            FROM SUPPORT_Job_Leave_Task_Details_Table sjl
                            WHERE sjl."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
                            LIMIT 1
                        )
                    ELSE NULL
                END
            ELSE NULL
        END AS "Allo_Leave_Hrs_perWorkday"
    FROM
        SUPPORT_Staff_Leave_Allocation_byDay_base_2 b
)
SELECT
    lb.*,
    -- Full_Leave_Days: IF(Allo_Leave_Hrs_perWorkday = 8, 1, BLANK())
    CASE
        WHEN lb."Allo_Leave_Hrs_perWorkday" = 8 THEN 1
        ELSE NULL
    END AS "Full_Leave_Days"
FROM leave_base lb;


CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay ("Date");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay ("Staff_Name");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay ("Job_Task_Staff_ID");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay ("Is_DateBetweenTask");


-- 2_Staff_Task_Allocation_byDay_base_2
-- DAX equivalent: Daily task allocation view with leave flags, billable selector, and KPI01
-- Extends 2_Staff_Task_Allocation_byDay_base_1 with leave-dependent columns from SUPPORT_Staff_Leave_Allocation_byDay.
-- Is_Day_With_a_Leave: TRUE if any leave allocation exists for this staff+date
-- Is_Full_Day_Leave: TRUE if a full-day leave exists for this staff+date
-- Billable_Selector: IF(Is_Billable=TRUE, "Billable", "Not Billable")
-- Is_Final_Invoice_Raised: LOOKUPVALUE(TOCHECK_JobWithFinalInvoice[Type], JobText, Job_ID) = "Final Invoice"
-- Recorded_Task_Hours: CALCULATE(SUM(4_Timesheet_Table[Recorded_Minutes])/60, FILTER(Job_Task_Staff_ID match), FILTER(Date match))
-- Admin_Task_To_Be_Removed: IF(AND(Task_Name="Admin - Non-billable", Date>=DATE(2021,02,01)), TRUE, FALSE)
-- Initial_Allo_Hrs_perWorkDay_KPI01: IF(Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek AND NOT Is_Full_Day_Leave AND NOT Admin_Task_To_Be_Removed, Initial_Avg_Mins_perWorkDay/60, BLANK())
-- Dependencies: 2_Staff_Task_Allocation_byDay_base_1 (#2), SUPPORT_Staff_Leave_Allocation_byDay (#6)
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay_base_3" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay_base_2" CASCADE;


CREATE MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay_base_2" AS
WITH leave_agg AS (
    -- Pre-aggregate SUPPORT_Staff_Leave_Allocation_byDay per (Staff_Name, Date)
    -- so the main query can hash-join O(1) per row instead of correlated subquery per row
    SELECT
        sl."Staff_Name",
        sl."Date",
        COALESCE(BOOL_OR(sl."Allo_Leave_Hrs_perWorkday" IS NOT NULL), FALSE) AS "Is_Day_With_a_Leave",
        COALESCE(BOOL_OR(sl."Full_Leave_Days" IS NOT NULL), FALSE)           AS "Is_Full_Day_Leave"
    FROM SUPPORT_Staff_Leave_Allocation_byDay sl
    GROUP BY sl."Staff_Name", sl."Date"
),
ts_task AS (
    -- Pre-aggregate 4_Timesheet_Table by (Job_Task_Staff_ID, Date) for Recorded_Task_Hours
    SELECT
        "Job_Task_Staff_ID",
        "Date",
        SUM("Recorded_Minutes") / 60.0 AS "Recorded_Task_Hours"
    FROM "4_Timesheet_Table"
    GROUP BY "Job_Task_Staff_ID", "Date"
),
wdb AS (
    -- Pre-aggregate workable day count + Task_Allocated_Mins per Job_Task_Staff_ID
    -- Reuses leave_agg CTE for Is_Full_Day_Leave; avoids circular dependency on 1_Job_Task_Details_Table
    -- Joins to _base_1 (from 025) to get Task_Allocated_Mins (not available on 2_Staff_Task_Allocation_byDay_base_1)
    SELECT
        b1."Job_Task_Staff_ID",
        COUNT(*) AS wdb_cnt,
        jt."Task_Allocated_Mins"
    FROM "2_Staff_Task_Allocation_byDay_base_1" b1
    LEFT JOIN leave_agg lv1 ON lv1."Staff_Name" = b1."Staff_Name" AND lv1."Date" = b1."Date"
    JOIN "1_Job_Task_Details_Table_base_1" jt ON jt."Job_Task_Staff_ID" = b1."Job_Task_Staff_ID"
    WHERE b1."Is_Date_Between_Task_Days" = TRUE
      AND b1."Is_Workable_Day" = TRUE
      AND b1."Is_Staff_Workable_DayOfWeek" = TRUE
      AND COALESCE(lv1."Is_Full_Day_Leave", FALSE) = FALSE
    GROUP BY b1."Job_Task_Staff_ID", jt."Task_Allocated_Mins"
)
SELECT
    b.*,
    -- Is_Day_With_a_Leave: TRUE if any leave allocation exists for this staff+date
    COALESCE(lv."Is_Day_With_a_Leave", FALSE) AS "Is_Day_With_a_Leave",
    -- Is_Full_Day_Leave: TRUE if a full-day leave exists for this staff+date
    COALESCE(lv."Is_Full_Day_Leave", FALSE)   AS "Is_Full_Day_Leave",
    -- Billable_Selector: IF(Is_Billable=TRUE, "Billable", "Not Billable")
    CASE
        WHEN b."Is_Billable" = TRUE THEN 'Billable'
        ELSE 'Not Billable'
    END AS "Billable_Selector",
    -- Is_Final_Invoice_Raised: TRUE if Job_ID has a final invoice in TOCHECK_JobWithFinalInvoice
    CASE WHEN fi."JobText" IS NOT NULL THEN TRUE ELSE FALSE END AS "Is_Final_Invoice_Raised",
    -- Recorded_Task_Hours: SUM(Recorded_Minutes)/60 from 4_Timesheet_Table for this Job_Task_Staff_ID + Date
    COALESCE(ts."Recorded_Task_Hours", 0) AS "Recorded_Task_Hours",
    -- Admin_Task_To_Be_Removed: Task_Name = 'Admin - Non-billable' AND Date >= 2021-02-01
    (b."Task_Name" = 'Admin - Non-billable' AND b."Date" >= DATE '2021-02-01') AS "Admin_Task_To_Be_Removed",
    -- Initial_Allo_Hrs_perWorkDay_KPI01:
    -- IF(Is_Workable_Day AND Is_Date_Between_Task_Days,
    --   IF(Is_Staff_Workable_DayOfWeek AND NOT Is_Full_Day_Leave,
    --     IF(NOT Admin_Task_To_Be_Removed, Initial_Avg_Mins_perWorkDay / 60, BLANK())))
    -- Initial_Avg_Mins_perWorkDay = Task_Allocated_Mins / Workable_Days_Between_Task (computed inline via wdb CTE)
    CASE
        WHEN b."Is_Workable_Day" = TRUE
         AND b."Is_Date_Between_Task_Days" = TRUE
         AND b."Is_Staff_Workable_DayOfWeek" = TRUE
         AND COALESCE(lv."Is_Full_Day_Leave", FALSE) = FALSE
         AND NOT (b."Task_Name" = 'Admin - Non-billable' AND b."Date" >= DATE '2021-02-01')
        THEN w."Task_Allocated_Mins"::NUMERIC / NULLIF(w.wdb_cnt, 0) / 60.0
    END AS "Initial_Allo_Hrs_perWorkDay_KPI01"
FROM "2_Staff_Task_Allocation_byDay_base_1" b
LEFT JOIN leave_agg lv ON lv."Staff_Name" = b."Staff_Name" AND lv."Date" = b."Date"
LEFT JOIN TOCHECK_JobWithFinalInvoice fi ON fi."JobText" = b."Job_ID"
LEFT JOIN ts_task ts ON ts."Job_Task_Staff_ID" = b."Job_Task_Staff_ID" AND ts."Date" = b."Date"
LEFT JOIN wdb w ON w."Job_Task_Staff_ID" = b."Job_Task_Staff_ID";


CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_2" ("Date");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_2" ("Job_Task_Staff_ID");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_2" ("Staff_Name");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_2" ("Is_Billable");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_2" ("Is_Workable_Day");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_2" ("Is_Day_With_a_Leave");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_2" ("Is_Full_Day_Leave");


-- 1_Job_Task_Details_Table_base_2
-- DAX equivalent: 1_Job_Task_Details_Table with computed workable day counts, leave flag, and leave-adjusted metrics
-- Extends 1_Job_Task_Details_Table_base_1 with:
--   Workable_Days_Between_Task, Is_Task_a_Leave, Workable_Hrs_Between_Task, Initial_Avg_Mins_perWorkDay,
--   Total_Leave_Hrs_between_Workable_Days, Rev_Workable_Days_Between_Task, Avg_Mins_perWorkDay_WITHOUT_Leave
-- Uses CTE + LEFT JOIN to pre-aggregate counts (hash join O(1) per row, not correlated subquery)
-- Dependencies: 1_Job_Task_Details_Table_base_1 (from 025), 2_Staff_Task_Allocation_byDay_base_2 (#7)
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table_base_3" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table_base_2" CASCADE;


CREATE MATERIALIZED VIEW "1_Job_Task_Details_Table_base_2" AS
WITH wdb AS (
    -- Pre-aggregate workable day counts per Job_Task_Staff_ID
    -- One grouped scan of 2_Staff_Task_Allocation_byDay, then hash-joined to _base_1
    SELECT
        d."Job_Task_Staff_ID",
        COUNT(*) AS "Workable_Days_Between_Task"
    FROM "2_Staff_Task_Allocation_byDay_base_2" d
    WHERE d."Is_Date_Between_Task_Days" = TRUE
      AND d."Is_Workable_Day" = TRUE
      AND d."Is_Staff_Workable_DayOfWeek" = TRUE
      AND d."Is_Full_Day_Leave" = FALSE
    GROUP BY d."Job_Task_Staff_ID"
),
tlh AS (
    -- Pre-aggregate Total_Leave_Hrs_between_Workable_Days per Job_Task_Staff_ID
    -- SUM of Initial_Allo_Hrs_perWorkDay_KPI01 for leave tasks within each task's date range
    SELECT
        b1."Job_Task_Staff_ID",
        SUM(d."Initial_Allo_Hrs_perWorkDay_KPI01") AS "Total_Leave_Hrs"
    FROM "1_Job_Task_Details_Table_base_1" b1
    JOIN "2_Staff_Task_Allocation_byDay_base_2" d
      ON d."Staff_Name" = b1."Staff_Name"
     AND d."Date" >= b1."StartDateAdjusted"
     AND d."Date" <= b1."DueDateAdjusted"
     AND d."Task_Category" = 'Leave Tasks'
     AND d."Is_Full_Day_Leave" = FALSE
    GROUP BY b1."Job_Task_Staff_ID"
)
SELECT
    b.*,
    -- Is_Task_a_Leave: IF(OR(CONTAINSSTRING(Task_Name,"Holiday"),CONTAINSSTRING(Task_Name,"Sick leave")),TRUE,
    --   IF(CONTAINSSTRING(Task_Name,"Other leave"),TRUE,FALSE))
    CASE
        WHEN b."Task_Name" ILIKE '%Holiday%' OR b."Task_Name" ILIKE '%Sick Leave%' THEN TRUE
        WHEN b."Task_Name" ILIKE '%Other leave%' THEN TRUE
        ELSE FALSE
    END AS "Is_Task_a_Leave",
    -- Workable_Days_Between_Task: COUNTROWS from 2_Staff_Task_Allocation_byDay with filters
    COALESCE(w."Workable_Days_Between_Task", 0) AS "Workable_Days_Between_Task",
    -- Workable_Hrs_Between_Task: Workable_Days_Between_Task * 8
    COALESCE(w."Workable_Days_Between_Task", 0) * 8 AS "Workable_Hrs_Between_Task",
    -- Initial_Avg_Mins_perWorkDay: DIVIDE(Task_Allocated_Mins, Workable_Days_Between_Task, BLANK())
    -- NULLIF prevents divide-by-zero, returning NULL (DAX BLANK())
    b."Task_Allocated_Mins"::NUMERIC / NULLIF(COALESCE(w."Workable_Days_Between_Task", 0), 0) AS "Initial_Avg_Mins_perWorkDay",
    -- Total_Leave_Hrs_between_Workable_Days: IF(Is_Task_a_Leave=FALSE, SUM(KPI01) for leave tasks in date range, BLANK())
    CASE
        WHEN NOT (b."Task_Name" ILIKE '%Holiday%' OR b."Task_Name" ILIKE '%Sick Leave%' OR b."Task_Name" ILIKE '%Other leave%')
        THEN COALESCE(t."Total_Leave_Hrs", 0)
    END AS "Total_Leave_Hrs_between_Workable_Days",
    -- Rev_Workable_Days_Between_Task: IF(Is_Task_a_Leave=FALSE, (Workable_Hrs - Total_Leave_Hrs) / 8, BLANK())
    CASE
        WHEN NOT (b."Task_Name" ILIKE '%Holiday%' OR b."Task_Name" ILIKE '%Sick Leave%' OR b."Task_Name" ILIKE '%Other leave%')
        THEN (COALESCE(w."Workable_Days_Between_Task", 0) * 8 - COALESCE(t."Total_Leave_Hrs", 0)) / 8.0
    END AS "Rev_Workable_Days_Between_Task",
    -- Avg_Mins_perWorkDay_WITHOUT_Leave: IF(Is_Task_a_Leave=FALSE, DIVIDE(Task_Allocated_Mins, Rev_Workable_Days, BLANK()), BLANK())
    CASE
        WHEN NOT (b."Task_Name" ILIKE '%Holiday%' OR b."Task_Name" ILIKE '%Sick Leave%' OR b."Task_Name" ILIKE '%Other leave%')
        THEN b."Task_Allocated_Mins"::NUMERIC / NULLIF(
            (COALESCE(w."Workable_Days_Between_Task", 0) * 8 - COALESCE(t."Total_Leave_Hrs", 0)) / 8.0,
            0
        )
    END AS "Avg_Mins_perWorkDay_WITHOUT_Leave"
FROM "1_Job_Task_Details_Table_base_1" b
LEFT JOIN wdb w ON w."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
LEFT JOIN tlh t ON t."Job_Task_Staff_ID" = b."Job_Task_Staff_ID";


CREATE INDEX ON "1_Job_Task_Details_Table_base_2" ("Job_Task_Staff_ID");
CREATE INDEX ON "1_Job_Task_Details_Table_base_2" ("Job_ID");
CREATE INDEX ON "1_Job_Task_Details_Table_base_2" ("Staff_Name");
CREATE INDEX ON "1_Job_Task_Details_Table_base_2" ("Workable_Days_Between_Task");


-- 2_Staff_Task_Allocation_byDay_base_3
-- DAX equivalent: Intermediate daily task allocation view with KPI02
-- Extends 2_Staff_Task_Allocation_byDay_base_2 with:
--   Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02: IF(Is_Workable_Day AND Is_Date_Between_Task_Days
--     AND Is_Staff_Workable_DayOfWeek AND NOT Is_Day_With_a_Leave AND NOT Is_Task_a_Leave
--     AND NOT Is_Full_Day_Leave AND NOT Admin_Task_To_Be_Removed,
--     LOOKUPVALUE(1_Job_Task_Details_Table[Avg_Mins_perWorkDay_WITHOUT_Leave], Job_Task_Staff_ID) / 60, BLANK())
-- Dependencies: 2_Staff_Task_Allocation_byDay_base_2 (#7), 1_Job_Task_Details_Table_base_2 (#8)
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay_base_3" CASCADE;


CREATE MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay_base_3" AS
WITH jt_lookup AS (
    -- Deduplicate 1_Job_Task_Details_Table by Job_Task_Staff_ID for LOOKUPVALUE semantics
    SELECT DISTINCT ON ("Job_Task_Staff_ID")
        "Job_Task_Staff_ID",
        "Avg_Mins_perWorkDay_WITHOUT_Leave"
    FROM "1_Job_Task_Details_Table_base_2"
    ORDER BY "Job_Task_Staff_ID"
)
SELECT
    b.*,
    -- Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02:
    -- IF(Is_Workable_Day AND Is_Date_Between_Task_Days,
    --   IF(Is_Staff_Workable_DayOfWeek AND NOT Is_Day_With_a_Leave,
    --     IF(NOT Is_Task_a_Leave AND NOT Is_Full_Day_Leave,
    --       IF(NOT Admin_Task_To_Be_Removed, Avg_Mins_perWorkDay_WITHOUT_Leave / 60, BLANK()))))
    CASE
        WHEN b."Is_Workable_Day" = TRUE
         AND b."Is_Date_Between_Task_Days" = TRUE
         AND b."Is_Staff_Workable_DayOfWeek" = TRUE
         AND b."Is_Day_With_a_Leave" = FALSE
         AND b."Is_Task_a_Leave" = FALSE
         AND b."Is_Full_Day_Leave" = FALSE
         AND b."Admin_Task_To_Be_Removed" = FALSE
        THEN jt."Avg_Mins_perWorkDay_WITHOUT_Leave" / 60.0
    END AS "Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02",
    -- Is_Date_between_Start&Today: IF(AND(Date < TODAY(), Is_Date_Between_Task_Days = TRUE), TRUE, FALSE)
    CASE
        WHEN b."Date" < CURRENT_DATE AND b."Is_Date_Between_Task_Days" = TRUE THEN TRUE
        ELSE FALSE
    END AS "Is_Date_between_Start&Today"
FROM "2_Staff_Task_Allocation_byDay_base_2" b
LEFT JOIN jt_lookup jt ON jt."Job_Task_Staff_ID" = b."Job_Task_Staff_ID";


CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_3" ("Date");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_3" ("Job_Task_Staff_ID");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay_base_3" ("Staff_Name");


-- 1_Job_Task_Details_Table_base_3
-- DAX equivalent: Intermediate 1_Job_Task_Details_Table with leave-adjusted allocation metrics
-- Extends 1_Job_Task_Details_Table_base_2 with:
--   Total_Task_Mins_WorkDays_WITHOUT_Leave: SUM(Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02)*60 per Job_Task_Staff_ID (non-leave only)
--   Remaining_Allocated_Task_Mins: Task_Allocated_Mins - Total_Task_Mins_WorkDays_WITHOUT_Leave (non-leave only)
--   WorkDays_WITH_Leaves_between_Task: COUNTROWS from 2_Staff_Task_Allocation_byDay with Is_Day_With_a_Leave=TRUE (non-leave only)
--   Avg_Mins_perWorkDay_WITH_Leaves: DIVIDE(Remaining_Allocated_Task_Mins, WorkDays_WITH_Leaves, BLANK()) (non-leave only)
--   Task_Mins_Worked_Till_Date: SUM(4_Timesheet_Table[Recorded_Minutes]) per Job_Task_Staff_ID (non-leave only)
--   IS_Task_Mins_Worked_>_Allocated: Task_Mins_Worked_Till_Date > Task_Allocated_Mins (non-leave only)
--   Task_Mins_Remain_until_Due: IF(IS_Task_Mins_Worked_>_Allocated, 0, Task_Allocated_Mins - Task_Mins_Worked_Till_Date) (non-leave only)
-- Dependencies: 1_Job_Task_Details_Table_base_2 (#8), 2_Staff_Task_Allocation_byDay_base_3 (#9)
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table_base_3" CASCADE;


CREATE MATERIALIZED VIEW "1_Job_Task_Details_Table_base_3" AS
WITH ttm AS (
    -- Pre-aggregate SUM(KPI02)*60 per Job_Task_Staff_ID for Total_Task_Mins_WorkDays_WITHOUT_Leave
    SELECT
        d."Job_Task_Staff_ID",
        SUM(d."Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02") * 60.0 AS "Total_Task_Mins"
    FROM "2_Staff_Task_Allocation_byDay_base_3" d
    GROUP BY d."Job_Task_Staff_ID"
),
wdl AS (
    -- Pre-aggregate workable days WITH leaves per Job_Task_Staff_ID
    -- COUNTROWS where Is_Date_Between_Task_Days, Is_Workable_Day, Is_Day_With_a_Leave, Is_Staff_Workable_DayOfWeek, NOT Is_Full_Day_Leave
    SELECT
        d."Job_Task_Staff_ID",
        COUNT(*) AS "WorkDays_WITH_Leaves"
    FROM "2_Staff_Task_Allocation_byDay_base_3" d
    WHERE d."Is_Date_Between_Task_Days" = TRUE
      AND d."Is_Workable_Day" = TRUE
      AND d."Is_Day_With_a_Leave" = TRUE
      AND d."Is_Staff_Workable_DayOfWeek" = TRUE
      AND d."Is_Full_Day_Leave" = FALSE
    GROUP BY d."Job_Task_Staff_ID"
),
tmt AS (
    -- Pre-aggregate SUM(Recorded_Minutes) per Job_Task_Staff_ID from 4_Timesheet_Table
    SELECT
        "Job_Task_Staff_ID",
        SUM("Recorded_Minutes") AS recorded_mins
    FROM "4_Timesheet_Table"
    GROUP BY "Job_Task_Staff_ID"
),
pwdl AS (
    -- Pre-aggregate Prior_WorkDays_WITH_Leave per Job_Task_Staff_ID
    -- COUNTROWS where Is_Day_With_a_Leave, Is_Date_Between_Task_Days, Is_Workable_Day,
    -- Is_Date_between_Start&Today, Is_Staff_Workable_DayOfWeek, NOT Is_Full_Day_Leave
    SELECT
        d."Job_Task_Staff_ID",
        COUNT(*) AS prior_wdl_cnt
    FROM "2_Staff_Task_Allocation_byDay_base_3" d
    WHERE d."Is_Day_With_a_Leave" = TRUE
      AND d."Is_Date_Between_Task_Days" = TRUE
      AND d."Is_Workable_Day" = TRUE
      AND d."Is_Date_between_Start&Today" = TRUE
      AND d."Is_Staff_Workable_DayOfWeek" = TRUE
      AND d."Is_Full_Day_Leave" = FALSE
    GROUP BY d."Job_Task_Staff_ID"
)
SELECT
    b.*,
    -- Total_Task_Mins_WorkDays_WITHOUT_Leave: IF(Is_Task_a_Leave=FALSE, SUM(KPI02)*60, BLANK())
    CASE
        WHEN b."Is_Task_a_Leave" = FALSE
        THEN COALESCE(t."Total_Task_Mins", 0)
    END AS "Total_Task_Mins_WorkDays_WITHOUT_Leave",
    -- Remaining_Allocated_Task_Mins: IF(Is_Task_a_Leave=FALSE, Task_Allocated_Mins - Total_Task_Mins, BLANK())
    CASE
        WHEN b."Is_Task_a_Leave" = FALSE
        THEN b."Task_Allocated_Mins" - COALESCE(t."Total_Task_Mins", 0)
    END AS "Remaining_Allocated_Task_Mins",
    -- WorkDays_WITH_Leaves_between_Task: IF(Is_Task_a_Leave=FALSE, COUNTROWS(...), BLANK())
    CASE
        WHEN b."Is_Task_a_Leave" = FALSE
        THEN COALESCE(w."WorkDays_WITH_Leaves", 0)
    END AS "WorkDays_WITH_Leaves_between_Task",
    -- Avg_Mins_perWorkDay_WITH_Leaves: IF(Is_Task_a_Leave=FALSE, DIVIDE(Remaining, WorkDays_WITH_Leaves, BLANK()), BLANK())
    CASE
        WHEN b."Is_Task_a_Leave" = FALSE
        THEN (b."Task_Allocated_Mins" - COALESCE(t."Total_Task_Mins", 0))
             / NULLIF(COALESCE(w."WorkDays_WITH_Leaves", 0), 0)::NUMERIC
    END AS "Avg_Mins_perWorkDay_WITH_Leaves",
    -- Task_Mins_Worked_Till_Date: IF(Is_Task_a_Leave=FALSE, SUM(Recorded_Minutes), BLANK())
    CASE
        WHEN b."Is_Task_a_Leave" = FALSE
        THEN COALESCE(tm.recorded_mins, 0)
    END AS "Task_Mins_Worked_Till_Date",
    -- IS_Task_Mins_Worked_>_Allocated: IF(Is_Task_a_Leave=FALSE, IF(Task_Mins_Worked > Task_Allocated_Mins, TRUE, FALSE), BLANK())
    CASE
        WHEN b."Is_Task_a_Leave" = FALSE THEN
            CASE
                WHEN COALESCE(tm.recorded_mins, 0) > b."Task_Allocated_Mins" THEN TRUE
                ELSE FALSE
            END
    END AS "IS_Task_Mins_Worked_>_Allocated",
    -- Task_Mins_Remain_until_Due: IF(Is_Task_a_Leave=FALSE, IF(worked > allocated, 0, allocated - worked), BLANK())
    CASE
        WHEN b."Is_Task_a_Leave" = FALSE
        THEN GREATEST(0, b."Task_Allocated_Mins" - COALESCE(tm.recorded_mins, 0))
    END AS "Task_Mins_Remain_until_Due",
    -- Is_Task_DueDate_Over: IF(Is_Task_a_Leave=FALSE, IF(DueDateAdjusted < TODAY(), TRUE, FALSE), BLANK())
    CASE
        WHEN b."Is_Task_a_Leave" = FALSE THEN
            CASE
                WHEN b."DueDateAdjusted" < CURRENT_DATE THEN TRUE
                ELSE FALSE
            END
    END AS "Is_Task_DueDate_Over",
    -- Task_Mins_Worked_Adjusted: IF(Is_Task_a_Leave=FALSE,
    --   IF(OR(IS_Task_Mins_Worked_>_Allocated, Is_Task_DueDate_Over), Task_Allocated_Mins, Task_Mins_Worked_Till_Date), BLANK())
    CASE
        WHEN b."Is_Task_a_Leave" = FALSE THEN
            CASE
                WHEN COALESCE(tm.recorded_mins, 0) > b."Task_Allocated_Mins"
                  OR b."DueDateAdjusted" < CURRENT_DATE
                THEN b."Task_Allocated_Mins"
                ELSE COALESCE(tm.recorded_mins, 0)
            END
    END AS "Task_Mins_Worked_Adjusted",
    -- Prior_WorkDays_WITH_Leave: IF(Is_Task_a_Leave=FALSE, COUNTROWS(...), BLANK())
    CASE
        WHEN b."Is_Task_a_Leave" = FALSE
        THEN COALESCE(p.prior_wdl_cnt, 0)
    END AS "Prior_WorkDays_WITH_Leave"
FROM "1_Job_Task_Details_Table_base_2" b
LEFT JOIN ttm t ON t."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
LEFT JOIN wdl w ON w."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
LEFT JOIN tmt tm ON tm."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
LEFT JOIN pwdl p ON p."Job_Task_Staff_ID" = b."Job_Task_Staff_ID";


CREATE INDEX ON "1_Job_Task_Details_Table_base_3" ("Job_Task_Staff_ID");
CREATE INDEX ON "1_Job_Task_Details_Table_base_3" ("Job_ID");
CREATE INDEX ON "1_Job_Task_Details_Table_base_3" ("Staff_Name");


-- 2_Staff_Task_Allocation_byDay
-- DAX equivalent: Final daily task allocation view with KPI03 and FIN01
-- Extends 2_Staff_Task_Allocation_byDay_base_3 with:
--   Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE: IF(Is_Date_between_Start&Today AND Task_Category="Billable Tasks",
--     IF(Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek AND Is_Day_With_a_Leave
--       AND NOT Is_Task_a_Leave AND NOT Is_Full_Day_Leave,
--       LOOKUPVALUE(1_Job_Task_Details_Table[Avg_Mins_perWorkDay_WITH_Leaves], Job_Task_Staff_ID) / 60, BLANK()))
--   Allo_Hrs_perWorkday_WITH_Leave_KPI03: Same conditions as KPI02 but Is_Day_With_a_Leave=TRUE
--     AND NOT Admin_Task_To_Be_Removed, Avg_Mins_perWorkDay_WITH_Leaves / 60
--   Allo_Hrs_perWorkDay_AdjLeaves_FIN01: KPI02 + KPI03
--   Is_Date_between_Today&Due: IF(AND(Date >= TODAY(), Is_Date_Between_Task_Days), TRUE, FALSE)
--   Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04: Same structure as KPI03 but Is_Date_between_Today&Due instead of Is_Date_between_Start&Today
-- Dependencies: 2_Staff_Task_Allocation_byDay_base_3 (#9), 1_Job_Task_Details_Table_base_3 (#10)
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay" CASCADE;


CREATE MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay" AS
WITH jt_lookup AS (
    -- Deduplicate 1_Job_Task_Details_Table by Job_Task_Staff_ID for LOOKUPVALUE semantics
    SELECT DISTINCT ON ("Job_Task_Staff_ID")
        "Job_Task_Staff_ID",
        "Avg_Mins_perWorkDay_WITH_Leaves"
    FROM "1_Job_Task_Details_Table_base_3"
    ORDER BY "Job_Task_Staff_ID"
),
base AS (
    SELECT
        b.*,
        -- Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE:
        -- VAR Logic = IF(AND(Is_Workable_Day, Is_Date_Between_Task_Days),
        --   IF(AND(Is_Staff_Workable_DayOfWeek, Is_Day_With_a_Leave),
        --     IF(AND(NOT Is_Task_a_Leave, NOT Is_Full_Day_Leave),
        --       Avg_Mins_perWorkDay_WITH_Leaves / 60, BLANK())))
        -- RETURN IF(AND(Is_Date_between_Start&Today, Task_Category="Billable Tasks"), Logic, BLANK())
        CASE
            WHEN b."Is_Date_between_Start&Today" = TRUE
             AND b."Task_Category" = 'Billable Tasks'
             AND b."Is_Workable_Day" = TRUE
             AND b."Is_Date_Between_Task_Days" = TRUE
             AND b."Is_Staff_Workable_DayOfWeek" = TRUE
             AND b."Is_Day_With_a_Leave" = TRUE
             AND b."Is_Task_a_Leave" = FALSE
             AND b."Is_Full_Day_Leave" = FALSE
            THEN jt."Avg_Mins_perWorkDay_WITH_Leaves" / 60.0
        END AS "Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE",
        -- Allo_Hrs_perWorkday_WITH_Leave_KPI03:
        -- IF(AND(Is_Workable_Day, Is_Date_Between_Task_Days),
        --   IF(AND(Is_Staff_Workable_DayOfWeek, Is_Day_With_a_Leave),
        --     IF(AND(NOT Is_Task_a_Leave, NOT Is_Full_Day_Leave),
        --       IF(NOT Admin_Task_To_Be_Removed, Avg_Mins_perWorkDay_WITH_Leaves / 60, BLANK()))))
        CASE
            WHEN b."Is_Workable_Day" = TRUE
             AND b."Is_Date_Between_Task_Days" = TRUE
             AND b."Is_Staff_Workable_DayOfWeek" = TRUE
             AND b."Is_Day_With_a_Leave" = TRUE
             AND b."Is_Task_a_Leave" = FALSE
             AND b."Is_Full_Day_Leave" = FALSE
             AND b."Admin_Task_To_Be_Removed" = FALSE
            THEN jt."Avg_Mins_perWorkDay_WITH_Leaves" / 60.0
        END AS "Allo_Hrs_perWorkday_WITH_Leave_KPI03",
        -- Is_Date_between_Today&Due: IF(AND(Date >= TODAY(), Is_Date_Between_Task_Days = TRUE), TRUE, FALSE)
        CASE
            WHEN b."Date" >= CURRENT_DATE AND b."Is_Date_Between_Task_Days" = TRUE THEN TRUE
            ELSE FALSE
        END AS "Is_Date_between_Today&Due",
        -- Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04:
        -- VAR Logic = IF(AND(Is_Workable_Day, Is_Date_Between_Task_Days),
        --   IF(AND(Is_Staff_Workable_DayOfWeek, Is_Day_With_a_Leave),
        --     IF(AND(NOT Is_Task_a_Leave, NOT Is_Full_Day_Leave),
        --       Avg_Mins_perWorkDay_WITH_Leaves / 60, BLANK())))
        -- RETURN IF(AND(Is_Date_between_Today&Due, Task_Category="Billable Tasks"), Logic, BLANK())
        CASE
            WHEN b."Date" >= CURRENT_DATE
             AND b."Is_Date_Between_Task_Days" = TRUE
             AND b."Task_Category" = 'Billable Tasks'
             AND b."Is_Workable_Day" = TRUE
             AND b."Is_Staff_Workable_DayOfWeek" = TRUE
             AND b."Is_Day_With_a_Leave" = TRUE
             AND b."Is_Task_a_Leave" = FALSE
             AND b."Is_Full_Day_Leave" = FALSE
            THEN jt."Avg_Mins_perWorkDay_WITH_Leaves" / 60.0
        END AS "Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04"
    FROM "2_Staff_Task_Allocation_byDay_base_3" b
    LEFT JOIN jt_lookup jt ON jt."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
)
SELECT
    base.*,
    -- Allo_Hrs_perWorkDay_AdjLeaves_FIN01: KPI02 + KPI03
    COALESCE(base."Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02", 0)
    + COALESCE(base."Allo_Hrs_perWorkday_WITH_Leave_KPI03", 0) AS "Allo_Hrs_perWorkDay_AdjLeaves_FIN01"
FROM base;


CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Date");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Job_Task_Staff_ID");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Staff_Name");


-- 1_Job_Task_Details_Table
-- DAX equivalent: Final 1_Job_Task_Details_Table with remaining workday allocation metrics
-- Extends 1_Job_Task_Details_Table_base_3 with:
--   Allo_Mins_during_Remaining_workDays_WITH_leave: SUM(KPI04)*60 per Job_Task_Staff_ID (non-leave only)
--   Remain_WorkDays_WITHOUT_Leave: COUNTROWS from 2_Staff_Task_Allocation_byDay with DATESBETWEEN(TODAY(), DueDateAdjusted) (non-leave only)
--   Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave: Task_Mins_Remain_until_Due - Allo_Mins_during_Remaining (non-leave only)
--   Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave: DIVIDE(Remain_Mins, Remain_WorkDays, BLANK()) (non-leave only)
--   Is_Task_WITHIN_Allo_Time_IMP: IF(Remain_Mins > 0, IF(Remain_WorkDays >= 1 AND Avg <= 480, TRUE, FALSE), TRUE) (non-leave only)
--   Prior_WorkDays_WITHOUT_Leave: COUNTROWS with Is_Day_With_a_Leave=FALSE, Is_Date_between_Start&Today (non-leave only)
--   Allo_Mins_during_PriorWorkDays_WITH_leave: SUM(Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE)*60 with leave filters (non-leave only)
-- Dependencies: 1_Job_Task_Details_Table_base_3 (#10), 2_Staff_Task_Allocation_byDay (#11)
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table" CASCADE;


CREATE MATERIALIZED VIEW "1_Job_Task_Details_Table" AS
WITH arm AS (
    -- Pre-aggregate SUM(KPI04)*60 per Job_Task_Staff_ID for Allo_Mins_during_Remaining_workDays_WITH_leave
    SELECT
        d."Job_Task_Staff_ID",
        SUM(d."Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04") * 60.0 AS allo_mins_remaining
    FROM "2_Staff_Task_Allocation_byDay" d
    GROUP BY d."Job_Task_Staff_ID"
),
rwdwol AS (
    -- Pre-aggregate Remain_WorkDays_WITHOUT_Leave per Job_Task_Staff_ID
    -- COUNTROWS where Is_Day_With_a_Leave=FALSE, Is_Date_Between_Task_Days, Is_Workable_Day,
    -- Is_Staff_Workable_DayOfWeek, NOT Is_Full_Day_Leave, DATESBETWEEN(Date, TODAY(), DueDateAdjusted)
    SELECT
        d."Job_Task_Staff_ID",
        COUNT(*) AS remain_wdwol_cnt
    FROM "2_Staff_Task_Allocation_byDay" d
    WHERE d."Is_Day_With_a_Leave" = FALSE
      AND d."Is_Date_Between_Task_Days" = TRUE
      AND d."Is_Workable_Day" = TRUE
      AND d."Is_Staff_Workable_DayOfWeek" = TRUE
      AND d."Is_Full_Day_Leave" = FALSE
      AND d."Date" >= CURRENT_DATE
      AND d."Date" <= d."DueDateAdjusted"
    GROUP BY d."Job_Task_Staff_ID"
),
pwdwol AS (
    -- Pre-aggregate Prior_WorkDays_WITHOUT_Leave per Job_Task_Staff_ID
    -- COUNTROWS where Is_Day_With_a_Leave=FALSE, Is_Date_Between_Task_Days, Is_Workable_Day,
    -- Is_Date_between_Start&Today, Is_Staff_Workable_DayOfWeek, NOT Is_Full_Day_Leave
    SELECT
        d."Job_Task_Staff_ID",
        COUNT(*) AS prior_wdwol_cnt
    FROM "2_Staff_Task_Allocation_byDay" d
    WHERE d."Is_Day_With_a_Leave" = FALSE
      AND d."Is_Date_Between_Task_Days" = TRUE
      AND d."Is_Workable_Day" = TRUE
      AND d."Is_Date_between_Start&Today" = TRUE
      AND d."Is_Staff_Workable_DayOfWeek" = TRUE
      AND d."Is_Full_Day_Leave" = FALSE
    GROUP BY d."Job_Task_Staff_ID"
),
ampwl AS (
    -- Pre-aggregate SUM(Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE)*60 per Job_Task_Staff_ID
    -- Filters: Is_Day_With_a_Leave=TRUE, Is_Date_Between_Task_Days, Is_Workable_Day,
    -- Is_Date_between_Start&Today, Is_Staff_Workable_DayOfWeek, NOT Is_Full_Day_Leave
    SELECT
        d."Job_Task_Staff_ID",
        SUM(d."Initial_Allo_Hrs_perPriorWorkDays_WITH_LEAVE") * 60.0 AS allo_mins_prior
    FROM "2_Staff_Task_Allocation_byDay" d
    WHERE d."Is_Day_With_a_Leave" = TRUE
      AND d."Is_Date_Between_Task_Days" = TRUE
      AND d."Is_Workable_Day" = TRUE
      AND d."Is_Date_between_Start&Today" = TRUE
      AND d."Is_Staff_Workable_DayOfWeek" = TRUE
      AND d."Is_Full_Day_Leave" = FALSE
    GROUP BY d."Job_Task_Staff_ID"
),
base AS (
    SELECT
        b.*,
        -- Allo_Mins_during_Remaining_workDays_WITH_leave
        CASE WHEN b."Is_Task_a_Leave" = FALSE THEN COALESCE(a.allo_mins_remaining, 0) END
            AS "Allo_Mins_during_Remaining_workDays_WITH_leave",
        -- Remain_WorkDays_WITHOUT_Leave
        CASE WHEN b."Is_Task_a_Leave" = FALSE THEN COALESCE(r.remain_wdwol_cnt, 0) END
            AS "Remain_WorkDays_WITHOUT_Leave",
        -- Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave
        CASE WHEN b."Is_Task_a_Leave" = FALSE THEN b."Task_Mins_Remain_until_Due" - COALESCE(a.allo_mins_remaining, 0) END
            AS "Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave",
        -- Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave
        CASE WHEN b."Is_Task_a_Leave" = FALSE
            THEN (b."Task_Mins_Remain_until_Due" - COALESCE(a.allo_mins_remaining, 0))
                 / NULLIF(COALESCE(r.remain_wdwol_cnt, 0), 0)::NUMERIC
        END AS "Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave",
        -- Prior_WorkDays_WITHOUT_Leave
        CASE WHEN b."Is_Task_a_Leave" = FALSE THEN COALESCE(p.prior_wdwol_cnt, 0) END
            AS "Prior_WorkDays_WITHOUT_Leave",
        -- Allo_Mins_during_PriorWorkDays_WITH_leave
        CASE WHEN b."Is_Task_a_Leave" = FALSE THEN COALESCE(ap.allo_mins_prior, 0) END
            AS "Allo_Mins_during_PriorWorkDays_WITH_leave"
    FROM "1_Job_Task_Details_Table_base_3" b
    LEFT JOIN arm a ON a."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    LEFT JOIN rwdwol r ON r."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    LEFT JOIN pwdwol p ON p."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    LEFT JOIN ampwl ap ON ap."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
)
SELECT
    base.*,
    -- Is_Task_WITHIN_Allo_Time_IMP:
    -- IF(Remain_Mins > 0, IF(AND(Remain_WorkDays >= 1, Avg_Remain <= 480), TRUE, FALSE), TRUE)
    -- RETURN IF(Is_Task_a_Leave=FALSE, Result, BLANK())
    CASE
        WHEN base."Is_Task_a_Leave" = FALSE THEN
            CASE
                WHEN base."Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave" > 0 THEN
                    CASE
                        WHEN base."Remain_WorkDays_WITHOUT_Leave" >= 1
                         AND base."Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave" <= 480 THEN TRUE
                        ELSE FALSE
                    END
                ELSE TRUE
            END
    END AS "Is_Task_WITHIN_Allo_Time_IMP"
FROM base;


CREATE INDEX ON "1_Job_Task_Details_Table" ("Job_Task_Staff_ID");
CREATE INDEX ON "1_Job_Task_Details_Table" ("Job_ID");
CREATE INDEX ON "1_Job_Task_Details_Table" ("Staff_Name");
