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
--   7. 2_Staff_Task_Allocation_byDay       (depends on #2, #6)
--   8. 1_Job_Task_Details_Table            (depends on 1_Job_Task_Details_Table_base_1 from 025, #7)
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


-- 2_Staff_Task_Allocation_byDay
-- DAX equivalent: Final daily task allocation view with leave flags and billable selector
-- Extends 2_Staff_Task_Allocation_byDay_base_1 with leave-dependent columns from SUPPORT_Staff_Leave_Allocation_byDay.
-- Is_Day_With_a_Leave: CALCULATE(FIRSTNONBLANK(SUPPORT_Staff_Leave_Allocation_byDay[Allo_Leave_Hrs_perWorkday],1), FILTER(Staff_Name match), FILTER(Date match)) — TRUE if any leave allocation exists for this staff+date
-- Is_Full_Day_Leave: CALCULATE(FIRSTNONBLANK(SUPPORT_Staff_Leave_Allocation_byDay[Full_Leave_Days],1), FILTER(Staff_Name match), FILTER(Date match)) — TRUE if a full-day leave exists for this staff+date
-- Billable_Selector: IF(Is_Billable=TRUE, "Billable", "Not Billable")
-- Is_Final_Invoice_Raised: LOOKUPVALUE(TOCHECK_JobWithFinalInvoice[Type], JobText, Job_ID) = "Final Invoice"
-- Recorded_Task_Hours: CALCULATE(SUM(4_Timesheet_Table[Recorded_Minutes])/60, FILTER(Job_Task_Staff_ID match), FILTER(Date match))
-- Admin_Task_To_Be_Removed: IF(AND(Task_Name="Admin - Non-billable", Date>=DATE(2021,02,01)), TRUE, FALSE)
-- Initial_Allo_Hrs_perWorkDay_KPI01: IF(Is_Workable_Day AND Is_Date_Between_Task_Days AND Is_Staff_Workable_DayOfWeek AND NOT Is_Full_Day_Leave AND NOT Admin_Task_To_Be_Removed, Initial_Avg_Mins_perWorkDay/60, BLANK())
-- Dependencies: 2_Staff_Task_Allocation_byDay_base_1 (#2), SUPPORT_Staff_Leave_Allocation_byDay (#6)
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay" CASCADE;


CREATE MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay" AS
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


CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Date");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Job_Task_Staff_ID");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Staff_Name");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Is_Billable");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Is_Workable_Day");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Is_Day_With_a_Leave");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Is_Full_Day_Leave");


-- 1_Job_Task_Details_Table
-- DAX equivalent: 1_Job_Task_Details_Table with computed workable day counts and leave flag
-- Extends 1_Job_Task_Details_Table_base_1 with:
--   Workable_Days_Between_Task: COUNTROWS from 2_Staff_Task_Allocation_byDay where
--     Job_Task_Staff_ID matches, Is_Date_Between_Task_Days=TRUE, Is_Workable_Day=TRUE,
--     Is_Staff_Workable_DayOfWeek=TRUE, Is_Full_Day_Leave=FALSE
--   Is_Task_a_Leave: TRUE if Task_Name contains Holiday, Sick leave, or Other leave
--   Workable_Hrs_Between_Task: Workable_Days_Between_Task * 8
--   Initial_Avg_Mins_perWorkDay: DIVIDE(Task_Allocated_Mins, Workable_Days_Between_Task, BLANK())
--   Total_Leave_Hrs_between_Workable_Days: SUM(Initial_Allo_Hrs_perWorkDay_KPI01) for leave tasks within task date range (non-leave tasks only)
--   Rev_Workable_Days_Between_Task: (Workable_Hrs_Between_Task - Total_Leave_Hrs) / 8 (non-leave tasks only)
--   Avg_Mins_perWorkDay_WITHOUT_Leave: DIVIDE(Task_Allocated_Mins, Rev_Workable_Days_Between_Task, BLANK()) (non-leave tasks only)
-- Uses CTE + LEFT JOIN to pre-aggregate counts (hash join O(1) per row, not correlated subquery)
-- Dependencies: 1_Job_Task_Details_Table_base_1 (from 025), 2_Staff_Task_Allocation_byDay (#7)
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table" CASCADE;


CREATE MATERIALIZED VIEW "1_Job_Task_Details_Table" AS
WITH wdb AS (
    -- Pre-aggregate workable day counts per Job_Task_Staff_ID
    -- One grouped scan of 2_Staff_Task_Allocation_byDay, then hash-joined to _base_1
    SELECT
        d."Job_Task_Staff_ID",
        COUNT(*) AS "Workable_Days_Between_Task"
    FROM "2_Staff_Task_Allocation_byDay" d
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
    JOIN "2_Staff_Task_Allocation_byDay" d
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


CREATE INDEX ON "1_Job_Task_Details_Table" ("Job_Task_Staff_ID");
CREATE INDEX ON "1_Job_Task_Details_Table" ("Job_ID");
CREATE INDEX ON "1_Job_Task_Details_Table" ("Staff_Name");
CREATE INDEX ON "1_Job_Task_Details_Table" ("Workable_Days_Between_Task");
