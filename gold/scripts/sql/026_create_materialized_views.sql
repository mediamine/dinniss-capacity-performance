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
--   2. 2_Staff_Task_Allocation_byDay       (depends on #1, 1_Job_Task_Details_Table from 025_create_materialized_views.sql, EXCEL01_Staff_Workable_Days from 021_create_materialized_views.sql; uses CTE + LEFT JOIN to consolidate intermediate calculations into a single MV write)
--   3. 3_Staff_Performance_Table_base      (depends on key01_calendar_date, key03_staff_table from 010_create_views.sql)
--   4. SUPPORT_Job_Leave_Task_Details_Table_base_2 (depends on SUPPORT_Job_Leave_Task_Details_Table_base_1 from 025, #2)
--   5. SUPPORT_Job_Leave_Task_Details_Table (depends on #4)
--   6. SUPPORT_Staff_Leave_Allocation_byDay (depends on SUPPORT_Staff_Leave_Allocation_byDay_base_2 from 025, #5)
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


-- 2_Staff_Task_Allocation_byDay
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
-- Dependencies: 2_Staff_Task_Allocation_byDay_base (#1), 1_Job_Task_Details_Table (from 025_create_materialized_views.sql),
--               EXCEL01_Staff_Workable_Days (from 021_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "2_Staff_Task_Allocation_byDay" CASCADE;


CREATE MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay" AS
WITH jtd AS (
    -- Deduplicate 1_Job_Task_Details_Table by Job_Task_Staff_ID (preserves LIMIT 1 semantics)
    SELECT DISTINCT ON ("Job_Task_Staff_ID")
        "Job_Task_Staff_ID",
        "StartDateAdjusted",
        "DueDateAdjusted"
    FROM "1_Job_Task_Details_Table"
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


CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Date");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Job_Task_Staff_ID");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Staff_Name");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Is_Billable");
CREATE INDEX ON "2_Staff_Task_Allocation_byDay" ("Is_Workable_Day");


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
-- Workable_Days_Between_Task: Counts rows from 2_Staff_Task_Allocation_byDay where Job_Task_Staff_ID matches,
--   Is_Date_Between_Task_Days=TRUE, Is_Workable_Day=TRUE, and Is_Staff_Workable_DayOfWeek=TRUE.
-- Dependencies: SUPPORT_Job_Leave_Task_Details_Table_base_1 (from 025_create_materialized_views.sql), 2_Staff_Task_Allocation_byDay (#3)
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
        FROM "2_Staff_Task_Allocation_byDay" d
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
    SUPPORT_Staff_Leave_Allocation_byDay_base_2 b;


CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay ("Date");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay ("Staff_Name");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay ("Job_Task_Staff_ID");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay ("Is_DateBetweenTask");
