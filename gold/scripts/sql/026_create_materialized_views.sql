-- =============================================================================
-- MATERIALIZED VIEWS - Staff Task Allocation by Day
-- =============================================================================
-- Run AFTER 01_create_views.sql and 025_create_materialized_views.sql
-- This view creates the per-day task allocation matrix by cross-joining calendar dates
-- with unique job-task-staff combinations. It serves as the foundation for per-day
-- allocation calculations and staff performance metrics.
--
-- For daily refresh use 03_refresh_materialized_views.sql instead.
--
-- Creation order (dependency chain):
--   1. 2_Staff_Task_Allocation_byDay_base  (depends on key01_calendar_date from 01_create_views.sql, KEY02_Job_Task_Staff_ID from 025_create_materialized_views.sql)
--   2. 3_Staff_Performance_Table_base      (depends on key01_calendar_date, key03_staff_table from 01_create_views.sql)
-- =============================================================================
-- 2_Staff_Task_Allocation_byDay_base
-- DAX equivalent: 2_Staff_Task_Allocation_byDay = CROSSJOIN(KEY01_CalendarDate, KEY02_Job_Task_Staff_ID)
-- Base view combining every calendar date with every unique job-task-staff combination.
-- This creates the foundation for per-day task allocation calculations.
-- Dependencies: key01_calendar_date (from 01_create_views.sql), KEY02_Job_Task_Staff_ID (from 025_create_materialized_views.sql)
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


-- 3_Staff_Performance_Table_base
-- DAX equivalent: 3_Staff_Performance_Table = CROSSJOIN(KEY01_CalendarDate, KEY03_Staff_Table)
-- Base view combining every calendar date with every unique staff member.
-- This creates the foundation for per-day staff performance metrics and utilization calculations.
-- Dependencies: key01_calendar_date, key03_staff_table (from 01_create_views.sql)
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
