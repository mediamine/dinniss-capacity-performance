-- =============================================================================
-- MATERIALIZED VIEWS - Staff Performance Table (Base)
-- =============================================================================
-- Run AFTER 010_create_views.sql
-- This view creates the per-day staff performance base by cross-joining calendar dates
-- with unique staff members. It serves as the foundation for per-day staff performance
-- metrics and utilization calculations.
--
-- For daily refresh use 030_refresh_materialized_views.sql instead.
--
-- Creation order (dependency chain):
--   1. 3_Staff_Performance_Table_base  (depends on key01_calendar_date, key03_staff_table from 010_create_views.sql)
-- =============================================================================
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
