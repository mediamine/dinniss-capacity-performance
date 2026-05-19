-- =============================================================================
-- MATERIALIZED VIEWS - Excel-Based Reference Tables
-- =============================================================================
-- Run AFTER 010_create_materialized_views.sql and 011_create_materialized_views.sql
-- These views source from Excel workbooks synced from SharePoint and provide
-- reference data for staff workable days, targets, public holidays, budgets,
-- adjustments, recorded/invoiced hours, and incentive targets.
--
-- For daily refresh use 110_refresh_materialized_views.sql instead.
-- Only re-run this file when the view structure changes (adding/changing columns).
--
-- Creation order (dependency chain):
--   1. EXCEL01_Staff_Workable_Days          (raw table dependency only)
--   2. EXCEL02_Staff_Target_Sheet           (raw table dependency only)
--   3. EXCEL03_Public_Holidays              (raw table dependency only)
--   4. EXCEL04_Budget_Tracker               (raw table dependency only)
--   5. EXCEL05_Staff_Adjustment_Sheet       (raw table dependency only)
--   6. EXCEL06_Staff_Recorded_vs_Invoiced_Hours (raw table dependency only)
--   7. excel07_staff_incentive+target_hours (raw table dependency only)
--
-- EMPTY-SHEET HANDLING: each section below issues a `CREATE TABLE IF NOT EXISTS`
-- placeholder for its `excel_*` source. If `05_sharepoint_to_db.py` skipped the
-- corresponding Excel tab (e.g. the tab was blank), this guarantees the table
-- exists with the expected column names so the matview can still be created
-- (returning zero rows). When the sheet is later repopulated, the 05 script
-- detects the missing SCD2 columns via `is_scd2_table()` and rebuilds the
-- table as a real SCD2 table with the correct pandas-derived types.
-- =============================================================================
-- EXCEL01_Staff_Workable_Days
-- DAX equivalent: EXCEL01_Staff_Workable_Days
-- Source: EmployeeDashboard_ManualData.xlsx > "Workable days" sheet
-- Lookup table for staff workable days by day of week.
DROP MATERIALIZED VIEW IF EXISTS EXCEL01_Staff_Workable_Days CASCADE;

CREATE TABLE IF NOT EXISTS excel_workable_days (
    staffname text, staffid text, day_of_week text, day_name text,
    working_day text, adjustment_factor text, updated_on text
);

CREATE MATERIALIZED VIEW EXCEL01_Staff_Workable_Days AS
SELECT
    "staffname" AS "StaffName",
    "staffid" AS "StaffID",
    "day_of_week"::bigint AS "Day of Week",
    "day_name" AS "Day Name",
    "working_day"::boolean AS "Working Day",
    "adjustment_factor"::numeric AS "Adjustment Factor",
    "updated_on"::date AS "Updated_On"
FROM
    excel_workable_days;


CREATE INDEX ON EXCEL01_Staff_Workable_Days ("StaffName");


CREATE INDEX ON EXCEL01_Staff_Workable_Days ("Day of Week");


-- EXCEL02_Staff_Target_Sheet
-- DAX equivalent: EXCEL02_Staff_Target_Sheet
-- Source: EmployeeDashboard_ManualData.xlsx > "Staff_Target_Sheet" sheet
-- Lookup table for staff performance targets by month (utilisation, efficiency, profitability).
DROP MATERIALIZED VIEW IF EXISTS EXCEL02_Staff_Target_Sheet CASCADE;

CREATE TABLE IF NOT EXISTS excel_staff_target_sheet (
    staff_name text, staff_id text, startofmonth text,
    targetutilisation text, targetdinnissinternalefficiency text,
    completedtaskefficiency text, coachingtarget text,
    targetpotentialprofitability text, targetactualprofitability text
);

CREATE MATERIALIZED VIEW EXCEL02_Staff_Target_Sheet AS
SELECT
    "staff_name" AS "Staff.Name",
    "staff_id" AS "Staff.ID",
    "startofmonth"::date AS "StartOfMonth",
    "targetutilisation"::numeric AS "TargetUtilisation",
    "targetdinnissinternalefficiency"::numeric AS "TargetDinnissInternalEfficiency",
    "completedtaskefficiency"::numeric AS "CompletedTaskEfficiency",
    "coachingtarget"::numeric AS "CoachingTarget",
    "targetpotentialprofitability"::numeric AS "TargetPotentialProfitability",
    "targetactualprofitability"::numeric AS "TargetActualProfitability"
FROM
    excel_staff_target_sheet;


CREATE INDEX ON EXCEL02_Staff_Target_Sheet ("Staff.Name");


CREATE INDEX ON EXCEL02_Staff_Target_Sheet ("StartOfMonth");


-- EXCEL03_Public_Holidays
-- DAX equivalent: EXCEL03_Public_Holidays
-- Source: EmployeeDashboard_ManualData.xlsx > "Public_Holidays" sheet
-- Lookup table for public holidays (by date and holiday name).
DROP MATERIALIZED VIEW IF EXISTS EXCEL03_Public_Holidays CASCADE;

CREATE TABLE IF NOT EXISTS excel_public_holidays (
    "date" text, "day" text, holiday text, column1 text
);

CREATE MATERIALIZED VIEW EXCEL03_Public_Holidays AS
SELECT
    "date"::date AS "Date",
    "day" AS "Day",
    "holiday" AS "Holiday",
    "column1" AS "Column1"
FROM
    excel_public_holidays;


CREATE INDEX ON EXCEL03_Public_Holidays ("Date");


CREATE INDEX ON EXCEL03_Public_Holidays ("Holiday");


-- EXCEL04_Budget_Tracker
-- DAX equivalent: EXCEL04_Budget_Tracker
-- Source: EmployeeDashboard_ManualData.xlsx > "Budget_Tracker" sheet
-- Lookup table for budget tracking with actual vs target Gross Profit (GP) by month.
DROP MATERIALIZED VIEW IF EXISTS EXCEL04_Budget_Tracker CASCADE;

CREATE TABLE IF NOT EXISTS excel_budget_tracker (
    month_year text, actual_gp text, gp_target text
);

CREATE MATERIALIZED VIEW EXCEL04_Budget_Tracker AS
SELECT
    "month_year"::date AS "Month & Year",
    "actual_gp"::numeric AS "Actual GP",
    "gp_target"::numeric AS "GP Target"
FROM
    excel_budget_tracker;


CREATE INDEX ON EXCEL04_Budget_Tracker ("Month & Year");


-- EXCEL05_Staff_Adjustment_Sheet
-- DAX equivalent: EXCEL05_Staff_Adjustment_Sheet
-- Source: EmployeeDashboard_ManualData.xlsx > "Staff_Adjustment_Sheet" sheet
-- Lookup table for staff adjustment factors applied by month.
DROP MATERIALIZED VIEW IF EXISTS EXCEL05_Staff_Adjustment_Sheet CASCADE;

CREATE TABLE IF NOT EXISTS excel_staff_adjustment_sheet (
    staff_name text, staff_id text, startofmonth text,
    adjustmentfactor text, updated_on text
);

CREATE MATERIALIZED VIEW EXCEL05_Staff_Adjustment_Sheet AS
SELECT
    "staff_name" AS "StaffName",
    "staff_id" AS "Staff.ID",
    "startofmonth"::date AS "Month",
    "adjustmentfactor"::numeric AS "AdjustmentFactor",
    "updated_on"::date AS "Updated_On"
FROM
    excel_staff_adjustment_sheet;


CREATE INDEX ON EXCEL05_Staff_Adjustment_Sheet ("StaffName");


CREATE INDEX ON EXCEL05_Staff_Adjustment_Sheet ("Month");


-- EXCEL06_Staff_Recorded_vs_Invoiced_Hours
-- DAX equivalent: EXCEL06_Staff_Recorded_vs_Invoiced_Hours
-- Source: EmployeeDashboard_ManualData.xlsx > "Recorded_Invoiced_Hours" sheet
-- Lookup table for recorded vs invoiced hours by timesheet entry (for reconciliation).
DROP MATERIALIZED VIEW IF EXISTS EXCEL06_Staff_Recorded_vs_Invoiced_Hours CASCADE;

CREATE TABLE IF NOT EXISTS excel_recorded_invoiced_hours (
    timesheet_uuid text, staff text, "date" text, client text, task text,
    recorded_minutes text, invoiced_mins text
);

CREATE MATERIALIZED VIEW EXCEL06_Staff_Recorded_vs_Invoiced_Hours AS
SELECT
    timesheet_uuid AS "Timesheet_UUID",
    staff AS "Staff",
    date AS "Date",
    client AS "Client",
    task AS "Task",
    recorded_minutes::bigint AS "Recorded_Minutes",
    invoiced_mins::numeric AS "Invoiced_Mins"
FROM
    excel_recorded_invoiced_hours;


CREATE INDEX ON EXCEL06_Staff_Recorded_vs_Invoiced_Hours ("Timesheet_UUID");


CREATE INDEX ON EXCEL06_Staff_Recorded_vs_Invoiced_Hours ("Staff");


CREATE INDEX ON EXCEL06_Staff_Recorded_vs_Invoiced_Hours ("Date");


-- excel07_staff_incentive+target_hours
-- DAX equivalent: excel07_staff_incentive+target_hours
-- Source: EmployeeDashboard_ManualData.xlsx > "INCENTIVE_TARGETS" sheet
-- Lookup table for staff incentive targets and billable hours performance targets by month.
DROP MATERIALIZED VIEW IF EXISTS "excel07_staff_incentive+target_hours" CASCADE;

CREATE TABLE IF NOT EXISTS excel_incentive_targets (
    staff_name text, staff_id text, month_year text,
    target_billable_hours text, target_recorded_2_billable_hrs text,
    target_allocated_2_billable_hrs text, target_invoiced_2_billable_hrs text,
    updated_on text
);

CREATE MATERIALIZED VIEW "excel07_staff_incentive+target_hours" AS
SELECT
    "staff_name" AS "Staff_Name",
    "staff_id" AS "Staff_ID",
    "month_year"::date AS "Month_Year",
    "target_billable_hours"::numeric AS "Target_Billable_Hours",
    "target_recorded_2_billable_hrs"::bigint AS "Target_%Recorded_2_Billable_Hrs",
    "target_allocated_2_billable_hrs"::bigint AS "Target_%Allocated_2_Billable_Hrs",
    "target_invoiced_2_billable_hrs"::numeric AS "Target_%Invoiced_2_Billable_Hrs",
    "updated_on"::date AS "Updated_On"
FROM
    excel_incentive_targets;


CREATE INDEX ON "excel07_staff_incentive+target_hours" ("Staff_Name");


CREATE INDEX ON "excel07_staff_incentive+target_hours" ("Month_Year");
