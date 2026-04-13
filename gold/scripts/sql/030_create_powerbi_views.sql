-- =============================================================================
-- POWER BI VIEWS
-- =============================================================================
-- Purpose: Regular views wrapping each materialized view so Power BI can
--          discover and import them via the Navigator (Get Data → PostgreSQL).
--
-- Naming convention: pbi_ prefix to distinguish from the source MVs.
--
-- Run order: after 010_create_materialized_views.sql
-- Refresh:   not needed — these views always read the latest MV snapshot.
-- =============================================================================

DROP VIEW IF EXISTS "pbi_1_Job_Task_Details_Table" CASCADE;
CREATE OR REPLACE VIEW "pbi_1_Job_Task_Details_Table" AS
SELECT * FROM "1_Job_Task_Details_Table";


DROP VIEW IF EXISTS "pbi_2_Staff_Task_Allocation_byDay" CASCADE;
CREATE OR REPLACE VIEW "pbi_2_Staff_Task_Allocation_byDay" AS
SELECT * FROM "2_Staff_Task_Allocation_byDay";


DROP VIEW IF EXISTS "pbi_3_Staff_Performance_Table" CASCADE;
CREATE OR REPLACE VIEW "pbi_3_Staff_Performance_Table" AS
SELECT * FROM "3_Staff_Performance_Table";


DROP VIEW IF EXISTS "pbi_4_Timesheet_Table" CASCADE;
CREATE OR REPLACE VIEW "pbi_4_Timesheet_Table" AS
SELECT * FROM "4_Timesheet_Table";


DROP VIEW IF EXISTS "pbi_key02_job_task_staff_id" CASCADE;
CREATE OR REPLACE VIEW "pbi_key02_job_task_staff_id" AS
SELECT * FROM key02_job_task_staff_id;


DROP VIEW IF EXISTS "pbi_key07_is_billable" CASCADE;
CREATE OR REPLACE VIEW "pbi_key07_is_billable" AS
SELECT * FROM key07_is_billable;


-- -----------------------------------------------------------------------------
-- 010 key tables
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS "pbi_key01_calendar_date" CASCADE;
CREATE OR REPLACE VIEW "pbi_key01_calendar_date" AS
SELECT * FROM key01_calendar_date;


DROP VIEW IF EXISTS "pbi_key03_staff_table" CASCADE;
CREATE OR REPLACE VIEW "pbi_key03_staff_table" AS
SELECT * FROM key03_staff_table;


DROP VIEW IF EXISTS "pbi_key04_task_name" CASCADE;
CREATE OR REPLACE VIEW "pbi_key04_task_name" AS
SELECT * FROM key04_task_name;


DROP VIEW IF EXISTS "pbi_key05_task_type" CASCADE;
CREATE OR REPLACE VIEW "pbi_key05_task_type" AS
SELECT * FROM key05_task_type;


DROP VIEW IF EXISTS "pbi_key06_job_table" CASCADE;
CREATE OR REPLACE VIEW "pbi_key06_job_table" AS
SELECT * FROM key06_job_table;


DROP VIEW IF EXISTS "pbi_key08_incentive_table_display_measure" CASCADE;
CREATE OR REPLACE VIEW "pbi_key08_incentive_table_display_measure" AS
SELECT * FROM key08_incentive_table_display_measure;


-- -----------------------------------------------------------------------------
-- 011 invoice support
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS "pbi_tocheck_clientdetails" CASCADE;
CREATE OR REPLACE VIEW "pbi_tocheck_clientdetails" AS
SELECT * FROM TOCHECK_ClientDetails;


DROP VIEW IF EXISTS "pbi_tocheck_clientdetails_2" CASCADE;
CREATE OR REPLACE VIEW "pbi_tocheck_clientdetails_2" AS
SELECT * FROM tocheck_clientdetails_2;


DROP VIEW IF EXISTS "pbi_tocheck_jobwithfinalinvoice" CASCADE;
CREATE OR REPLACE VIEW "pbi_tocheck_jobwithfinalinvoice" AS
SELECT * FROM TOCHECK_JobWithFinalInvoice;


DROP VIEW IF EXISTS "pbi_tocheck_invoice" CASCADE;
CREATE OR REPLACE VIEW "pbi_tocheck_invoice" AS
SELECT * FROM TOCHECK_Invoice;


DROP VIEW IF EXISTS "pbi_support_invoice_task_table" CASCADE;
CREATE OR REPLACE VIEW "pbi_support_invoice_task_table" AS
SELECT * FROM SUPPORT_Invoice_Task_Table;


DROP VIEW IF EXISTS "pbi_support_invoicetaskuuid_multiplestaff" CASCADE;
CREATE OR REPLACE VIEW "pbi_support_invoicetaskuuid_multiplestaff" AS
SELECT * FROM SUPPORT_InvoiceTaskUUID_MultipleStaff;


-- -----------------------------------------------------------------------------
-- 012 Excel imports
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS "pbi_excel01_staff_workable_days" CASCADE;
CREATE OR REPLACE VIEW "pbi_excel01_staff_workable_days" AS
SELECT * FROM EXCEL01_Staff_Workable_Days;


DROP VIEW IF EXISTS "pbi_excel02_staff_target_sheet" CASCADE;
CREATE OR REPLACE VIEW "pbi_excel02_staff_target_sheet" AS
SELECT * FROM EXCEL02_Staff_Target_Sheet;


DROP VIEW IF EXISTS "pbi_excel03_public_holidays" CASCADE;
CREATE OR REPLACE VIEW "pbi_excel03_public_holidays" AS
SELECT * FROM EXCEL03_Public_Holidays;


DROP VIEW IF EXISTS "pbi_excel04_budget_tracker" CASCADE;
CREATE OR REPLACE VIEW "pbi_excel04_budget_tracker" AS
SELECT * FROM EXCEL04_Budget_Tracker;


DROP VIEW IF EXISTS "pbi_excel05_staff_adjustment_sheet" CASCADE;
CREATE OR REPLACE VIEW "pbi_excel05_staff_adjustment_sheet" AS
SELECT * FROM EXCEL05_Staff_Adjustment_Sheet;


DROP VIEW IF EXISTS "pbi_excel06_staff_recorded_vs_invoiced_hours" CASCADE;
CREATE OR REPLACE VIEW "pbi_excel06_staff_recorded_vs_invoiced_hours" AS
SELECT * FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours;


DROP VIEW IF EXISTS "pbi_excel07_staff_incentive+target_hours" CASCADE;
CREATE OR REPLACE VIEW "pbi_excel07_staff_incentive+target_hours" AS
SELECT * FROM "excel07_staff_incentive+target_hours";


-- -----------------------------------------------------------------------------
-- 013 / 014 non-base support
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS "pbi_keys_time" CASCADE;
CREATE OR REPLACE VIEW "pbi_keys_time" AS
SELECT * FROM KEYS_TIME;


DROP VIEW IF EXISTS "pbi_support_job_leave_task_details_table" CASCADE;
CREATE OR REPLACE VIEW "pbi_support_job_leave_task_details_table" AS
SELECT * FROM SUPPORT_Job_Leave_Task_Details_Table;


DROP VIEW IF EXISTS "pbi_support_staff_leave_allocation_byday" CASCADE;
CREATE OR REPLACE VIEW "pbi_support_staff_leave_allocation_byday" AS
SELECT * FROM SUPPORT_Staff_Leave_Allocation_byDay;
