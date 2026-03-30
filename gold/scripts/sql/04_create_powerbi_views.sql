-- =============================================================================
-- POWER BI VIEWS
-- =============================================================================
-- Purpose: Regular views wrapping each materialized view so Power BI can
--          discover and import them via the Navigator (Get Data → PostgreSQL).
--
-- Naming convention: pbi_ prefix to distinguish from the source MVs.
--
-- Run order: after 02_create_materialized_views.sql
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


DROP VIEW IF EXISTS "pbi_leave_status_by_staff_date" CASCADE;
CREATE OR REPLACE VIEW "pbi_leave_status_by_staff_date" AS
SELECT * FROM leave_status_by_staff_date;
