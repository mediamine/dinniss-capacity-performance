-- =============================================================================
-- DAILY REFRESH — MATERIALIZED VIEWS
-- =============================================================================
-- Run this script daily (after source data updates, before Power BI refresh).
-- Does NOT recreate view structure — only reloads stored data.
--
-- Refreshes ALL materialized views created in 010–015 in dependency order.
-- Upstream views must be refreshed before downstream views, otherwise dependent
-- views will read stale data from their sources.
--
-- CONCURRENTLY: allows reads during refresh so Power BI is not blocked.
-- Requires a unique index on the view. Used here only for the 5 final
-- user-facing views; intermediate base views use plain REFRESH (short lock,
-- no index required).
--
-- HISTORY: CONCURRENTLY was briefly removed because it appeared to bloat the
-- Docker volume by ~90 GB per refresh. Investigation showed the bloat was
-- actually transient temp/WAL files held by the WSL2 ext4.vhdx (which never
-- shrinks back), not dead tuples in the matviews. CONCURRENTLY is being
-- restored for testing on a standard (non-Docker) PostgreSQL installation
-- where the WSL2 high-water-mark issue does not apply.
--
-- NOTE: key07_is_billable is a regular VIEW (not materialized) and does not need
-- refreshing.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Layer 0: Independent reference tables (010, 021)
-- No MV dependencies — safe to refresh in any order within this layer.
-- -----------------------------------------------------------------------------
REFRESH MATERIALIZED VIEW key01_calendar_date;
REFRESH MATERIALIZED VIEW key08_incentive_table_display_measure;
REFRESH MATERIALIZED VIEW key03_staff_table;
REFRESH MATERIALIZED VIEW key05_task_type;
REFRESH MATERIALIZED VIEW key04_task_name;
REFRESH MATERIALIZED VIEW key06_job_table;

REFRESH MATERIALIZED VIEW EXCEL01_Staff_Workable_Days;
REFRESH MATERIALIZED VIEW EXCEL02_Staff_Target_Sheet;
REFRESH MATERIALIZED VIEW EXCEL03_Public_Holidays;
REFRESH MATERIALIZED VIEW EXCEL04_Budget_Tracker;
REFRESH MATERIALIZED VIEW EXCEL05_Staff_Adjustment_Sheet;
REFRESH MATERIALIZED VIEW EXCEL06_Staff_Recorded_vs_Invoiced_Hours;
REFRESH MATERIALIZED VIEW "excel07_staff_incentive+target_hours";

-- -----------------------------------------------------------------------------
-- Layer 1: Invoice support tables (011)
-- Depends on: 010 (key06_job_table), raw base tables.
-- -----------------------------------------------------------------------------
REFRESH MATERIALIZED VIEW TOCHECK_ClientDetails;
REFRESH MATERIALIZED VIEW tocheck_clientdetails_2;
REFRESH MATERIALIZED VIEW TOCHECK_JobWithFinalInvoice;
REFRESH MATERIALIZED VIEW TOCHECK_Invoice;
REFRESH MATERIALIZED VIEW SUPPORT_Invoice_Task_Table;
REFRESH MATERIALIZED VIEW SUPPORT_InvoiceTaskUUID_MultipleStaff;

-- -----------------------------------------------------------------------------
-- Layer 2: Job/Task/Staff and Timesheet base chain (013)
-- Depends on: 010, 015, 021.
-- -----------------------------------------------------------------------------
REFRESH MATERIALIZED VIEW "1_Job_Task_Details_Table_base";
REFRESH MATERIALIZED VIEW "1_Job_Task_Details_Table_base_1";

REFRESH MATERIALIZED VIEW "4_Timesheet_Table_base";
REFRESH MATERIALIZED VIEW "4_Timesheet_Table_base_1";
REFRESH MATERIALIZED VIEW "4_Timesheet_Table_base_2";
REFRESH MATERIALIZED VIEW "4_Timesheet_Table_base_3";
REFRESH MATERIALIZED VIEW CONCURRENTLY "4_Timesheet_Table";

REFRESH MATERIALIZED VIEW KEYS_TIME;

REFRESH MATERIALIZED VIEW KEY02_Job_Task_Staff_ID_base;
REFRESH MATERIALIZED VIEW KEY02_Job_Task_Staff_ID_base_1;
REFRESH MATERIALIZED VIEW KEY02_Job_Task_Staff_ID_base_2;
REFRESH MATERIALIZED VIEW CONCURRENTLY KEY02_Job_Task_Staff_ID;

REFRESH MATERIALIZED VIEW SUPPORT_Staff_Leave_Allocation_byDay_base;
REFRESH MATERIALIZED VIEW SUPPORT_Staff_Leave_Allocation_byDay_base_1;
REFRESH MATERIALIZED VIEW SUPPORT_Staff_Leave_Allocation_byDay_base_2;

REFRESH MATERIALIZED VIEW SUPPORT_Job_Leave_Task_Details_Table_base;
REFRESH MATERIALIZED VIEW SUPPORT_Job_Leave_Task_Details_Table_base_1;

-- -----------------------------------------------------------------------------
-- Layer 3: Staff Task Allocation by Day chain (014)
-- Depends on: 010, 021, 025. Internal ordering follows 026 dependency chain.
-- -----------------------------------------------------------------------------
REFRESH MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay_base";
REFRESH MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay_base_1";

REFRESH MATERIALIZED VIEW SUPPORT_Job_Leave_Task_Details_Table_base_2;
REFRESH MATERIALIZED VIEW SUPPORT_Job_Leave_Task_Details_Table;
REFRESH MATERIALIZED VIEW SUPPORT_Staff_Leave_Allocation_byDay;

REFRESH MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay_base_2";
REFRESH MATERIALIZED VIEW "1_Job_Task_Details_Table_base_2";
REFRESH MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay_base_3";
REFRESH MATERIALIZED VIEW "1_Job_Task_Details_Table_base_3";
REFRESH MATERIALIZED VIEW "2_Staff_Task_Allocation_byDay_base_4";
REFRESH MATERIALIZED VIEW CONCURRENTLY "1_Job_Task_Details_Table";
REFRESH MATERIALIZED VIEW CONCURRENTLY "2_Staff_Task_Allocation_byDay";
REFRESH MATERIALIZED VIEW key07_is_billable;

-- -----------------------------------------------------------------------------
-- Layer 4: Staff Performance Table (015)
-- Depends on: 010, 021, 025, 026.
-- -----------------------------------------------------------------------------
REFRESH MATERIALIZED VIEW "3_Staff_Performance_Table_base";
REFRESH MATERIALIZED VIEW CONCURRENTLY "3_Staff_Performance_Table";

-- -----------------------------------------------------------------------------
-- Post-refresh vacuum: reclaims dead tuples left by CONCURRENTLY refreshes
-- back into PostgreSQL's free-space map so storage does not grow unboundedly
-- across daily runs.
-- -----------------------------------------------------------------------------
VACUUM ANALYZE "4_Timesheet_Table";
VACUUM ANALYZE KEY02_Job_Task_Staff_ID;
VACUUM ANALYZE "1_Job_Task_Details_Table";
VACUUM ANALYZE "2_Staff_Task_Allocation_byDay";
VACUUM ANALYZE "3_Staff_Performance_Table";
