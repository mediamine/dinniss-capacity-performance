-- =============================================================================
-- DAILY REFRESH — MATERIALIZED VIEWS
-- =============================================================================
-- Run this script daily (after source data updates, before Power BI refresh).
-- Does NOT recreate view structure — only reloads stored data.
--
-- Refresh order matches the dependency chain:
--   leave_status_by_staff_date must refresh before 1_Job_Task_Details_Table
--   4_Timesheet_Table must refresh before key02_job_task_staff_id
--   key02_job_task_staff_id must refresh before 2_Staff_Task_Allocation_byDay
--   2_Staff_Task_Allocation_byDay + 4_Timesheet_Table must refresh before 3_Staff_Performance_Table
--
-- CONCURRENTLY: allows reads during refresh so Power BI is not blocked.
-- Requires a unique index on each view (all indexes are created in 02_create_materialized_views.sql).
--
-- NOTE: keys_time and key07_is_billable are regular VIEWs (not materialized),
-- they are always up to date and do not need refreshing.
-- =============================================================================

REFRESH MATERIALIZED VIEW CONCURRENTLY leave_status_by_staff_date;

REFRESH MATERIALIZED VIEW CONCURRENTLY "1_Job_Task_Details_Table";

REFRESH MATERIALIZED VIEW CONCURRENTLY "4_Timesheet_Table";

REFRESH MATERIALIZED VIEW CONCURRENTLY key02_job_task_staff_id;

REFRESH MATERIALIZED VIEW CONCURRENTLY "2_Staff_Task_Allocation_byDay";

REFRESH MATERIALIZED VIEW CONCURRENTLY "3_Staff_Performance_Table";
