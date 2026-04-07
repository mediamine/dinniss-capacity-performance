-- =============================================================================
-- MATERIALIZED VIEWS - Job Task & Timesheet Details
-- =============================================================================
-- Run AFTER 01_create_views.sql, 015_create_materialized_views.sql, and 021_create_materialized_views.sql
-- These views extend job task and timesheet data with details and lookups.
-- For daily refresh use 03_refresh_materialized_views.sql instead.
--
-- Creation order (dependency chain):
--   1. 1_Job_Task_Details_Table_base       (depends on TOCHECK_ClientDetails from 015)
--   2. 1_Job_Task_Details_Table            (depends on #1)
--   3. 4_Timesheet_Table_base              (raw table dependency only)
--   4. 4_Timesheet_Table_base_1            (depends on #2, EXCEL06 from 021, #3)
--   5. 4_Timesheet_Table_base_2            (depends on #4, SUPPORT_Invoice_Task_Table from 015)
--   6. 4_Timesheet_Table_base_3            (depends on #5, SUPPORT_Invoice_Task_Table from 015, EXCEL06 from 021)
--   7. 4_Timesheet_Table                   (depends on #6)
--   8. KEYS_TIME                           (depends on #7)
--   9. KEY02_Job_Task_Staff_ID             (depends on #2, #8)
--
-- TODO: Optimization refactoring needed:
--   - Task_Category: Move from base_2 to base_1 (only depends on Task_Name, Client_Name available in base_1)
--   - Month_Time_Invoiced: Precompute in base_3 to avoid 6× subquery re-execution in Month_Invoiced_On
--   - Month_Invoiced_On: Simplify to reference precomputed Month_Time_Invoiced instead of running subquery 6 times
--   - Is_Billable/Billable_Selector: Reference b."Task_Type" directly instead of re-querying (available in base_1)
-- 1_Job_Task_Details_Table_base
-- DAX equivalent: 1_Job_Task_Details_Table_base
-- Base view for job task details with staff assignment and client information.
-- Dependencies: TOCHECK_ClientDetails (from 015_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table_base" CASCADE;


CREATE MATERIALIZED VIEW "1_Job_Task_Details_Table_base" AS
SELECT
    (jt."JobDetailsRemoteID"::text || jt."UUID"::text || jta."UUID"::text) AS "Job_Task_Staff_ID",
    jt."JobDetailsRemoteID"::text AS "Job_ID",
    jt."UUID" AS "Task_UUID",
    jt."Name" AS "Task_Name",
    jt."EstimatedMinutes",
    jt."ActualMinutes",
    jt."Billable" AS "Task_Billable",
    jt."Completed" AS "Task_Completed",
    jta."UUID" AS "Staff_UUID",
    jta."Name" AS "Staff_Name",
    cd."Name" AS "Client_Name",
    jt."StartDate",
    jt."DueDate",
    jta."AllocatedMinutes"::float AS "Task_Allocated_Mins"
FROM
    jobtask jt
    LEFT JOIN jobtaskassignee jta ON jta."JobTaskID" = jt."RemoteID"::uuid
    LEFT JOIN jobdetails jd ON jd."RemoteID" = jt."JobDetailsRemoteID"
    LEFT JOIN TOCHECK_ClientDetails cd ON cd."UUID" = jd."ClientUUID"::uuid
WHERE
    jt."IsDeleted" = FALSE
    AND jta."Name" IS NOT NULL
    AND jta."Name" NOT IN (
        'Anna Williams',
        'Conor Cameron',
        'Conor O''Brien',
        'Dinniss',
        'Sahar Sedaghat',
        'The OLD - Dani Millar'
    );


CREATE INDEX ON "1_Job_Task_Details_Table_base" ("Job_Task_Staff_ID");
CREATE INDEX ON "1_Job_Task_Details_Table_base" ("Job_ID");
CREATE INDEX ON "1_Job_Task_Details_Table_base" ("Staff_Name");


-- 1_Job_Task_Details_Table
-- DAX equivalent: 1_Job_Task_Details_Table
-- Extended view with task type and adjusted date columns.
-- Dependencies: 1_Job_Task_Details_Table_base, key04_task_name, key06_job_table (from 01_create_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table" CASCADE;


CREATE MATERIALIZED VIEW "1_Job_Task_Details_Table" AS
SELECT
    b."Job_Task_Staff_ID",
    b."Job_ID",
    b."Task_UUID",
    b."Task_Name",
    -- Task_Type: LOOKUPVALUE(KEY04_Task_Name[Task_Type], KEY04_Task_Name[Task_Name], Task_Name)
    kt."Task_Type",
    b."EstimatedMinutes",
    b."ActualMinutes",
    b."Task_Billable",
    b."Task_Completed",
    b."Staff_UUID",
    b."Staff_Name",
    b."Client_Name",
    b."StartDate",
    b."DueDate",
    b."Task_Allocated_Mins",
    -- StartDateAdjusted: IF(ISBLANK(StartDate), LOOKUPVALUE(KEY06_Job_Table[StartDate], ...), StartDate)
    COALESCE(b."StartDate", kj."StartDate") AS "StartDateAdjusted",
    -- DueDateAdjusted: IF(ISBLANK(DueDate), LOOKUPVALUE(KEY06_Job_Table[EarlierDate], ...), DueDate)
    COALESCE(b."DueDate", kj."EarlierDate") AS "DueDateAdjusted"
FROM
    "1_Job_Task_Details_Table_base" b
    LEFT JOIN (
        SELECT DISTINCT ON ("Task_Name")
            "Task_Name",
            "Task_Type"
        FROM key04_task_name
        ORDER BY "Task_Name"
    ) kt ON kt."Task_Name" = b."Task_Name"
    LEFT JOIN (
        SELECT DISTINCT ON ("Job_ID")
            "Job_ID",
            "StartDate",
            "EarlierDate"
        FROM key06_job_table
        ORDER BY "Job_ID"
    ) kj ON kj."Job_ID" = b."Job_ID";


CREATE INDEX ON "1_Job_Task_Details_Table" ("Job_Task_Staff_ID");
CREATE INDEX ON "1_Job_Task_Details_Table" ("Job_ID");
CREATE INDEX ON "1_Job_Task_Details_Table" ("Staff_Name");


-- 4_Timesheet_Table_base
-- DAX equivalent: 4_Timesheet_Table_base
-- Timesheet data joined with staff names, filtered from 2020 onwards.
-- Dependencies: key03_staff_table (from 01_create_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table_base" CASCADE;


CREATE MATERIALIZED VIEW "4_Timesheet_Table_base" AS
SELECT
    (t."JobID"::text || t."TaskUUID"::text || t."StaffMemberUUID"::text) AS "Job_Task_Staff_ID",
    t."UUID" AS "Timesheet_UUID",
    t."JobID"::text AS "Job_ID",
    t."TaskUUID" AS "Task_ID",
    t."StaffMemberUUID" AS "Staff_ID",
    s."Staff_Name",
    t."Note" AS "Timesheet_Notes",
    t."Billable",
    t."InvoiceUUID" AS "Invoice_ID",
    t."InvoiceTaskUUID" AS "Invoice_Task_ID",
    t."Date",
    t."Minutes" AS "Recorded_Minutes"
FROM
    "time" t
    LEFT JOIN key03_staff_table s ON s."Staff_UUID" = t."StaffMemberUUID"
WHERE
    t."Date" >= '2020-01-01'::timestamp
    AND s."Staff_Name" IS NOT NULL;


CREATE INDEX ON "4_Timesheet_Table_base" ("Job_Task_Staff_ID");
CREATE INDEX ON "4_Timesheet_Table_base" ("Job_ID");
CREATE INDEX ON "4_Timesheet_Table_base" ("Staff_Name");
CREATE INDEX ON "4_Timesheet_Table_base" ("Date");


-- 4_Timesheet_Table_base_1
-- DAX equivalent: 4_Timesheet_Table (extended version)
-- Extended timesheet view with task name lookup from job details and Excel fallback.
-- Dependencies: 4_Timesheet_Table_base, 1_Job_Task_Details_Table, EXCEL06_Staff_Recorded_vs_Invoiced_Hours (from 021_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table_base_1" CASCADE;


CREATE MATERIALIZED VIEW "4_Timesheet_Table_base_1" AS
SELECT
    b."Job_Task_Staff_ID",
    b."Timesheet_UUID",
    b."Job_ID",
    b."Task_ID",
    b."Staff_ID",
    b."Staff_Name",
    b."Timesheet_Notes",
    b."Billable",
    b."Invoice_ID",
    b."Invoice_Task_ID",
    b."Date",
    b."Recorded_Minutes",
    -- Task_Name: LOOKUPVALUE from 1_Job_Task_Details_Table, fallback to EXCEL06 if blank
    COALESCE(
        (SELECT DISTINCT ON ("Job_Task_Staff_ID")
            "Task_Name"
         FROM "1_Job_Task_Details_Table"
         WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
         ORDER BY "Job_Task_Staff_ID"
         LIMIT 1),
        (SELECT DISTINCT ON ("Timesheet_UUID")
            "Task"
         FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
         WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
         ORDER BY "Timesheet_UUID"
         LIMIT 1)
    ) AS "Task_Name",
    -- Client_Name: LOOKUPVALUE from 1_Job_Task_Details_Table, fallback to EXCEL06 if blank
    COALESCE(
        (SELECT DISTINCT ON ("Job_Task_Staff_ID")
            "Client_Name"
         FROM "1_Job_Task_Details_Table"
         WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
         ORDER BY "Job_Task_Staff_ID"
         LIMIT 1),
        (SELECT DISTINCT ON ("Timesheet_UUID")
            "Client"
         FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
         WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
         ORDER BY "Timesheet_UUID"
         LIMIT 1)
    ) AS "Client_Name",
    -- Task_Type: LOOKUPVALUE from 1_Job_Task_Details_Table, fallback to EXCEL06 if blank
    COALESCE(
        (SELECT DISTINCT ON ("Job_Task_Staff_ID")
            "Task_Type"
         FROM "1_Job_Task_Details_Table"
         WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
         ORDER BY "Job_Task_Staff_ID"
         LIMIT 1),
        (SELECT DISTINCT ON ("Timesheet_UUID")
            "Task"
         FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
         WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
         ORDER BY "Timesheet_UUID"
         LIMIT 1)
    ) AS "Task_Type",
    -- Task_Completed: LOOKUPVALUE from 1_Job_Task_Details_Table
    (SELECT DISTINCT ON ("Job_Task_Staff_ID")
        "Task_Completed"
     FROM "1_Job_Task_Details_Table"
     WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
     ORDER BY "Job_Task_Staff_ID"
     LIMIT 1) AS "Task_Completed",
    -- Is_Client: TRUE if not (Dinniss Admin client OR Admin - Non-billable task)
    NOT (
        COALESCE(
            (SELECT DISTINCT ON ("Job_Task_Staff_ID")
                "Client_Name"
             FROM "1_Job_Task_Details_Table"
             WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
             ORDER BY "Job_Task_Staff_ID"
             LIMIT 1),
            (SELECT DISTINCT ON ("Timesheet_UUID")
                "Client"
             FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
             WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
             ORDER BY "Timesheet_UUID"
             LIMIT 1)
        ) = 'Dinniss Admin'
        OR
        COALESCE(
            (SELECT DISTINCT ON ("Job_Task_Staff_ID")
                "Task_Type"
             FROM "1_Job_Task_Details_Table"
             WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
             ORDER BY "Job_Task_Staff_ID"
             LIMIT 1),
            (SELECT DISTINCT ON ("Timesheet_UUID")
                "Task"
             FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
             WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
             ORDER BY "Timesheet_UUID"
             LIMIT 1)
        ) = 'Admin - Non-billable'
    ) AS "Is_Client"
FROM
    "4_Timesheet_Table_base" b;


CREATE INDEX ON "4_Timesheet_Table_base_1" ("Job_Task_Staff_ID");
CREATE INDEX ON "4_Timesheet_Table_base_1" ("Job_ID");
CREATE INDEX ON "4_Timesheet_Table_base_1" ("Staff_Name");
CREATE INDEX ON "4_Timesheet_Table_base_1" ("Date");


-- 4_Timesheet_Table_base_2
-- DAX equivalent: 4_Timesheet_Table (with invoicing, billability, and categorization columns)
-- Extended timesheet view with invoicing status, billability classification, and task categorization.
-- Dependencies: 4_Timesheet_Table_base_1, SUPPORT_Invoice_Task_Table (from 015_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table_base_2" CASCADE;


CREATE MATERIALIZED VIEW "4_Timesheet_Table_base_2" AS
SELECT
    b."Job_Task_Staff_ID",
    b."Timesheet_UUID",
    b."Job_ID",
    b."Task_ID",
    b."Staff_ID",
    b."Staff_Name",
    b."Timesheet_Notes",
    b."Billable",
    b."Invoice_ID",
    b."Invoice_Task_ID",
    b."Date",
    b."Recorded_Minutes",
    b."Task_Name",
    b."Client_Name",
    b."Task_Type",
    b."Task_Completed",
    b."Is_Client",
    -- Invoiced_Time: Categorizes timesheet based on invoice status and client type
    CASE
        WHEN b."Invoice_Task_ID" IS NULL AND b."Is_Client" = TRUE THEN 'Un-Invoiced'
        WHEN b."Invoice_Task_ID" IS NULL THEN 'Dinniss Time'
        ELSE 'Invoiced'
    END AS "Invoiced_Time",
    -- Is_Billable: FALSE if not client work OR coaching task, else TRUE
    CASE
        WHEN b."Is_Client" = FALSE
            OR COALESCE(
                (SELECT DISTINCT ON ("Job_Task_Staff_ID")
                    "Task_Type"
                 FROM "1_Job_Task_Details_Table"
                 WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
                 ORDER BY "Job_Task_Staff_ID"
                 LIMIT 1),
                (SELECT DISTINCT ON ("Timesheet_UUID")
                    "Task"
                 FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
                 WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
                 ORDER BY "Timesheet_UUID"
                 LIMIT 1)
            ) = 'Coaching'
        THEN FALSE
        ELSE TRUE
    END AS "Is_Billable",
    -- Billable_Selector: Simple categorization based on Is_Billable logic
    CASE
        WHEN b."Is_Client" = FALSE
            OR COALESCE(
                (SELECT DISTINCT ON ("Job_Task_Staff_ID")
                    "Task_Type"
                 FROM "1_Job_Task_Details_Table"
                 WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
                 ORDER BY "Job_Task_Staff_ID"
                 LIMIT 1),
                (SELECT DISTINCT ON ("Timesheet_UUID")
                    "Task"
                 FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
                 WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
                 ORDER BY "Timesheet_UUID"
                 LIMIT 1)
            ) = 'Coaching'
        THEN 'Not Billable'
        ELSE 'Billable'
    END AS "Billable_Selector",
    -- Task_Category: Categorizes based on task name content and client type
    CASE
        WHEN (
            b."Task_Name" ILIKE '%Holiday%'
            OR b."Task_Name" ILIKE '%Other leave%'
        ) THEN 'Leave Tasks'
        WHEN b."Task_Name" ILIKE '%Sick leave%' THEN 'Leave Tasks'
        WHEN (
            b."Task_Name" ILIKE '%Admin - Non-billable%'
            OR b."Client_Name" = 'Dinniss Admin'
        ) THEN 'Admin Tasks'
        ELSE 'Billable Tasks'
    END AS "Task_Category",
    -- Invoice_Number: Lookup invoice ID from SUPPORT_Invoice_Task_Table
    (SELECT DISTINCT ON ("Invoice_Task_ID")
        "InvoiceID"
     FROM SUPPORT_Invoice_Task_Table
     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
     ORDER BY "Invoice_Task_ID"
     LIMIT 1) AS "Invoice_Number",
    -- Month_Time_Recorded: Month of timesheet date
    DATE_TRUNC('month', b."Date")::DATE AS "Month_Time_Recorded"
FROM
    "4_Timesheet_Table_base_1" b;


CREATE INDEX ON "4_Timesheet_Table_base_2" ("Job_Task_Staff_ID");
CREATE INDEX ON "4_Timesheet_Table_base_2" ("Job_ID");
CREATE INDEX ON "4_Timesheet_Table_base_2" ("Staff_Name");
CREATE INDEX ON "4_Timesheet_Table_base_2" ("Date");


-- 4_Timesheet_Table_base_3
-- DAX equivalent: 4_Timesheet_Table (with invoice timing columns)
-- Timesheet view with invoice month tracking and invoiced minutes reconciliation.
-- Dependencies: 4_Timesheet_Table_base_2, SUPPORT_Invoice_Task_Table (from 015_create_materialized_views.sql), EXCEL06_Staff_Recorded_vs_Invoiced_Hours (from 021_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table_base_3" CASCADE;


CREATE MATERIALIZED VIEW "4_Timesheet_Table_base_3" AS
SELECT
    b."Job_Task_Staff_ID",
    b."Timesheet_UUID",
    b."Job_ID",
    b."Task_ID",
    b."Staff_ID",
    b."Staff_Name",
    b."Timesheet_Notes",
    b."Billable",
    b."Invoice_ID",
    b."Invoice_Task_ID",
    b."Date",
    b."Recorded_Minutes",
    b."Task_Name",
    b."Client_Name",
    b."Task_Type",
    b."Task_Completed",
    b."Is_Client",
    b."Invoiced_Time",
    b."Is_Billable",
    b."Billable_Selector",
    b."Task_Category",
    b."Invoice_Number",
    b."Month_Time_Recorded",
    -- Month_Time_Invoiced: Lookup invoiced month from SUPPORT_Invoice_Task_Table
    (SELECT DISTINCT ON ("Invoice_Task_ID", "InvoiceID")
        "Invoiced_Month"
     FROM SUPPORT_Invoice_Task_Table
     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
       AND "InvoiceID" = b."Invoice_Number"
     ORDER BY "Invoice_Task_ID", "InvoiceID"
     LIMIT 1) AS "Month_Time_Invoiced",
    -- Invoiced_Minutes: Lookup invoiced minutes from EXCEL06
    (SELECT DISTINCT ON ("Timesheet_UUID")
        "Invoiced_Mins"
     FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
     WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
     ORDER BY "Timesheet_UUID"
     LIMIT 1) AS "Invoiced_Minutes",
    -- Month_Invoiced_On: Calculate months between recorded and invoiced dates
    CASE
        WHEN b."Invoice_Number" IS NULL THEN NULL
        ELSE
            CASE
                WHEN (EXTRACT(YEAR FROM (
                    SELECT DISTINCT ON ("Invoice_Task_ID", "InvoiceID")
                        "Invoiced_Month"
                     FROM SUPPORT_Invoice_Task_Table
                     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
                       AND "InvoiceID" = b."Invoice_Number"
                     ORDER BY "Invoice_Task_ID", "InvoiceID"
                     LIMIT 1
                )) - EXTRACT(YEAR FROM b."Month_Time_Recorded")) * 12 +
                (EXTRACT(MONTH FROM (
                    SELECT DISTINCT ON ("Invoice_Task_ID", "InvoiceID")
                        "Invoiced_Month"
                     FROM SUPPORT_Invoice_Task_Table
                     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
                       AND "InvoiceID" = b."Invoice_Number"
                     ORDER BY "Invoice_Task_ID", "InvoiceID"
                     LIMIT 1
                )) - EXTRACT(MONTH FROM b."Month_Time_Recorded")) = -1
                THEN 1
                ELSE (EXTRACT(YEAR FROM (
                    SELECT DISTINCT ON ("Invoice_Task_ID", "InvoiceID")
                        "Invoiced_Month"
                     FROM SUPPORT_Invoice_Task_Table
                     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
                       AND "InvoiceID" = b."Invoice_Number"
                     ORDER BY "Invoice_Task_ID", "InvoiceID"
                     LIMIT 1
                )) - EXTRACT(YEAR FROM b."Month_Time_Recorded")) * 12 +
                (EXTRACT(MONTH FROM (
                    SELECT DISTINCT ON ("Invoice_Task_ID", "InvoiceID")
                        "Invoiced_Month"
                     FROM SUPPORT_Invoice_Task_Table
                     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
                       AND "InvoiceID" = b."Invoice_Number"
                     ORDER BY "Invoice_Task_ID", "InvoiceID"
                     LIMIT 1
                )) - EXTRACT(MONTH FROM b."Month_Time_Recorded")) + 1
            END
    END AS "Month_Invoiced_On"
FROM
    "4_Timesheet_Table_base_2" b;


CREATE INDEX ON "4_Timesheet_Table_base_3" ("Job_Task_Staff_ID");
CREATE INDEX ON "4_Timesheet_Table_base_3" ("Job_ID");
CREATE INDEX ON "4_Timesheet_Table_base_3" ("Staff_Name");
CREATE INDEX ON "4_Timesheet_Table_base_3" ("Date");


-- 4_Timesheet_Table
-- DAX equivalent: 4_Timesheet_Table (with recorded hours invoicing classification)
-- Final timesheet view with invoicing timeline categorization for billable tasks.
-- Dependencies: 4_Timesheet_Table_base_3
CREATE MATERIALIZED VIEW "4_Timesheet_Table" AS
SELECT
    b."Job_Task_Staff_ID",
    b."Timesheet_UUID",
    b."Job_ID",
    b."Task_ID",
    b."Staff_ID",
    b."Staff_Name",
    b."Timesheet_Notes",
    b."Billable",
    b."Invoice_ID",
    b."Invoice_Task_ID",
    b."Date",
    b."Recorded_Minutes",
    b."Task_Name",
    b."Client_Name",
    b."Task_Type",
    b."Task_Completed",
    b."Is_Client",
    b."Invoiced_Time",
    b."Is_Billable",
    b."Billable_Selector",
    b."Task_Category",
    b."Invoice_Number",
    b."Month_Time_Recorded",
    b."Month_Time_Invoiced",
    b."Invoiced_Minutes",
    b."Month_Invoiced_On",
    -- Recorded_Hours_invoiced: Categorizes when billable tasks were invoiced relative to recording
    CASE
        WHEN b."Task_Category" = 'Billable Tasks'
        THEN
            CASE
                WHEN b."Invoiced_Time" = 'Un-Invoiced' THEN '5_Un-Invoiced'
                WHEN b."Month_Invoiced_On" = 1 THEN '1_Same Month'
                WHEN b."Month_Invoiced_On" = 2 THEN '2_Following Month'
                WHEN b."Month_Invoiced_On" = 3 THEN '3_Third Month'
                ELSE '4_Fourth Month +'
            END
        ELSE NULL
    END AS "Recorded_Hours_invoiced"
FROM
    "4_Timesheet_Table_base_3" b;


CREATE INDEX ON "4_Timesheet_Table" ("Job_Task_Staff_ID");
CREATE INDEX ON "4_Timesheet_Table" ("Job_ID");
CREATE INDEX ON "4_Timesheet_Table" ("Staff_Name");
CREATE INDEX ON "4_Timesheet_Table" ("Date");


-- KEYS_TIME
-- DAX equivalent: KEYS_TIME
-- Job-task-staff combinations from timesheet records with task names.
-- Dependencies: 4_Timesheet_Table
DROP MATERIALIZED VIEW IF EXISTS KEYS_TIME CASCADE;


CREATE MATERIALIZED VIEW KEYS_TIME AS
SELECT
    "Job_Task_Staff_ID",
    "Job_ID",
    "Task_ID",
    "Task_Name",
    "Staff_ID" AS "Staff_UUID",
    "Staff_Name"
FROM
    "4_Timesheet_Table";


CREATE INDEX ON KEYS_TIME ("Job_Task_Staff_ID");
CREATE INDEX ON KEYS_TIME ("Job_ID");
CREATE INDEX ON KEYS_TIME ("Staff_Name");


-- KEY02_Job_Task_Staff_ID
-- DAX equivalent: KEY02_Job_Task_Staff_ID
-- Unique job-task-staff combinations from job task details and timesheet records.
-- Combines 1_Job_Task_Details_Table and KEYS_TIME, then deduplicates by Job_Task_Staff_ID.
-- Dependencies: 1_Job_Task_Details_Table, KEYS_TIME
DROP MATERIALIZED VIEW IF EXISTS KEY02_Job_Task_Staff_ID CASCADE;


CREATE MATERIALIZED VIEW KEY02_Job_Task_Staff_ID AS
SELECT DISTINCT ON ("Job_Task_Staff_ID")
    "Job_Task_Staff_ID",
    "Job_ID",
    "Task_Type",
    "Staff_Name",
    "StartDateAdjusted",
    "DueDateAdjusted",
    "Task_Name"
FROM (
    -- From 1_Job_Task_Details_Table (excluding timesheet-only records)
    SELECT
        "Job_Task_Staff_ID",
        "Job_ID",
        "Task_Type",
        "Staff_Name",
        "StartDateAdjusted",
        "DueDateAdjusted",
        CAST(NULL AS VARCHAR) AS "Task_Name"
    FROM "1_Job_Task_Details_Table"

    UNION ALL

    -- From KEYS_TIME (timesheet records without job task details)
    SELECT
        "Job_Task_Staff_ID",
        "Job_ID",
        CAST(NULL AS VARCHAR) AS "Task_Type",
        "Staff_Name",
        CAST(NULL AS DATE) AS "StartDateAdjusted",
        CAST(NULL AS DATE) AS "DueDateAdjusted",
        "Task_Name"
    FROM KEYS_TIME
) combined
ORDER BY "Job_Task_Staff_ID";


CREATE INDEX ON KEY02_Job_Task_Staff_ID ("Job_Task_Staff_ID");
CREATE INDEX ON KEY02_Job_Task_Staff_ID ("Job_ID");
CREATE INDEX ON KEY02_Job_Task_Staff_ID ("Staff_Name");
