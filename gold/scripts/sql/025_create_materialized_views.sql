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
--   4. 4_Timesheet_Table                   (depends on #1, #2, EXCEL06 from 021, #3)
--   5. KEYS_TIME                           (depends on #4)
-- 1_Job_Task_Details_Table_base
-- DAX equivalent: 1_Job_Task_Details_Table_base
-- Base view for job task details with staff assignment and client information.
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


-- 4_Timesheet_Table
-- DAX equivalent: 4_Timesheet_Table
-- Extended timesheet view with task name lookup from job details and Excel fallback.
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table" CASCADE;


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


CREATE INDEX ON "4_Timesheet_Table" ("Job_Task_Staff_ID");
CREATE INDEX ON "4_Timesheet_Table" ("Job_ID");
CREATE INDEX ON "4_Timesheet_Table" ("Staff_Name");
CREATE INDEX ON "4_Timesheet_Table" ("Date");


-- KEYS_TIME
-- DAX equivalent: KEYS_TIME
-- Job-task-staff combinations from timesheet records with task names.
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
