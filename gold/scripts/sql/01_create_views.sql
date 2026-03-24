-- independent key views
DROP VIEW IF EXISTS key01_calendar_date CASCADE;


CREATE OR REPLACE VIEW key01_calendar_date AS
SELECT
    gs::DATE AS "Date",
    EXISTS (
        SELECT
            1
        FROM
            excel_public_holidays eph
        WHERE
            eph.DATE = gs::DATE
    ) AS "PublicHoliday",
    EXTRACT(
        DOW
        FROM
            gs
    )::INT + 1 AS "Weekday",
    (
        EXTRACT(
            DOW
            FROM
                gs
        )::INT + 1
    ) < 2
    OR (
        EXTRACT(
            DOW
            FROM
                gs
        )::INT + 1
    ) > 6 AS "WeekEnd",
    DATE_TRUNC('month', gs)::DATE AS "StartOfMonth",
    (
        DATE_TRUNC('month', gs) + INTERVAL '1 month' - INTERVAL '1 day'
    )::DATE AS "EndOfMonth",
    (
        (
            DATE_TRUNC('month', gs) + INTERVAL '1 month' - INTERVAL '1 day'
        )::DATE <= (
            DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day'
        )::DATE
        AND (
            DATE_TRUNC('month', gs) + INTERVAL '1 month' - INTERVAL '1 day'
        )::DATE > '2020-04-30'::DATE
    ) AS "Is_Range_for_Invoicing"
FROM
    generate_series(
        '2020-01-01'::DATE,
        (
            DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '4 months' - INTERVAL '1 day'
        )::DATE,
        '1 day'::INTERVAL
    ) gs;


DROP VIEW IF EXISTS key08_incentive_table_display_measure CASCADE;


CREATE OR REPLACE VIEW key08_incentive_table_display_measure AS
SELECT
    metric AS "Metric",
    'Selected Metric: ' || SUBSTRING(
        metric
        FROM
            4
    ) AS "Name",
    CASE metric
        WHEN '1. Allocated Billable Hours' THEN 'Target Metric: Hours to be Allocated'
        WHEN '2. Recorded Billable Hours' THEN 'Target Metric: Hours to be Recorded'
        WHEN '3. Invoiced Billable Hours' THEN 'Target Metric: Hours to be Invoiced'
        WHEN '4. % Allocated to Billable Hours' THEN 'Target Metric: Hours to be Allocated'
        WHEN '5. % Recorded to Billable Hours' THEN 'Target Metric: Hours to be Recorded'
        ELSE 'Target Metric: Hours to be Invoiced'
    END AS "Target_Metric"
FROM
    unnest(
        ARRAY[
            '1. Allocated Billable Hours',
            '2. Recorded Billable Hours',
            '3. Invoiced Billable Hours',
            '4. % Allocated to Billable Hours',
            '5. % Recorded to Billable Hours',
            '6. % Invoiced to Billable Hours'
        ]
    ) AS metric;


-- key views dependent on another table or view
DROP VIEW IF EXISTS key03_staff_table;


CREATE OR REPLACE VIEW key03_staff_table AS
SELECT DISTINCT
    "StaffID" AS "Staff_UUID",
    "Name" AS "Staff_Name"
FROM
    jobtaskassignee
WHERE
    "Name" NOT IN (
        'Anna Williams',
        'Caroline Dinniss',
        'Conor Cameron',
        'Conor O''Brien',
        'Dinniss',
        'Sahar Sedaghat',
        'The OLD - Dani Millar',
        'Vicky Jones'
    )
    AND "Name" IS NOT NULL;


DROP VIEW IF EXISTS key04_task_name CASCADE;


CREATE OR REPLACE VIEW key04_task_name AS
SELECT DISTINCT
    "Name" AS "Task_Name"
FROM
    jobtask;


DROP VIEW IF EXISTS key05_task_type CASCADE;


CREATE OR REPLACE VIEW key05_task_type AS
SELECT
    "UUID" AS "TaskType_UUID",
    "Name"
FROM
    task;


DROP VIEW IF EXISTS key06_job_table CASCADE;


CREATE OR REPLACE VIEW key06_job_table AS
SELECT
    jd."UUID" AS "Job_UUID",
    jd."ID" AS "Job_ID",
    jd."Name" AS "Job_Name",
    jd."Description" AS "Job_Description",
    jd."ClientUUID" AS "Client_UUID",
    cd."Name" AS "Client_Name",
    ja."UUID" AS "Staff_UUID",
    ja."Name" AS "Staff_Name",
    jd."State" AS "Job_State",
    jd."StartDate",
    jd."DueDate",
    jd."CompletedDate"
FROM
    jobdetails jd
    LEFT JOIN clientdetails cd ON cd."UUID" = jd."ClientUUID"::uuid
    LEFT JOIN jobassignee ja ON ja."JobID" = jd."RemoteID";


-- views dependent on another table or view
DROP VIEW IF EXISTS "1_Job_Task_Details_Table" CASCADE;


CREATE OR REPLACE VIEW "1_Job_Task_Details_Table" AS
SELECT
    (
        jt."JobDetailsRemoteID" || jt."UUID" || jta."UUID"
    ) AS "Job_Task_Staff_ID",
    jt."JobDetailsRemoteID" AS "Job_ID",
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
    jta."AllocatedMinutes" AS "Task_Allocated_Mins"
FROM
    jobtask jt
    LEFT JOIN jobtaskassignee jta ON jta."JobTaskID" = jt."RemoteID"::uuid
    LEFT JOIN jobdetails jd ON jd."RemoteID" = jt."JobDetailsRemoteID"
    LEFT JOIN clientdetails cd ON cd."UUID" = jd."ClientUUID"
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


DROP VIEW IF EXISTS "4_Timesheet_Table" CASCADE;


CREATE OR REPLACE VIEW "4_Timesheet_Table" AS
SELECT
    (t."JobID" || t."TaskUUID" || t."StaffMemberUUID") AS "Job_Task_Staff_ID",
    t."UUID" AS "Timesheet_UUID",
    t."JobID" AS "Job_ID",
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
    TIME t
    LEFT JOIN "key03_staff_table" s ON s."Staff_UUID" = t."StaffMemberUUID"
WHERE
    t."Date" >= '2020-01-01'
    AND s."Staff_Name" IS NOT NULL;


DROP VIEW IF EXISTS keys_time CASCADE;


CREATE OR REPLACE VIEW keys_time AS
SELECT
    "Job_Task_Staff_ID",
    "Job_ID",
    "Task_ID",
    "Staff_Name",
    "Staff_ID" AS "Staff_UUID"
FROM
    "4_Timesheet_Table";


DROP VIEW IF EXISTS key02_job_task_staff_id CASCADE;


CREATE OR REPLACE VIEW key02_job_task_staff_id AS
SELECT DISTINCT
    combined."Job_Task_Staff_ID",
    combined."Job_ID",
    combined."Staff_Name",
    -- Task_Name: LOOKUPVALUE from 1_Job_Task_Details_Table
    jt."Task_Name",
    -- Client_Name: LOOKUPVALUE from key06_job_table
    j."Client_Name",
    -- Job_Name: LOOKUPVALUE from key06_job_table
    j."Job_Name",
    -- Task_Category
    CASE
        WHEN jt."Task_Name" ILIKE '%Holiday%'
        OR jt."Task_Name" ILIKE '%Other leave%'
        OR jt."Task_Name" ILIKE '%Sick leave%' THEN 'Leave Tasks'
        WHEN jt."Task_Name" ILIKE '%Admin - Non-billable%'
        OR j."Client_Name" = 'Dinniss Admin' THEN 'Admin Tasks'
        ELSE 'Billable Tasks'
    END AS "Task_Category",
    -- Task_Type1: match Task_Name against key05_task_type Names
    COALESCE(
        (
            SELECT
                kt."Name"
            FROM
                "key05_task_type" kt
            WHERE
                jt."Task_Name" ILIKE ('%' || kt."Name" || '%')
            LIMIT
                1
        ),
        jt."Task_Name"
    ) AS "Task_Type1"
FROM
    (
        SELECT
            "Job_Task_Staff_ID",
            "Job_ID",
            "Task_UUID" AS "Task_ID",
            "Staff_UUID",
            "Staff_Name"
        FROM
            "1_Job_Task_Details_Table"
        UNION ALL
        SELECT
            "Job_Task_Staff_ID",
            "Job_ID",
            "Task_ID",
            "Staff_UUID",
            "Staff_Name"
        FROM
            keys_time
    ) combined
    LEFT JOIN "1_Job_Task_Details_Table" jt ON jt."Job_Task_Staff_ID" = combined."Job_Task_Staff_ID"
    LEFT JOIN "key06_job_table" j ON j."Job_ID" = combined."Job_ID";


DROP VIEW IF EXISTS "2_Staff_Task_Allocation_byDay" CASCADE;


CREATE OR REPLACE VIEW "2_Staff_Task_Allocation_byDay" AS
SELECT
    c.*,
    k.*,
    -- Is_Client
    CASE
        WHEN k."Client_Name" = 'Dinniss Admin'
        OR k."Task_Type1" = 'Admin - Non-billable' THEN FALSE
        ELSE TRUE
    END AS "Is_Client",
    -- Is_Billable
    CASE
        WHEN k."Client_Name" = 'Dinniss Admin'
        OR k."Task_Type1" = 'Admin - Non-billable'
        OR k."Task_Type1" = 'Coaching' THEN FALSE
        ELSE TRUE
    END AS "Is_Billable",
    -- Billable_Selector
    CASE
        WHEN k."Client_Name" = 'Dinniss Admin'
        OR k."Task_Type1" = 'Admin - Non-billable'
        OR k."Task_Type1" = 'Coaching' THEN 'Not Billable'
        ELSE 'Billable'
    END AS "Billable_Selector"
FROM
    key01_calendar_date c
    CROSS JOIN key02_job_task_staff_id k;


DROP VIEW IF EXISTS key07_is_billable CASCADE;


CREATE OR REPLACE VIEW key07_is_billable AS
SELECT DISTINCT
    "Billable_Selector"
FROM
    "2_Staff_Task_Allocation_byDay";


DROP VIEW IF EXISTS "3_Staff_Performance_Table" CASCADE;


CREATE OR REPLACE VIEW "3_Staff_Performance_Table" AS
SELECT
    c.*,
    s.*
FROM
    key01_calendar_date c
    CROSS JOIN key03_staff_table s;