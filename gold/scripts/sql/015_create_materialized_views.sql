-- =============================================================================
-- MATERIALIZED VIEWS - Support Tables for Invoice Processing
-- =============================================================================
-- Run AFTER 01_create_views.sql
-- These views support invoice-related calculations and lookups.
-- For daily refresh use 03_refresh_materialized_views.sql instead.
--
-- Creation order (dependency chain):
--   1. TOCHECK_ClientDetails               (depends on clientdetails base table)
--   2. tocheck_clientdetails_2             (depends on clientdetails base table)
--   3. TOCHECK_JobWithFinalInvoice         (depends on invoice base table)
--   4. TOCHECK_Invoice                     (depends on invoice base table, key06_job_table from 01_create_views.sql)
--   5. SUPPORT_Invoice_Task_Table          (depends on invoicetask base table, #4 TOCHECK_Invoice)
--   6. SUPPORT_InvoiceTaskUUID_MultipleStaff (hardcoded UUID list, no dependencies)
-- =============================================================================
-- TOCHECK_ClientDetails
-- DAX equivalent: TOCHECK_ClientDetails
-- Lookup table for client details, excluding deleted records.
-- Dependencies: clientdetails base table
DROP MATERIALIZED VIEW IF EXISTS TOCHECK_ClientDetails CASCADE;
DROP MATERIALIZED VIEW IF EXISTS tocheck_clientdetails_2 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS TOCHECK_JobWithFinalInvoice CASCADE;
DROP MATERIALIZED VIEW IF EXISTS TOCHECK_Invoice CASCADE;
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Invoice_Task_Table CASCADE;
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_InvoiceTaskUUID_MultipleStaff CASCADE;


CREATE MATERIALIZED VIEW TOCHECK_ClientDetails AS
SELECT
    "UUID",
    "Name",
    "FirstName",
    "LastName",
    "OtherName",
    "Email",
    "Address",
    "City",
    "Region",
    "PostCode",
    "Country",
    "PostalAddress",
    "PostalCity",
    "PostalRegion",
    "PostalPostCode",
    "PostalCountry",
    "Phone",
    "Fax",
    "Website",
    "ReferralSource",
    "ExportCode",
    "IsProspect",
    "IsArchived",
    "TypeName",
    "TypePaymentTerm",
    "TypeCostMarkup",
    "TypePaymentDay",
    "AccountManagerUUID",
    "AccountManagerName",
    "JobManagerUUID",
    "JobManagerName",
    "NoteSummary",
    "BillingClientUUID",
    "TaxNumber",
    "CompanyNumber",
    "BusinessNumber",
    "BranchNumber",
    "BusinessStructure",
    "GSTRegistered",
    "PrepareGST",
    "SignedTaxAuthority",
    "TaxAgent",
    "AgencyStatus",
    "PrepareActivityStatement",
    "PrepareTaxReturn",
    "ActiveAtoClient",
    "ClientCode",
    "BalanceMonth",
    "BankBSB",
    "BankAccountName",
    "BankAccountNumber"
FROM
    clientdetails
WHERE
    "IsDeleted"::text NOT IN ('true', 'True', '1') OR "IsDeleted" IS NULL;


CREATE INDEX ON TOCHECK_ClientDetails ("UUID");
CREATE INDEX ON TOCHECK_ClientDetails ("Name");


-- tocheck_clientdetails_2
-- DAX equivalent: tocheck_clientdetails_2
-- Filtered version of client details, excluding specific client UUIDs and deleted records.
-- Dependencies: clientdetails base table
DROP MATERIALIZED VIEW IF EXISTS "tocheck_clientdetails_2" CASCADE;


CREATE MATERIALIZED VIEW "tocheck_clientdetails_2" AS
SELECT
    "UUID",
    "Name",
    "FirstName",
    "LastName",
    "OtherName",
    "Email",
    "Address",
    "City",
    "Region",
    "PostCode",
    "Country",
    "PostalAddress",
    "PostalCity",
    "PostalRegion",
    "PostalPostCode",
    "PostalCountry",
    "Phone",
    "Fax",
    "Website",
    "ReferralSource",
    "ExportCode",
    "IsProspect",
    "IsArchived",
    "TypeName",
    "TypePaymentTerm",
    "TypeCostMarkup",
    "TypePaymentDay",
    "AccountManagerUUID",
    "AccountManagerName",
    "JobManagerUUID",
    "JobManagerName",
    "NoteSummary",
    "BillingClientUUID",
    "TaxNumber",
    "CompanyNumber",
    "BusinessNumber",
    "BranchNumber",
    "BusinessStructure",
    "GSTRegistered",
    "PrepareGST",
    "SignedTaxAuthority",
    "TaxAgent",
    "AgencyStatus",
    "PrepareActivityStatement",
    "PrepareTaxReturn",
    "ActiveAtoClient",
    "ClientCode",
    "BalanceMonth",
    "BankBSB",
    "BankAccountName",
    "BankAccountNumber"
FROM
    clientdetails
WHERE
    ("IsDeleted"::text NOT IN ('true', 'True', '1') OR "IsDeleted" IS NULL)
    AND "UUID" NOT IN (
        '18A314B8-7DE2-4BAF-9E9D-25A029ACEAC6'::uuid,
        'B0872D14-5FA0-42FF-B3F4-E8DD3F30938C'::uuid,
        'DD8EA7E2-B933-42BB-BC2C-316F4E97C11B'::uuid
    );


CREATE INDEX ON "tocheck_clientdetails_2" ("UUID");
CREATE INDEX ON "tocheck_clientdetails_2" ("Name");


-- TOCHECK_JobWithFinalInvoice
-- DAX equivalent: TOCHECK_JobWithFinalInvoice
-- Lookup table for distinct job text values from final invoices only.
-- Dependencies: invoice base table
DROP MATERIALIZED VIEW IF EXISTS TOCHECK_JobWithFinalInvoice CASCADE;


CREATE MATERIALIZED VIEW TOCHECK_JobWithFinalInvoice AS
SELECT DISTINCT ON ("JobText")
    "Type",
    "JobText"
FROM
    invoice
WHERE
    ("IsDeleted"::text NOT IN ('true', 'True', '1') OR "IsDeleted" IS NULL)
    AND "Type" = 'Final Invoice'
ORDER BY "JobText";


CREATE INDEX ON TOCHECK_JobWithFinalInvoice ("JobText");


-- TOCHECK_Invoice
-- DAX equivalent: TOCHECK_Invoice
-- Lookup table for invoices from the invoice table with client details.
-- Dependencies: invoice base table, key06_job_table (from 01_create_views.sql)
DROP MATERIALIZED VIEW IF EXISTS TOCHECK_Invoice CASCADE;


CREATE MATERIALIZED VIEW TOCHECK_Invoice AS
SELECT
    i.*,
    -- Client: LOOKUPVALUE(KEY06_Job_Table[Client_Name], KEY06_Job_Table[Client_UUID], invoice[ClientUUID])
    (SELECT DISTINCT ON (k6."Client_UUID")
        k6."Client_Name"
     FROM key06_job_table k6
     WHERE k6."Client_UUID" = i."ClientUUID"
     ORDER BY k6."Client_UUID"
    ) AS "Client"
FROM
    invoice i;


CREATE INDEX ON TOCHECK_Invoice ("ID");


-- SUPPORT_Invoice_Task_Table
-- DAX equivalent: SUPPORT_Invoice_Task_Table
-- Lookup table for invoice tasks from the invoicetask table with invoice details.
-- Dependencies: invoicetask base table, TOCHECK_Invoice
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Invoice_Task_Table CASCADE;


CREATE MATERIALIZED VIEW SUPPORT_Invoice_Task_Table AS
SELECT
    it."UUID" AS "Invoice_Task_ID",
    it."Name" AS "Task_Type",
    it."Description",
    it."InvoiceID",
    it."LineNumber",
    it."JobID",
    it."Minutes",
    it."BillableRate",
    it."Billable",
    it."Amount",
    it."AmountTax",
    it."AmountIncludingTax",
    -- Invoiced_Date: LOOKUPVALUE(TOCHECK_Invoice[Date], TOCHECK_Invoice[ID], InvoiceID)
    ti."Date" AS "Invoiced_Date",
    -- Client: LOOKUPVALUE(TOCHECK_Invoice[Client], TOCHECK_Invoice[ID], InvoiceID)
    ti."Client",
    -- Invoiced_Month: DATE(YEAR(Invoiced_Date), MONTH(Invoiced_Date), 1)
    CASE
        WHEN ti."Date" IS NOT NULL
        THEN DATE_TRUNC('month', ti."Date")::DATE
    END AS "Invoiced_Month"
FROM
    invoicetask it
    LEFT JOIN TOCHECK_Invoice ti ON ti."ID" = it."InvoiceID"
WHERE
    it."IsDeleted" = FALSE;


CREATE INDEX ON SUPPORT_Invoice_Task_Table ("Invoice_Task_ID");


-- SUPPORT_InvoiceTaskUUID_MultipleStaff
-- DAX equivalent: SUPPORT_InvoiceTaskUUID_MultipleStaff
-- Lookup table of invoice task UUIDs that have multiple staff assigned.
-- Dependencies: None (hardcoded UUID list)
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_InvoiceTaskUUID_MultipleStaff CASCADE;


CREATE MATERIALIZED VIEW SUPPORT_InvoiceTaskUUID_MultipleStaff AS
SELECT uuid AS "Invoice_Task_UUID"
FROM (VALUES
    ('06169858-189A-4E2A-9632-E3EE4541A89D'::uuid),
    ('0B737432-DFC1-4EF0-9BD7-951E1A506958'::uuid),
    ('0FB1B9AB-DBED-4ECE-8DE3-C9298923380F'::uuid),
    ('201DD184-C7A8-4414-9958-F817041A5A6A'::uuid),
    ('224F6AA0-F9B6-407D-9129-829D40FD3134'::uuid),
    ('285943B4-38BE-4FC3-9D26-580E32754779'::uuid),
    ('2AD00F9B-541D-42FF-AEE9-B605AC2FE454'::uuid),
    ('2DEE2796-67FB-4CE4-BC1C-F0B5545EB841'::uuid),
    ('387799DF-1C56-4321-ADC5-78216ABDE48D'::uuid),
    ('3D600E3C-4102-4176-AE27-09B864031C7B'::uuid),
    ('41FEC410-831E-417C-B6F5-D83C78767E75'::uuid),
    ('48127A97-C4FD-45AE-B698-39022156EE4D'::uuid),
    ('505A0DE0-FD48-439F-ADAC-467149502008'::uuid),
    ('525A2A2D-FA8F-42DF-A01F-EDE4EE7E9E0F'::uuid),
    ('5413CF1B-7CE4-49CD-ACD1-81097862C1A1'::uuid),
    ('56B13F47-CDAF-4C5D-814D-9636CE83205D'::uuid),
    ('5F88C7EB-FE2B-4F8F-B352-7DA73DDCA0FF'::uuid),
    ('6DCCD63C-4F86-4A6B-97C8-307EF7CB4FA4'::uuid),
    ('826F5BDC-31DC-4A76-9BAB-81EA48C90240'::uuid),
    ('8473E504-21C8-4C9F-B4FA-16BAB066FF07'::uuid),
    ('85715A60-0532-4183-9759-54207466A8C5'::uuid),
    ('8D489545-E741-4503-BAA6-EA29657534A8'::uuid),
    ('993963CA-8EED-4A08-98F2-BF1FFF5C03CE'::uuid),
    ('9EFCF806-D4FA-426D-857D-5E5C6D448D01'::uuid),
    ('BC88529F-A284-479B-A991-B226FD81709F'::uuid),
    ('C501DBCC-8087-4696-BD86-431358E87EA8'::uuid),
    ('CE2B2054-2F03-4547-BBAD-C74F7D32194F'::uuid),
    ('CFB41A70-6F8C-4182-8B07-8C99A9D1A752'::uuid),
    ('E7A3EF64-0D59-40CB-87DB-B67A6C891065'::uuid),
    ('EA9645F9-A780-4FA9-ACB4-AC6C1017805E'::uuid),
    ('F10744F1-C066-4EFE-B977-93BB5C62B7A0'::uuid),
    ('FCAEA5DB-E69F-42B7-800B-EF5303BFEE0B'::uuid),
    ('FE1AB840-A953-469F-A91E-CF81DE217F65'::uuid),
    ('94B24BB2-9197-45A4-A1C4-5BF0E5E87F6E'::uuid),
    ('2D9A9857-2C4F-4659-BFB3-8692C0C52EDF'::uuid)
) AS t(uuid);


CREATE INDEX ON SUPPORT_InvoiceTaskUUID_MultipleStaff ("Invoice_Task_UUID");
