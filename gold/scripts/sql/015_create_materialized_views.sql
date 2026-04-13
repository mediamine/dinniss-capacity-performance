-- =============================================================================
-- MATERIALIZED VIEWS - Staff Performance Table
-- =============================================================================
-- Run AFTER 010_create_materialized_views.sql, 012_create_materialized_views.sql, and 014_create_materialized_views.sql
-- This view creates the per-day staff performance base by cross-joining calendar dates
-- with unique staff members. It serves as the foundation for per-day staff performance
-- metrics and utilization calculations.
--
-- For daily refresh use 020_refresh_materialized_views.sql instead.
--
-- Creation order (dependency chain):
--   1. 3_Staff_Performance_Table_base  (depends on key01_calendar_date, key03_staff_table from 010_create_materialized_views.sql)
--   2. 3_Staff_Performance_Table       (depends on #1, 2_Staff_Task_Allocation_byDay from 014, 4_Timesheet_Table from 013, EXCEL01_Staff_Workable_Days from 012, EXCEL05_Staff_Adjustment_Sheet from 012, excel07_staff_incentive+target_hours from 012)
-- =============================================================================
-- 3_Staff_Performance_Table_base
-- DAX equivalent: 3_Staff_Performance_Table = CROSSJOIN(KEY01_CalendarDate, KEY03_Staff_Table)
-- Base view combining every calendar date with every unique staff member.
-- This creates the foundation for per-day staff performance metrics and utilization calculations.
-- Dependencies: key01_calendar_date, key03_staff_table (from 010_create_materialized_views.sql)
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


-- 3_Staff_Performance_Table
-- DAX equivalent: 3_Staff_Performance_Table (extended with computed columns)
-- Extends 3_Staff_Performance_Table_base with workability flags, adjustment factors,
-- week-of-month, and overall recordable hours.
-- Dependencies: 3_Staff_Performance_Table_base, 2_Staff_Task_Allocation_byDay (from 014),
--               EXCEL01_Staff_Workable_Days (from 012), EXCEL05_Staff_Adjustment_Sheet (from 012)
--
-- Columns added:
--   Is_Workable_Day: TRUE if not public holiday, not weekend, and not a full-day leave
--   Is_Staff_Workable_DayOfWeek: LOOKUPVALUE from EXCEL01_Staff_Workable_Days by (Weekday, Staff_Name)
--   Adjustment_Factor_by_Month: LOOKUPVALUE from EXCEL05_Staff_Adjustment_Sheet by (StartOfMonth, Staff_Name); 0 if blank
--   Week_Of_Month: 1 + WEEKNUM(Date) - WEEKNUM(STARTOFMONTH(Date))
--   Overall_Recordable_Hours: IF(Is_Workable_Day AND Is_Staff_Workable_DayOfWeek, 8 * Adjustment_Factor_by_Month, 0)
--   Allocated_Holiday_Hours: SUM of Allo_Hrs_perWorkableDay_Final_Output where Task_Name contains 'Holiday', * -1; BLANK if Overall=0
--   Allocated_Other_Leave_Hours: SUM of Allo_Hrs where Task_Name contains 'Sick leave' or 'Other leave', * -1; BLANK if Overall=0
--   Non_Leave_Recordable_Hours: Overall + Holiday + Other_Leave; 0 if Overall=0
--   Allocated_Dinniss_Admin_Hours: SUM of Allo_Hrs where Task_Category = 'Admin Tasks'; BLANK if Overall=0
--   Available_Billable_Hours: Non_Leave_Recordable - Admin; 0 if Overall=0
--   Allocated_Billable_Hours_Original: SUM of FIN01 where Billable Tasks, by (Date, Staff_Name); BLANK if Overall=0
--   Allocated_Billable_Hours_Capacity_Planning: SUM of Final_Output where Billable Tasks, by (Date, Staff_Name); BLANK if Overall=0
--   Target_Billable_Hours: Available_Billable_Hours * LOOKUPVALUE(EXCEL07 Target_Billable_Hours)
--   Target_Non_Leave_Hours: Non_Leave_Recordable * LOOKUPVALUE(EXCEL07 Target_Billable_Hours); BLANK if <= 0
--   M2Date_Overall_Recordable_Hours: Overall_Recordable_Hours if Date < TODAY, else BLANK
--   M2Date_Target_Billable_Hours: Target_Billable_Hours if Date < TODAY, else BLANK
--   Annual_Leave_Hours_Recorded: SUM(Recorded_Minutes)/60 from 4_Timesheet_Table where Task_Name contains 'Holiday', * -1
--   Other_Leave_Hours_Recorded: SUM(Recorded_Minutes)/60 where Task_Name contains 'Sick leave' or 'Other leave', * -1
--   Dinniss_Admin_Hours_Recorded: SUM(Recorded_Minutes)/60 where Task_Category = 'Admin Tasks'
--   Billable_Hours_Recorded: SUM(Recorded_Minutes)/60 where Task_Category = 'Billable Tasks'
--   Overall_Hours_Recorded: (Annual_Leave + Other_Leave)*-1 + Billable + Admin recorded hours
--   Billable_Hours_Invoiced: SUM(Invoiced_Minutes)/60 where Invoiced_Time = 'Invoiced' and Billable Tasks
--   Billable_Hours_To_Be_Invoiced: SUM(Recorded_Minutes)/60 where Invoiced_Time = 'Un-Invoiced' and Billable Tasks
--   M2Date_Overall_Hours_Recorded: Overall_Hours_Recorded if Date < TODAY, else BLANK
--   Allocated_Non_Leave_Hours_Capacity_Planning_CHECK_VARIABLES: SUM(Admin+Billable Final_Output); BLANK if Overall=0
--   Adjustment_Factor_by_Day: LOOKUPVALUE(Adjustment Factor) from EXCEL01 by (Weekday, Staff_Name); 0 if blank
--   Is_Current_Month: TRUE if Date is in the same month/year as CURRENT_DATE
--   Target_Hours_to_be_Recorded: Target_Billable_Hours * Target_%Recorded_2_Billable_Hrs; BLANK if <= 0
--   Target_Hours_to_be_Allocated: Target_Billable_Hours * Target_%Allocated_2_Billable_Hrs; BLANK if <= 0
--   Target_Hours_to_be_Invoiced: Target_Billable_Hours * Target_%Invoiced_2_Billable_Hrs; BLANK if <= 0
--   Target_Billable_Percent: LOOKUPVALUE(Target_Billable_Hours by Date from EXCEL07); BLANK if 0
--   Target_Recordable Percent: LOOKUPVALUE(Target_%Recorded_2_Billable_Hrs by Date from EXCEL07); BLANK if 0
--   Target_Allocation_Percent: LOOKUPVALUE(Target_%Allocated_2_Billable_Hrs by Date from EXCEL07); BLANK if 0
--   Target_Invoice_Percent: LOOKUPVALUE(Target_%Invoiced_2_Billable_Hrs by Date from EXCEL07); BLANK if 0
DROP MATERIALIZED VIEW IF EXISTS "3_Staff_Performance_Table" CASCADE;


CREATE MATERIALIZED VIEW "3_Staff_Performance_Table" AS
WITH alloc_agg AS (
    -- Single scan of 2_Staff_Task_Allocation_byDay with conditional aggregation
    -- Replaces 6 separate CTEs (fdl, hol_agg, ol_agg, admin_agg, bill_fin01_agg, bill_final_agg)
    SELECT "Date", "Staff_Name",
           BOOL_OR("Is_Full_Day_Leave")                                                                    AS is_fdl,
           SUM(CASE WHEN "Task_Name" ILIKE '%Holiday%'                    THEN "Allo_Hrs_perWorkableDay_Final_Output" END) AS hol_hrs,
           SUM(CASE WHEN "Task_Name" ILIKE '%Sick leave%'
                      OR "Task_Name" ILIKE '%Other leave%'                THEN "Allo_Hrs_perWorkableDay_Final_Output" END) AS ol_hrs,
           SUM(CASE WHEN "Task_Category" = 'Admin Tasks'                  THEN "Allo_Hrs_perWorkableDay_Final_Output" END) AS admin_hrs,
           SUM(CASE WHEN "Task_Category" = 'Billable Tasks'               THEN "Allo_Hrs_perWorkDay_AdjLeaves_FIN01"  END) AS bill_fin01_hrs,
           SUM(CASE WHEN "Task_Category" = 'Billable Tasks'               THEN "Allo_Hrs_perWorkableDay_Final_Output" END) AS bill_final_hrs
    FROM "2_Staff_Task_Allocation_byDay"
    GROUP BY "Date", "Staff_Name"
),
wd AS (
    -- Deduplicate EXCEL01_Staff_Workable_Days by (Day of Week, StaffName)
    SELECT DISTINCT ON ("Day of Week", "StaffName")
        "Day of Week",
        "StaffName",
        "Working Day",
        "Adjustment Factor"
    FROM EXCEL01_Staff_Workable_Days
    ORDER BY "Day of Week", "StaffName"
),
adj AS (
    -- Deduplicate EXCEL05_Staff_Adjustment_Sheet by (Month, StaffName)
    SELECT DISTINCT ON ("Month", "StaffName")
        "Month",
        "StaffName",
        "AdjustmentFactor"
    FROM EXCEL05_Staff_Adjustment_Sheet
    ORDER BY "Month", "StaffName"
),
tgt AS (
    -- Deduplicate excel07_staff_incentive+target_hours by (Staff_Name, Month_Year)
    SELECT DISTINCT ON ("Staff_Name", "Month_Year")
        "Staff_Name",
        "Month_Year",
        "Target_Billable_Hours",
        "Target_%Recorded_2_Billable_Hrs",
        "Target_%Allocated_2_Billable_Hrs",
        "Target_%Invoiced_2_Billable_Hrs"
    FROM "excel07_staff_incentive+target_hours"
    ORDER BY "Staff_Name", "Month_Year"
),
tgt_date AS (
    -- Deduplicate excel07 by (Staff_Name, Month_Year) for Date-based lookup (Target_Billable_Percent)
    -- Matches Month_Year to Date (not StartOfMonth), so only returns on 1st of month
    SELECT DISTINCT ON ("Staff_Name", "Month_Year")
        "Staff_Name",
        "Month_Year",
        "Target_Billable_Hours" AS "Target_Billable_Hours_byDate",
        "Target_%Recorded_2_Billable_Hrs" AS "Target_Rec_Pct_byDate",
        "Target_%Allocated_2_Billable_Hrs" AS "Target_Allo_Pct_byDate",
        "Target_%Invoiced_2_Billable_Hrs" AS "Target_Inv_Pct_byDate"
    FROM "excel07_staff_incentive+target_hours"
    ORDER BY "Staff_Name", "Month_Year"
),
ts_agg AS (
    -- Single scan of 4_Timesheet_Table with conditional aggregation
    -- Joins on Staff_ID (UUID) for holiday/other leave, Staff_Name for admin/billable (matches DAX LOOKUPVALUEs)
    SELECT "Date", "Staff_ID", "Staff_Name",
           SUM(CASE WHEN "Task_Name" ILIKE '%Holiday%'
                    THEN "Recorded_Minutes" END) / 60.0                                    AS hol_rec_hrs,
           SUM(CASE WHEN "Task_Name" ILIKE '%Sick leave%' OR "Task_Name" ILIKE '%Other leave%'
                    THEN "Recorded_Minutes" END) / 60.0                                    AS ol_rec_hrs,
           SUM(CASE WHEN "Task_Category" = 'Admin Tasks'
                    THEN "Recorded_Minutes" END) / 60.0                                    AS admin_rec_hrs,
           SUM(CASE WHEN "Task_Category" = 'Billable Tasks'
                    THEN "Recorded_Minutes" END) / 60.0                                    AS bill_rec_hrs,
           SUM(CASE WHEN "Task_Category" = 'Billable Tasks' AND "Invoiced_Time" = 'Invoiced'
                    THEN "Invoiced_Minutes" END) / 60.0                                    AS bill_inv_hrs,
           SUM(CASE WHEN "Task_Category" = 'Billable Tasks' AND "Invoiced_Time" = 'Un-Invoiced'
                    THEN "Recorded_Minutes" END) / 60.0                                    AS bill_tbi_hrs
    FROM "4_Timesheet_Table"
    GROUP BY "Date", "Staff_ID", "Staff_Name"
),
base AS (
    SELECT
        b.*,
        -- Is_Workable_Day: IF(AND(PublicHoliday=FALSE, WeekEnd=FALSE), IF(Is_Full_Day_Leave=FALSE, TRUE, FALSE))
        CASE
            WHEN b."PublicHoliday" = FALSE AND b."WeekEnd" = FALSE THEN
                CASE WHEN COALESCE(aa.is_fdl, FALSE) = FALSE THEN TRUE ELSE FALSE END
        END AS "Is_Workable_Day",
        -- Is_Staff_Workable_DayOfWeek: LOOKUPVALUE(Working Day, Day of Week=Weekday, StaffName=Staff_Name)
        COALESCE(w."Working Day", FALSE) AS "Is_Staff_Workable_DayOfWeek",
        -- Adjustment_Factor_by_Day: LOOKUPVALUE(Adjustment Factor, Day of Week=Weekday, StaffName=Staff_Name)
        COALESCE(w."Adjustment Factor", 0) AS "Adjustment_Factor_by_Day",
        -- Adjustment_Factor_by_Month: IF(ISBLANK(AdjFac), 0, AdjFac)
        COALESCE(a."AdjustmentFactor", 0) AS "Adjustment_Factor_by_Month",
        -- Week_Of_Month: 1 + WEEKNUM(Date) - WEEKNUM(STARTOFMONTH(Date))
        -- Uses Sunday-start week calculation matching DAX WEEKNUM default
        1 + (EXTRACT(DAY FROM b."Date")::INT - 1 + EXTRACT(DOW FROM b."StartOfMonth"::DATE)::INT) / 7
            AS "Week_Of_Month",
        -- Pre-join aggregates for use in outer CTEs
        aa.hol_hrs,
        aa.ol_hrs,
        aa.admin_hrs,
        aa.bill_fin01_hrs,
        aa.bill_final_hrs,
        t."Target_Billable_Hours" AS tgt_bill_hrs,
        t."Target_%Recorded_2_Billable_Hrs" AS tgt_rec_pct,
        t."Target_%Allocated_2_Billable_Hrs" AS tgt_allo_pct,
        t."Target_%Invoiced_2_Billable_Hrs" AS tgt_inv_pct,
        td."Target_Billable_Hours_byDate" AS tgt_bill_bydate,
        td."Target_Rec_Pct_byDate" AS tgt_rec_pct_bydate,
        td."Target_Allo_Pct_byDate" AS tgt_allo_pct_bydate,
        td."Target_Inv_Pct_byDate" AS tgt_inv_pct_bydate,
        -- Pre-join timesheet aggregates for use in outer SELECT
        ts.hol_rec_hrs,
        ts.ol_rec_hrs,
        ts.admin_rec_hrs,
        ts.bill_rec_hrs,
        ts.bill_inv_hrs,
        ts.bill_tbi_hrs
    FROM "3_Staff_Performance_Table_base" b
    LEFT JOIN alloc_agg aa ON aa."Date" = b."Date" AND aa."Staff_Name" = b."Staff_Name"
    LEFT JOIN wd  w ON w."Day of Week" = b."Weekday" AND w."StaffName" = b."Staff_Name"
    LEFT JOIN adj a ON a."Month" = b."StartOfMonth" AND a."StaffName" = b."Staff_Name"
    LEFT JOIN tgt t ON t."Staff_Name" = b."Staff_Name" AND t."Month_Year" = b."StartOfMonth"
    LEFT JOIN tgt_date td ON td."Staff_Name" = b."Staff_Name" AND td."Month_Year" = b."Date"
    LEFT JOIN ts_agg ts ON ts."Date" = b."Date" AND ts."Staff_ID" = b."Staff_UUID"
),
base2 AS (
    SELECT
        base.*,
        -- Overall_Recordable_Hours: IF(AND(Is_Workable_Day=TRUE, Is_Staff_Workable_DayOfWeek=TRUE), 8*Adjustment_Factor_by_Month, 0)
        CASE
            WHEN base."Is_Workable_Day" = TRUE AND base."Is_Staff_Workable_DayOfWeek" = TRUE
            THEN 8 * base."Adjustment_Factor_by_Month"
            ELSE 0
        END AS "Overall_Recordable_Hours",
        -- Allocated_Holiday_Hours: IF(Overall=0, BLANK(), SUM(holiday hrs) * -1)
        CASE
            WHEN base."Is_Workable_Day" = TRUE AND base."Is_Staff_Workable_DayOfWeek" = TRUE
            THEN COALESCE(base.hol_hrs, 0) * -1
        END AS "Allocated_Holiday_Hours",
        -- Allocated_Other_Leave_Hours: IF(Overall=0, BLANK(), SUM(sick/other leave hrs) * -1)
        CASE
            WHEN base."Is_Workable_Day" = TRUE AND base."Is_Staff_Workable_DayOfWeek" = TRUE
            THEN COALESCE(base.ol_hrs, 0) * -1
        END AS "Allocated_Other_Leave_Hours",
        -- Allocated_Dinniss_Admin_Hours: IF(Overall=0, BLANK(), SUM(admin hrs))
        CASE
            WHEN base."Is_Workable_Day" = TRUE AND base."Is_Staff_Workable_DayOfWeek" = TRUE
            THEN COALESCE(base.admin_hrs, 0)
        END AS "Allocated_Dinniss_Admin_Hours",
        -- Allocated_Billable_Hours_Original: IF(Overall=0, BLANK(), SUM(FIN01) for Billable Tasks)
        CASE
            WHEN base."Is_Workable_Day" = TRUE AND base."Is_Staff_Workable_DayOfWeek" = TRUE
            THEN COALESCE(base.bill_fin01_hrs, 0)
        END AS "Allocated_Billable_Hours_Original",
        -- Allocated_Billable_Hours_Capacity_Planning: IF(Overall=0, BLANK(), SUM(Final_Output) for Billable Tasks)
        CASE
            WHEN base."Is_Workable_Day" = TRUE AND base."Is_Staff_Workable_DayOfWeek" = TRUE
            THEN COALESCE(base.bill_final_hrs, 0)
        END AS "Allocated_Billable_Hours_Capacity_Planning"
    FROM base
),
base3 AS (
    SELECT
        base2.*,
        -- Non_Leave_Recordable_Hours: IF(Overall=0, 0, Overall + Holiday + Other_Leave)
        CASE
            WHEN base2."Overall_Recordable_Hours" = 0 THEN 0
            ELSE base2."Overall_Recordable_Hours"
                 + COALESCE(base2."Allocated_Holiday_Hours", 0)
                 + COALESCE(base2."Allocated_Other_Leave_Hours", 0)
        END AS "Non_Leave_Recordable_Hours",
        -- Available_Billable_Hours: IF(Overall=0, 0, Non_Leave_Recordable - Admin)
        CASE
            WHEN base2."Overall_Recordable_Hours" = 0 THEN 0
            ELSE base2."Overall_Recordable_Hours"
                 + COALESCE(base2."Allocated_Holiday_Hours", 0)
                 + COALESCE(base2."Allocated_Other_Leave_Hours", 0)
                 - COALESCE(base2."Allocated_Dinniss_Admin_Hours", 0)
        END AS "Available_Billable_Hours"
    FROM base2
)
SELECT
    base3."Date",
    base3."PublicHoliday",
    base3."Weekday",
    base3."WeekEnd",
    base3."StartOfMonth",
    base3."EndOfMonth",
    base3."Is_Range_for_Invoicing",
    base3."Staff_UUID",
    base3."Staff_Name",
    base3."Is_Workable_Day",
    base3."Is_Staff_Workable_DayOfWeek",
    base3."Adjustment_Factor_by_Month",
    base3."Week_Of_Month",
    base3."Overall_Recordable_Hours",
    base3."Allocated_Holiday_Hours",
    base3."Allocated_Other_Leave_Hours",
    base3."Non_Leave_Recordable_Hours",
    base3."Allocated_Dinniss_Admin_Hours",
    base3."Available_Billable_Hours",
    base3."Allocated_Billable_Hours_Original",
    base3."Allocated_Billable_Hours_Capacity_Planning",
    -- Target_Billable_Hours: Available_Billable_Hours * Target_Billable_Hours lookup
    base3."Available_Billable_Hours" * base3.tgt_bill_hrs AS "Target_Billable_Hours",
    -- Target_Non_Leave_Hours: IF(Logic <= 0, BLANK(), Non_Leave_Recordable * Target_Billable_Hours)
    CASE
        WHEN base3."Non_Leave_Recordable_Hours" * COALESCE(base3.tgt_bill_hrs, 0) <= 0 THEN NULL
        ELSE base3."Non_Leave_Recordable_Hours" * base3.tgt_bill_hrs
    END AS "Target_Non_Leave_Hours",
    -- M2Date_Overall_Recordable_Hours: IF(Date >= TODAY, BLANK(), Overall_Recordable_Hours)
    CASE
        WHEN base3."Date" >= CURRENT_DATE THEN NULL
        ELSE base3."Overall_Recordable_Hours"
    END AS "M2Date_Overall_Recordable_Hours",
    -- M2Date_Target_Billable_Hours: IF(Date >= TODAY, BLANK(), Target_Billable_Hours)
    CASE
        WHEN base3."Date" >= CURRENT_DATE THEN NULL
        ELSE base3."Available_Billable_Hours" * base3.tgt_bill_hrs
    END AS "M2Date_Target_Billable_Hours",
    -- Annual_Leave_Hours_Recorded: SUM(Recorded_Minutes)/60 for Holiday tasks, * -1
    COALESCE(base3.hol_rec_hrs, 0) * -1 AS "Annual_Leave_Hours_Recorded",
    -- Other_Leave_Hours_Recorded: SUM(Recorded_Minutes)/60 for Sick/Other leave tasks, * -1
    COALESCE(base3.ol_rec_hrs, 0) * -1 AS "Other_Leave_Hours_Recorded",
    -- Dinniss_Admin_Hours_Recorded: SUM(Recorded_Minutes)/60 for Admin Tasks
    COALESCE(base3.admin_rec_hrs, 0) AS "Dinniss_Admin_Hours_Recorded",
    -- Billable_Hours_Recorded: SUM(Recorded_Minutes)/60 for Billable Tasks
    COALESCE(base3.bill_rec_hrs, 0) AS "Billable_Hours_Recorded",
    -- Overall_Hours_Recorded: (Annual_Leave + Other_Leave)*-1 + Billable + Admin
    (COALESCE(base3.hol_rec_hrs, 0) + COALESCE(base3.ol_rec_hrs, 0))
        + COALESCE(base3.bill_rec_hrs, 0) + COALESCE(base3.admin_rec_hrs, 0)
        AS "Overall_Hours_Recorded",
    -- Billable_Hours_Invoiced: SUM(Invoiced_Minutes)/60 for Billable Tasks where Invoiced_Time = 'Invoiced'
    COALESCE(base3.bill_inv_hrs, 0) AS "Billable_Hours_Invoiced",
    -- Billable_Hours_To_Be_Invoiced: SUM(Recorded_Minutes)/60 for Billable Tasks where Invoiced_Time = 'Un-Invoiced'
    COALESCE(base3.bill_tbi_hrs, 0) AS "Billable_Hours_To_Be_Invoiced",
    -- M2Date_Overall_Hours_Recorded: IF(Date >= TODAY, BLANK(), Overall_Hours_Recorded)
    CASE
        WHEN base3."Date" >= CURRENT_DATE THEN NULL
        ELSE (COALESCE(base3.hol_rec_hrs, 0) + COALESCE(base3.ol_rec_hrs, 0))
             + COALESCE(base3.bill_rec_hrs, 0) + COALESCE(base3.admin_rec_hrs, 0)
    END AS "M2Date_Overall_Hours_Recorded",
    -- Allocated_Non_Leave_Hours_Capacity_Planning_CHECK_VARIABLES: IF(Overall=0, BLANK(), SUM(Admin+Billable Final_Output))
    CASE
        WHEN base3."Overall_Recordable_Hours" = 0 THEN NULL
        ELSE COALESCE(base3.admin_hrs, 0) + COALESCE(base3.bill_final_hrs, 0)
    END AS "Allocated_Non_Leave_Hours_Capacity_Planning_CHECK_VARIABLES",
    -- Adjustment_Factor_by_Day: LOOKUPVALUE(Adjustment Factor, Day of Week=Weekday, StaffName=Staff_Name)
    base3."Adjustment_Factor_by_Day",
    -- Is_Current_Month: TRUE if Date is in the same month/year as TODAY
    DATE_TRUNC('month', base3."Date") = DATE_TRUNC('month', CURRENT_DATE) AS "Is_Current_Month",
    -- Target_Hours_to_be_Recorded: Target_Billable_Hours * Target_%Recorded_2_Billable_Hrs; BLANK if <= 0
    CASE
        WHEN (base3."Available_Billable_Hours" * base3.tgt_bill_hrs) * base3.tgt_rec_pct <= 0 THEN NULL
        ELSE (base3."Available_Billable_Hours" * base3.tgt_bill_hrs) * base3.tgt_rec_pct
    END AS "Target_Hours_to_be_Recorded",
    -- Target_Hours_to_be_Allocated: Target_Billable_Hours * Target_%Allocated_2_Billable_Hrs; BLANK if <= 0
    CASE
        WHEN (base3."Available_Billable_Hours" * base3.tgt_bill_hrs) * base3.tgt_allo_pct <= 0 THEN NULL
        ELSE (base3."Available_Billable_Hours" * base3.tgt_bill_hrs) * base3.tgt_allo_pct
    END AS "Target_Hours_to_be_Allocated",
    -- Target_Hours_to_be_Invoiced: Target_Billable_Hours * Target_%Invoiced_2_Billable_Hrs; BLANK if <= 0
    CASE
        WHEN (base3."Available_Billable_Hours" * base3.tgt_bill_hrs) * base3.tgt_inv_pct <= 0 THEN NULL
        ELSE (base3."Available_Billable_Hours" * base3.tgt_bill_hrs) * base3.tgt_inv_pct
    END AS "Target_Hours_to_be_Invoiced",
    -- Target_Billable_Percent: LOOKUPVALUE(Target_Billable_Hours by Date); BLANK if 0
    CASE
        WHEN COALESCE(base3.tgt_bill_bydate, 0) = 0 THEN NULL
        ELSE base3.tgt_bill_bydate
    END AS "Target_Billable_Percent",
    -- Target_Recordable Percent: LOOKUPVALUE(Target_%Recorded_2_Billable_Hrs by Date); BLANK if 0
    CASE
        WHEN COALESCE(base3.tgt_rec_pct_bydate, 0) = 0 THEN NULL
        ELSE base3.tgt_rec_pct_bydate
    END AS "Target_Recordable Percent",
    -- Target_Allocation_Percent: LOOKUPVALUE(Target_%Allocated_2_Billable_Hrs by Date); BLANK if 0
    CASE
        WHEN COALESCE(base3.tgt_allo_pct_bydate, 0) = 0 THEN NULL
        ELSE base3.tgt_allo_pct_bydate
    END AS "Target_Allocation_Percent",
    -- Target_Invoice_Percent: LOOKUPVALUE(Target_%Invoiced_2_Billable_Hrs by Date); BLANK if 0
    CASE
        WHEN COALESCE(base3.tgt_inv_pct_bydate, 0) = 0 THEN NULL
        ELSE base3.tgt_inv_pct_bydate
    END AS "Target_Invoice_Percent"
FROM base3;


CREATE INDEX ON "3_Staff_Performance_Table" ("Date");
CREATE INDEX ON "3_Staff_Performance_Table" ("Staff_Name");
CREATE INDEX ON "3_Staff_Performance_Table" ("Staff_UUID");
