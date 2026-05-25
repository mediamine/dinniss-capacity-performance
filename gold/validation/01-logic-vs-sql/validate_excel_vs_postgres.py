"""
Same validation as scripts/validate_excel_vs_sql.py, but runs the SQL
transformations against the PostgreSQL instance configured by
POSTGRES_CONNECTION_TEST in the .env file (searched at the repo root, then
gold/.env).

Each run:
  1. Drops the `validation_test` schema (if any) and recreates it — clean slate.
  2. Inserts the Excel scenario data into source tables in that schema.
  3. Runs the materialized-view SQL chain inside that schema.
  4. Probes the resulting tables for each step and writes a timestamped PDF.
  5. **Leaves the tables in place** afterward so the run can be inspected via
     DBeaver / psql / pgAdmin.

Only DuckDB ↔ Postgres dialect difference touched: `::DOUBLE` (DuckDB alias)
is rewritten as `::float8` (Postgres alias for double precision).
"""
from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2 import extras as pg_extras
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    KeepTogether,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

# ---------------------------------------------------------------------------
# Scenario (matches Excel exactly — identical to DuckDB validator)
# ---------------------------------------------------------------------------
TODAY = dt.date(2021, 1, 11)
TASK_START = dt.date(2020, 12, 23)
TASK_DUE = dt.date(2021, 3, 31)
LEAVE_START = dt.date(2020, 12, 23)
LEAVE_DUE = dt.date(2021, 3, 31)

WORK_TASK_ALLOC_HRS = 65
LEAVE_TASK_ALLOC_HRS = 25
TASK_WORKED_HRS = 1

PUBLIC_HOLIDAYS = {
    dt.date(2020, 12, 25),
    dt.date(2021, 1, 1),
    dt.date(2021, 1, 6),
    dt.date(2021, 1, 25),
    dt.date(2021, 2, 8),
}
CARL_WORKABLE_DOW = {"Mon", "Tue", "Wed"}
STAFF = "Carl"

WORK_TASK_ID = "WORK-TASK-CARL"
LEAVE_TASK_ID = "LEAVE-CARL"

SCHEMA = "validation_test"
LOCAL_DIR = Path(__file__).resolve().parent            # gold/validation/01-logic-vs-sql/
REPO_ROOT = Path(__file__).resolve().parents[3]        # repo root (3 levels up)


def make_output_path() -> Path:
    """Timestamped PDF path, e.g. reports/excel-vs-postgres-validation_2026-05-24_01-34-22.pdf."""
    ts = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return LOCAL_DIR / "reports" / f"excel-vs-postgres-validation_{ts}.pdf"


def dow_short(d: dt.date) -> str:
    return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][d.weekday()]


# ---------------------------------------------------------------------------
# Build input tables (identical to DuckDB validator)
# ---------------------------------------------------------------------------
def build_calendar_df() -> pd.DataFrame:
    rows = []
    d = TASK_START
    while d <= TASK_DUE:
        rows.append({
            "Date": d,
            "Weekday": dow_short(d),
            "WeekEnd": d.weekday() >= 5,
            "PublicHoliday": d in PUBLIC_HOLIDAYS,
            "StartOfMonth": d.day == 1,
            "EndOfMonth": (d + dt.timedelta(days=1)).month != d.month,
            "Is_Range_for_Invoicing": True,
        })
        d += dt.timedelta(days=1)
    return pd.DataFrame(rows)


def build_staff_workable_days_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"Day of Week": dow, "StaffName": STAFF, "Working Day": (dow in CARL_WORKABLE_DOW)}
        for dow in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    ])


def build_tasks_df() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "Job_Task_Staff_ID": WORK_TASK_ID,
            "Job_ID": "JOB-1",
            "Task_UUID": "uuid-work",
            "Task_Name": "Work Task",
            "Task_Type": "Billable",
            "Task_Type1": "Billable - Standard",
            "Task_Category": "Billable Tasks",
            "Staff_UUID": "uuid-carl",
            "Staff_Name": STAFF,
            "Client_Name": "Client A",
            "Job_Name": "Job 1",
            "StartDateAdjusted": TASK_START,
            "DueDateAdjusted": TASK_DUE,
            "Task_Allocated_Mins": WORK_TASK_ALLOC_HRS * 60,
        },
        {
            "Job_Task_Staff_ID": LEAVE_TASK_ID,
            "Job_ID": "JOB-LEAVE",
            "Task_UUID": "uuid-leave",
            "Task_Name": "Holiday — Annual Leave",
            "Task_Type": "Leave",
            "Task_Type1": "Leave",
            "Task_Category": "Leave Tasks",
            "Staff_UUID": "uuid-carl",
            "Staff_Name": STAFF,
            "Client_Name": "Dinniss Admin",
            "Job_Name": "Leave",
            "StartDateAdjusted": LEAVE_START,
            "DueDateAdjusted": LEAVE_DUE,
            "Task_Allocated_Mins": LEAVE_TASK_ALLOC_HRS * 60,
        },
    ])


def build_timesheet_df() -> pd.DataFrame:
    return pd.DataFrame([{
        "Job_Task_Staff_ID": WORK_TASK_ID,
        "Date": TODAY - dt.timedelta(days=1),
        "Recorded_Minutes": 60,
    }])


# ---------------------------------------------------------------------------
# Database connection + schema management
# ---------------------------------------------------------------------------
def load_connection_string() -> str:
    """Read POSTGRES_CONNECTION_TEST, searching .env at repo root then gold/.env."""
    for candidate in (REPO_ROOT / ".env", REPO_ROOT / "gold" / ".env"):
        if candidate.exists():
            load_dotenv(candidate, override=False)
    dsn = os.getenv("POSTGRES_CONNECTION_TEST")
    if not dsn:
        raise SystemExit(
            "POSTGRES_CONNECTION_TEST is not set.\n"
            "Add it to .env (repo root or gold/.env), e.g.:\n"
            "  POSTGRES_CONNECTION_TEST=postgresql://user:pass@localhost:5432/test_db"
        )
    return dsn


def reset_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
        cur.execute(f'CREATE SCHEMA "{SCHEMA}"')
        cur.execute(f'SET search_path TO "{SCHEMA}"')


def create_source_tables(conn) -> None:
    """Explicit DDLs for the four input tables (input grain is small, no inference needed)."""
    ddls = [
        """
        CREATE TABLE key01_calendar_date (
            "Date" DATE,
            "Weekday" TEXT,
            "WeekEnd" BOOLEAN,
            "PublicHoliday" BOOLEAN,
            "StartOfMonth" BOOLEAN,
            "EndOfMonth" BOOLEAN,
            "Is_Range_for_Invoicing" BOOLEAN
        )
        """,
        """
        CREATE TABLE excel01_staff_workable_days (
            "Day of Week" TEXT,
            "StaffName" TEXT,
            "Working Day" BOOLEAN
        )
        """,
        """
        CREATE TABLE "1_Job_Task_Details_Table_base_1" (
            "Job_Task_Staff_ID" TEXT,
            "Job_ID" TEXT,
            "Task_UUID" TEXT,
            "Task_Name" TEXT,
            "Task_Type" TEXT,
            "Task_Type1" TEXT,
            "Task_Category" TEXT,
            "Staff_UUID" TEXT,
            "Staff_Name" TEXT,
            "Client_Name" TEXT,
            "Job_Name" TEXT,
            "StartDateAdjusted" DATE,
            "DueDateAdjusted" DATE,
            "Task_Allocated_Mins" INTEGER
        )
        """,
        """
        CREATE TABLE "4_Timesheet_Table" (
            "Job_Task_Staff_ID" TEXT,
            "Date" DATE,
            "Recorded_Minutes" INTEGER
        )
        """,
    ]
    with conn.cursor() as cur:
        for ddl in ddls:
            cur.execute(ddl)


def insert_df(conn, table: str, df: pd.DataFrame) -> None:
    """Bulk-insert a DataFrame into a Postgres table via execute_values."""
    cols = list(df.columns)
    quoted_cols = ", ".join(f'"{c}"' for c in cols)
    values = list(df.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        pg_extras.execute_values(
            cur,
            f'INSERT INTO "{table}" ({quoted_cols}) VALUES %s',
            values,
        )


# ---------------------------------------------------------------------------
# Run the SQL transformation chain
# ---------------------------------------------------------------------------
TODAY_LITERAL = f"DATE '{TODAY.isoformat()}'"


def run_pipeline(conn) -> None:
    """Execute the chain of CREATE TABLE AS SELECT statements (mirrors 013/014 MVs)."""
    statements: list[str] = []

    # 2_Staff_Task_Allocation_byDay_base_1 — mirrors 014:36-164
    statements.append(f"""
    CREATE TABLE "2_Staff_Task_Allocation_byDay_base_1" AS
    WITH base AS (
        SELECT c."Date", c."PublicHoliday", c."Weekday", c."WeekEnd",
               c."StartOfMonth", c."EndOfMonth", c."Is_Range_for_Invoicing",
               k."Job_Task_Staff_ID", k."Job_ID", k."Staff_Name",
               k."StartDateAdjusted", k."DueDateAdjusted",
               k."Task_Name", k."Client_Name", k."Job_Name",
               k."Task_Category", k."Task_Type1", k."Task_Type"
        FROM key01_calendar_date c
        CROSS JOIN "1_Job_Task_Details_Table_base_1" k
    )
    SELECT b.*,
        CASE WHEN b."Client_Name" = 'Dinniss Admin' OR b."Task_Type1" ILIKE '%Admin - Non-billable%'
             THEN FALSE ELSE TRUE END AS "Is_Client",
        CASE WHEN (b."Client_Name" = 'Dinniss Admin' OR b."Task_Type1" ILIKE '%Admin - Non-billable%')
                  OR b."Task_Type1" ILIKE '%Coaching%'
             THEN FALSE ELSE TRUE END AS "Is_Billable",
        (NOT b."WeekEnd" AND NOT b."PublicHoliday") AS "Is_Workable_Day",
        (b."Date" >= b."StartDateAdjusted" AND b."Date" <= b."DueDateAdjusted")
            AS "Is_Date_Between_Task_Days",
        COALESCE(w."Working Day", FALSE) AS "Is_Staff_Workable_DayOfWeek",
        CASE
            WHEN b."Task_Name" ILIKE '%Holiday%' OR b."Task_Name" ILIKE '%Sick Leave%' THEN TRUE
            WHEN b."Task_Name" ILIKE '%Other leave%' THEN TRUE
            ELSE FALSE
        END AS "Is_Task_a_Leave"
    FROM base b
    LEFT JOIN excel01_staff_workable_days w
      ON w."Day of Week" = b."Weekday" AND w."StaffName" = b."Staff_Name"
    """)

    # SUPPORT_Job_Leave_Task_Details_Table — mirrors 014:184-258
    statements.append("""
    CREATE TABLE support_job_leave_task_details AS
    SELECT
        b."Job_Task_Staff_ID",
        b."Staff_Name",
        b."Task_Allocated_Mins",
        b."StartDateAdjusted",
        b."DueDateAdjusted",
        (
            SELECT COUNT(*) FROM "2_Staff_Task_Allocation_byDay_base_1" d
            WHERE d."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
              AND d."Is_Date_Between_Task_Days" AND d."Is_Workable_Day" AND d."Is_Staff_Workable_DayOfWeek"
        ) AS "Workable_Days_Between_Task",
        CASE WHEN (SELECT COUNT(*) FROM "2_Staff_Task_Allocation_byDay_base_1" d
                   WHERE d."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
                     AND d."Is_Date_Between_Task_Days" AND d."Is_Workable_Day" AND d."Is_Staff_Workable_DayOfWeek") > 0
             THEN b."Task_Allocated_Mins"::float8
                  / (SELECT COUNT(*) FROM "2_Staff_Task_Allocation_byDay_base_1" d
                     WHERE d."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
                       AND d."Is_Date_Between_Task_Days" AND d."Is_Workable_Day" AND d."Is_Staff_Workable_DayOfWeek") / 60.0
             ELSE NULL END AS "Avg_Daily_Hours"
    FROM "1_Job_Task_Details_Table_base_1" b
    WHERE b."Task_Name" ILIKE '%Holiday%' OR b."Task_Name" ILIKE '%Sick Leave%' OR b."Task_Name" ILIKE '%Other leave%'
    """)

    # SUPPORT_Staff_Leave_Allocation_byDay — mirrors 014:277-331
    statements.append("""
    CREATE TABLE support_staff_leave_allo AS
    SELECT b."Staff_Name", b."Date",
           CASE WHEN b."Is_Staff_Workable_DayOfWeek" AND b."Is_Workable_Day" AND b."Is_Date_Between_Task_Days"
                THEN (SELECT s."Avg_Daily_Hours" FROM support_job_leave_task_details s
                      WHERE s."Job_Task_Staff_ID" = b."Job_Task_Staff_ID" LIMIT 1)
                ELSE NULL END AS "Allo_Leave_Hrs_perWorkday",
           CASE WHEN b."Is_Staff_Workable_DayOfWeek" AND b."Is_Workable_Day" AND b."Is_Date_Between_Task_Days"
                     AND (SELECT s."Avg_Daily_Hours" FROM support_job_leave_task_details s
                          WHERE s."Job_Task_Staff_ID" = b."Job_Task_Staff_ID" LIMIT 1) = 8
                THEN 1 ELSE NULL END AS "Full_Leave_Days"
    FROM "2_Staff_Task_Allocation_byDay_base_1" b
    WHERE b."Is_Task_a_Leave"
    """)

    # 2_Staff_Task_Allocation_byDay_base_2 — mirrors 014:356-428
    statements.append(f"""
    CREATE TABLE "2_Staff_Task_Allocation_byDay_base_2" AS
    WITH leave_agg AS (
        SELECT "Staff_Name", "Date",
               COALESCE(BOOL_OR("Allo_Leave_Hrs_perWorkday" IS NOT NULL), FALSE) AS "Is_Day_With_a_Leave",
               COALESCE(BOOL_OR("Full_Leave_Days" IS NOT NULL), FALSE) AS "Is_Full_Day_Leave"
        FROM support_staff_leave_allo
        GROUP BY "Staff_Name", "Date"
    ),
    ts_task AS (
        SELECT "Job_Task_Staff_ID", "Date", SUM("Recorded_Minutes")/60.0 AS "Recorded_Task_Hours"
        FROM "4_Timesheet_Table" GROUP BY "Job_Task_Staff_ID", "Date"
    ),
    wdb AS (
        SELECT b1."Job_Task_Staff_ID", COUNT(*) AS wdb_cnt, jt."Task_Allocated_Mins"
        FROM "2_Staff_Task_Allocation_byDay_base_1" b1
        LEFT JOIN leave_agg lv1 ON lv1."Staff_Name"=b1."Staff_Name" AND lv1."Date"=b1."Date"
        JOIN "1_Job_Task_Details_Table_base_1" jt ON jt."Job_Task_Staff_ID"=b1."Job_Task_Staff_ID"
        WHERE b1."Is_Date_Between_Task_Days" AND b1."Is_Workable_Day" AND b1."Is_Staff_Workable_DayOfWeek"
          AND COALESCE(lv1."Is_Full_Day_Leave", FALSE) = FALSE
        GROUP BY b1."Job_Task_Staff_ID", jt."Task_Allocated_Mins"
    )
    SELECT b.*,
        COALESCE(lv."Is_Day_With_a_Leave", FALSE) AS "Is_Day_With_a_Leave",
        COALESCE(lv."Is_Full_Day_Leave", FALSE)   AS "Is_Full_Day_Leave",
        CASE WHEN b."Is_Billable" THEN 'Billable' ELSE 'Not Billable' END AS "Billable_Selector",
        FALSE AS "Is_Final_Invoice_Raised",
        COALESCE(ts."Recorded_Task_Hours", 0) AS "Recorded_Task_Hours",
        (b."Task_Name" = 'Admin - Non-billable' AND b."Date" >= DATE '2021-02-01') AS "Admin_Task_To_Be_Removed",
        CASE
            WHEN b."Is_Workable_Day" AND b."Is_Date_Between_Task_Days"
                 AND b."Is_Staff_Workable_DayOfWeek"
                 AND COALESCE(lv."Is_Full_Day_Leave", FALSE) = FALSE
                 AND NOT (b."Task_Name" = 'Admin - Non-billable' AND b."Date" >= DATE '2021-02-01')
            THEN w."Task_Allocated_Mins"::float8 / NULLIF(w.wdb_cnt, 0) / 60.0
        END AS "Initial_Allo_Hrs_perWorkDay_KPI01"
    FROM "2_Staff_Task_Allocation_byDay_base_1" b
    LEFT JOIN leave_agg lv ON lv."Staff_Name"=b."Staff_Name" AND lv."Date"=b."Date"
    LEFT JOIN ts_task ts ON ts."Job_Task_Staff_ID"=b."Job_Task_Staff_ID" AND ts."Date"=b."Date"
    LEFT JOIN wdb w ON w."Job_Task_Staff_ID"=b."Job_Task_Staff_ID"
    """)

    # 1_Job_Task_Details_Table_base_2 — mirrors 014:452-519
    statements.append("""
    CREATE TABLE "1_Job_Task_Details_Table_base_2" AS
    WITH wdb AS (
        SELECT "Job_Task_Staff_ID", COUNT(*) AS "Workable_Days_Between_Task"
        FROM "2_Staff_Task_Allocation_byDay_base_2"
        WHERE "Is_Date_Between_Task_Days" AND "Is_Workable_Day" AND "Is_Staff_Workable_DayOfWeek"
          AND NOT "Is_Full_Day_Leave"
        GROUP BY "Job_Task_Staff_ID"
    ),
    tlh AS (
        SELECT b1."Job_Task_Staff_ID",
               SUM(d."Initial_Allo_Hrs_perWorkDay_KPI01") AS "Total_Leave_Hrs"
        FROM "1_Job_Task_Details_Table_base_1" b1
        JOIN "2_Staff_Task_Allocation_byDay_base_2" d
          ON d."Staff_Name" = b1."Staff_Name"
         AND d."Date" >= b1."StartDateAdjusted" AND d."Date" <= b1."DueDateAdjusted"
         AND d."Task_Category" = 'Leave Tasks' AND NOT d."Is_Full_Day_Leave"
        GROUP BY b1."Job_Task_Staff_ID"
    )
    SELECT b.*,
        CASE
            WHEN b."Task_Name" ILIKE '%Holiday%' OR b."Task_Name" ILIKE '%Sick Leave%' OR b."Task_Name" ILIKE '%Other leave%' THEN TRUE
            ELSE FALSE END AS "Is_Task_a_Leave",
        COALESCE(w."Workable_Days_Between_Task", 0) AS "Workable_Days_Between_Task",
        COALESCE(w."Workable_Days_Between_Task", 0) * 8 AS "Workable_Hrs_Between_Task",
        b."Task_Allocated_Mins"::float8 / NULLIF(COALESCE(w."Workable_Days_Between_Task",0), 0) AS "Initial_Avg_Mins_perWorkDay",
        CASE WHEN NOT (b."Task_Name" ILIKE '%Holiday%' OR b."Task_Name" ILIKE '%Sick Leave%' OR b."Task_Name" ILIKE '%Other leave%')
             THEN COALESCE(t."Total_Leave_Hrs", 0) END AS "Total_Leave_Hrs_between_Workable_Days",
        CASE WHEN NOT (b."Task_Name" ILIKE '%Holiday%' OR b."Task_Name" ILIKE '%Sick Leave%' OR b."Task_Name" ILIKE '%Other leave%')
             THEN GREATEST(0::float8, (COALESCE(w."Workable_Days_Between_Task",0)*8 - COALESCE(t."Total_Leave_Hrs",0))/8.0) END
            AS "Rev_Workable_Days_Between_Task",
        CASE WHEN NOT (b."Task_Name" ILIKE '%Holiday%' OR b."Task_Name" ILIKE '%Sick Leave%' OR b."Task_Name" ILIKE '%Other leave%')
             THEN b."Task_Allocated_Mins"::float8
                  / NULLIF(GREATEST(0::float8, (COALESCE(w."Workable_Days_Between_Task",0)*8 - COALESCE(t."Total_Leave_Hrs",0))/8.0), 0)
             END AS "Avg_Mins_perWorkDay_WITHOUT_Leave"
    FROM "1_Job_Task_Details_Table_base_1" b
    LEFT JOIN wdb w ON w."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    LEFT JOIN tlh t ON t."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    """)

    # 2_Staff_Task_Allocation_byDay_base_3 — mirrors 014:540-572
    statements.append(f"""
    CREATE TABLE "2_Staff_Task_Allocation_byDay_base_3" AS
    WITH jt AS (
        SELECT "Job_Task_Staff_ID", "Avg_Mins_perWorkDay_WITHOUT_Leave"
        FROM "1_Job_Task_Details_Table_base_2"
    )
    SELECT b.*,
        CASE WHEN b."Is_Workable_Day" AND b."Is_Date_Between_Task_Days"
                  AND b."Is_Staff_Workable_DayOfWeek" AND NOT b."Is_Day_With_a_Leave"
                  AND NOT b."Is_Task_a_Leave" AND NOT b."Is_Full_Day_Leave"
                  AND NOT b."Admin_Task_To_Be_Removed"
             THEN jt."Avg_Mins_perWorkDay_WITHOUT_Leave" / 60.0 END AS "Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02",
        (b."Date" < {TODAY_LITERAL} AND b."Is_Date_Between_Task_Days") AS "Is_Date_between_Start_Today"
    FROM "2_Staff_Task_Allocation_byDay_base_2" b
    LEFT JOIN jt ON jt."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    """)

    # 1_Job_Task_Details_Table_base_3 — mirrors 014:595-711
    statements.append(f"""
    CREATE TABLE "1_Job_Task_Details_Table_base_3" AS
    WITH ttm AS (
        SELECT "Job_Task_Staff_ID", SUM("Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02")*60.0 AS "Total_Task_Mins"
        FROM "2_Staff_Task_Allocation_byDay_base_3" GROUP BY "Job_Task_Staff_ID"
    ),
    wdl AS (
        SELECT "Job_Task_Staff_ID", COUNT(*) AS "WorkDays_WITH_Leaves"
        FROM "2_Staff_Task_Allocation_byDay_base_3"
        WHERE "Is_Date_Between_Task_Days" AND "Is_Workable_Day" AND "Is_Day_With_a_Leave"
          AND "Is_Staff_Workable_DayOfWeek" AND NOT "Is_Full_Day_Leave"
        GROUP BY "Job_Task_Staff_ID"
    ),
    tmt AS (
        SELECT "Job_Task_Staff_ID", SUM("Recorded_Minutes") AS recorded_mins
        FROM "4_Timesheet_Table" GROUP BY "Job_Task_Staff_ID"
    )
    SELECT b.*,
        CASE WHEN NOT b."Is_Task_a_Leave" THEN COALESCE(t."Total_Task_Mins", 0) END AS "Total_Task_Mins_WorkDays_WITHOUT_Leave",
        CASE WHEN NOT b."Is_Task_a_Leave" THEN b."Task_Allocated_Mins" - COALESCE(t."Total_Task_Mins", 0) END AS "Remaining_Allocated_Task_Mins",
        CASE WHEN NOT b."Is_Task_a_Leave" THEN COALESCE(w."WorkDays_WITH_Leaves", 0) END AS "WorkDays_WITH_Leaves_between_Task",
        CASE WHEN NOT b."Is_Task_a_Leave"
             THEN (b."Task_Allocated_Mins" - COALESCE(t."Total_Task_Mins", 0))
                  / NULLIF(COALESCE(w."WorkDays_WITH_Leaves", 0), 0)::float8 END AS "Avg_Mins_perWorkDay_WITH_Leaves",
        CASE WHEN NOT b."Is_Task_a_Leave" THEN COALESCE(tm.recorded_mins, 0) END AS "Task_Mins_Worked_Till_Date",
        CASE WHEN NOT b."Is_Task_a_Leave"
             THEN GREATEST(0, b."Task_Allocated_Mins" - COALESCE(tm.recorded_mins, 0)) END AS "Task_Mins_Remain_until_Due",
        CASE WHEN NOT b."Is_Task_a_Leave"
             THEN CASE WHEN COALESCE(tm.recorded_mins, 0) > b."Task_Allocated_Mins"
                       OR b."DueDateAdjusted" < {TODAY_LITERAL}
                       THEN b."Task_Allocated_Mins"
                       ELSE COALESCE(tm.recorded_mins, 0) END END AS "Task_Mins_Worked_Adjusted"
    FROM "1_Job_Task_Details_Table_base_2" b
    LEFT JOIN ttm t ON t."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    LEFT JOIN wdl w ON w."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    LEFT JOIN tmt tm ON tm."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    """)

    # 2_Staff_Task_Allocation_byDay_base_4 — mirrors 014:736-810
    statements.append(f"""
    CREATE TABLE "2_Staff_Task_Allocation_byDay_base_4" AS
    WITH jt AS (
        SELECT "Job_Task_Staff_ID", "Avg_Mins_perWorkDay_WITH_Leaves"
        FROM "1_Job_Task_Details_Table_base_3"
    ),
    base AS (
        SELECT b.*,
            CASE WHEN b."Is_Workable_Day" AND b."Is_Date_Between_Task_Days"
                      AND b."Is_Staff_Workable_DayOfWeek" AND b."Is_Day_With_a_Leave"
                      AND NOT b."Is_Task_a_Leave" AND NOT b."Is_Full_Day_Leave"
                      AND NOT b."Admin_Task_To_Be_Removed"
                 THEN jt."Avg_Mins_perWorkDay_WITH_Leaves" / 60.0 END AS "Allo_Hrs_perWorkday_WITH_Leave_KPI03",
            (b."Date" >= {TODAY_LITERAL} AND b."Is_Date_Between_Task_Days") AS "Is_Date_between_Today_Due",
            CASE WHEN b."Date" >= {TODAY_LITERAL} AND b."Is_Date_Between_Task_Days"
                      AND b."Task_Category" = 'Billable Tasks'
                      AND b."Is_Workable_Day" AND b."Is_Staff_Workable_DayOfWeek"
                      AND b."Is_Day_With_a_Leave" AND NOT b."Is_Task_a_Leave" AND NOT b."Is_Full_Day_Leave"
                 THEN jt."Avg_Mins_perWorkDay_WITH_Leaves" / 60.0 END AS "Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04"
        FROM "2_Staff_Task_Allocation_byDay_base_3" b
        LEFT JOIN jt ON jt."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    )
    SELECT base.*,
        COALESCE(base."Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02", 0)
        + COALESCE(base."Allo_Hrs_perWorkday_WITH_Leave_KPI03", 0) AS "Allo_Hrs_perWorkDay_AdjLeaves_FIN01"
    FROM base
    """)

    # 1_Job_Task_Details_Table — mirrors 014:837-989
    statements.append(f"""
    CREATE TABLE "1_Job_Task_Details_Table" AS
    WITH arm AS (
        SELECT "Job_Task_Staff_ID", SUM("Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04") * 60.0 AS allo_mins_remaining
        FROM "2_Staff_Task_Allocation_byDay_base_4" GROUP BY "Job_Task_Staff_ID"
    ),
    rwdwol AS (
        SELECT "Job_Task_Staff_ID", COUNT(*) AS cnt
        FROM "2_Staff_Task_Allocation_byDay_base_4"
        WHERE NOT "Is_Day_With_a_Leave" AND "Is_Date_Between_Task_Days" AND "Is_Workable_Day"
          AND "Is_Staff_Workable_DayOfWeek" AND NOT "Is_Full_Day_Leave"
          AND "Date" >= {TODAY_LITERAL} AND "Date" <= "DueDateAdjusted"
        GROUP BY "Job_Task_Staff_ID"
    )
    SELECT b.*,
        CASE WHEN NOT b."Is_Task_a_Leave" THEN COALESCE(a.allo_mins_remaining, 0) END AS "Allo_Mins_during_Remaining_workDays_WITH_leave",
        CASE WHEN NOT b."Is_Task_a_Leave" THEN COALESCE(r.cnt, 0) END AS "Remain_WorkDays_WITHOUT_Leave",
        CASE WHEN NOT b."Is_Task_a_Leave" THEN b."Task_Mins_Remain_until_Due" - COALESCE(a.allo_mins_remaining, 0) END AS "Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave",
        CASE WHEN NOT b."Is_Task_a_Leave"
             THEN (b."Task_Mins_Remain_until_Due" - COALESCE(a.allo_mins_remaining, 0))
                  / NULLIF(COALESCE(r.cnt, 0), 0)::float8 END AS "Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave",
        CASE WHEN NOT b."Is_Task_a_Leave" THEN
            CASE
                WHEN (b."Task_Mins_Remain_until_Due" - COALESCE(a.allo_mins_remaining, 0)) > 0 THEN
                    CASE WHEN COALESCE(r.cnt, 0) >= 1
                          AND ((b."Task_Mins_Remain_until_Due" - COALESCE(a.allo_mins_remaining, 0))
                               / NULLIF(COALESCE(r.cnt, 0), 0)::float8) <= 480 THEN TRUE ELSE FALSE END
                ELSE TRUE
            END
        END AS "Is_Task_WITHIN_Allo_Time_IMP"
    FROM "1_Job_Task_Details_Table_base_3" b
    LEFT JOIN arm a ON a."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    LEFT JOIN rwdwol r ON r."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    """)

    # 2_Staff_Task_Allocation_byDay — mirrors 014:1015-1121
    statements.append(f"""
    CREATE TABLE "2_Staff_Task_Allocation_byDay" AS
    WITH jt AS (
        SELECT "Job_Task_Staff_ID", "Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave"
        FROM "1_Job_Task_Details_Table"
    )
    SELECT b.*,
        CASE WHEN b."Is_Date_between_Today_Due" AND b."Task_Category" = 'Billable Tasks'
                  AND b."Is_Workable_Day" AND b."Is_Date_Between_Task_Days"
                  AND b."Is_Staff_Workable_DayOfWeek" AND NOT b."Is_Day_With_a_Leave"
                  AND NOT b."Is_Task_a_Leave" AND NOT b."Is_Full_Day_Leave"
             THEN jt."Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave" / 60.0 END
            AS "Allo_Hrs_perRemainingWorkDay_WITHOUT_LEAVE_KPI05"
    FROM "2_Staff_Task_Allocation_byDay_base_4" b
    LEFT JOIN jt ON jt."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
    """)

    with conn.cursor() as cur:
        for sql in statements:
            cur.execute(sql)


# ---------------------------------------------------------------------------
# Result collection
# ---------------------------------------------------------------------------
def fmt(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return f"{v:.6g}"
    return str(v)


def fetchone(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()


def fetchdf(conn, sql: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def collect_results(conn) -> tuple[list[dict], pd.DataFrame]:
    """Probe the SQL tables and return (comparison rows, workable-day breakdown)."""
    rows: list[dict] = []

    def add(step, excel_label, expected, actual, note="", actual_display=None):
        try:
            if expected is None or actual is None:
                dev = "—"
            elif isinstance(expected, bool) or isinstance(actual, bool):
                dev = "match" if bool(expected) == bool(actual) else "MISMATCH"
            else:
                d = float(actual) - float(expected)
                dev = f"{d:+.6g}" if abs(d) > 1e-9 else "0"
        except Exception:
            dev = "?"
        rows.append({
            "Step": step,
            "Excel cell": excel_label,
            "Expected (Excel)": fmt(expected),
            "Actual (SQL)": actual_display if actual_display is not None else fmt(actual),
            "Deviation": dev,
            "Note": note,
        })

    wt = f"'{WORK_TASK_ID}'"
    lt = f"'{LEAVE_TASK_ID}'"
    probe = TODAY.isoformat()

    # Step 01
    s01 = fetchone(conn, f"""
        SELECT "Is_Workable_Day", "PublicHoliday", "WeekEnd", "Is_Staff_Workable_DayOfWeek"
        FROM "2_Staff_Task_Allocation_byDay_base_1"
        WHERE "Job_Task_Staff_ID"={wt} AND "Date"=DATE '{probe}'
    """)
    actual_s01 = bool(s01[0]) and not bool(s01[1]) and not bool(s01[2]) and bool(s01[3])
    add("01", "C35 (probe@TODAY)", True, actual_s01,
        "Workable for Carl on 2021-01-11 (Mon). Excel: TODAY flag = WORKDAY.")

    # Step 02
    s02 = fetchone(conn, f"""
        SELECT "Is_Date_Between_Task_Days", "Is_Day_With_a_Leave"
        FROM "2_Staff_Task_Allocation_byDay_base_2"
        WHERE "Job_Task_Staff_ID"={wt} AND "Date"=DATE '{probe}'
    """)
    add("02", "E35 (probe@TODAY)", 1, int(bool(s02[0]) and bool(s02[1])),
        "Leave flag on 2021-01-11 (date in both task range and leave range).")

    # Steps 03, 04, 05, 07, 08, 09, 10
    jt = fetchone(conn, f"""
        SELECT "Workable_Days_Between_Task",
               "Workable_Hrs_Between_Task",
               "Initial_Avg_Mins_perWorkDay"/60.0,
               "Total_Leave_Hrs_between_Workable_Days",
               "Rev_Workable_Days_Between_Task",
               "Avg_Mins_perWorkDay_WITHOUT_Leave"/60.0
        FROM "1_Job_Task_Details_Table_base_2" WHERE "Job_Task_Staff_ID"={wt}
    """)
    add("03", "C6", 40, jt[0], "Workable_Days_Between_Task")
    add("04", "D6", 320, jt[1], "Workable_Hrs_Between_Task")
    add("05", "E6", 1.625, jt[2], "Initial_Avg_Mins_perWorkDay / 60")

    # Step 06
    s06 = fetchone(conn, f"""
        SELECT "Initial_Allo_Hrs_perWorkDay_KPI01"
        FROM "2_Staff_Task_Allocation_byDay_base_2"
        WHERE "Job_Task_Staff_ID"={lt} AND "Date"=DATE '{probe}'
    """)
    add("06", "F35 (probe@TODAY)", 0.625, s06[0],
        "Per-day leave-task KPI01 broadcast = leave initial avg / day.")

    add("07", "F6", 25, jt[3], "Total_Leave_Hrs_between_Workable_Days")
    add("08", "G6", 295, float(jt[1]) - float(jt[3]),
        "Derived: Step 04 − Step 07 (not stored as a column).")
    add("09", "H6", 36.875, jt[4], "Rev_Workable_Days_Between_Task")
    add("10", "I6", 1.7627118644067796, jt[5], "Avg_Mins_perWorkDay_WITHOUT_Leave / 60")

    # Step 11
    s11 = fetchone(conn, f"""
        SELECT COALESCE(SUM("Allo_Hrs_perWorkday_WITHOUT_Leave_KPI02"), 0)
        FROM "2_Staff_Task_Allocation_byDay_base_3"
        WHERE "Job_Task_Staff_ID"={wt}
    """)[0]
    add("11", "G column (sum)", 0, float(s11),
        "SUM(KPI02). 0 here because leave covers every workable day.")

    # Step 12
    s12 = fetchone(conn, f"""
        SELECT "Total_Task_Mins_WorkDays_WITHOUT_Leave"/60.0
        FROM "1_Job_Task_Details_Table_base_3" WHERE "Job_Task_Staff_ID"={wt}
    """)[0]
    add("12", "J6", 0, s12, "Total_Task_Mins_WorkDays_WITHOUT_Leave / 60")

    # Steps 13, 14, 15
    r2 = fetchone(conn, f"""
        SELECT "Remaining_Allocated_Task_Mins"/60.0,
               "WorkDays_WITH_Leaves_between_Task",
               "Avg_Mins_perWorkDay_WITH_Leaves"/60.0
        FROM "1_Job_Task_Details_Table_base_3" WHERE "Job_Task_Staff_ID"={wt}
    """)
    add("13", "K6", 65, r2[0], "Remaining_Allocated_Task_Mins / 60")
    add("14", "L6", 40, r2[1], "WorkDays_WITH_Leaves_between_Task")
    add("15", "M6", 1.625, r2[2], "Avg_Mins_perWorkDay_WITH_Leaves / 60")

    # Step 16
    s16 = fetchone(conn, f"""
        SELECT "Allo_Hrs_perWorkday_WITH_Leave_KPI03"
        FROM "2_Staff_Task_Allocation_byDay_base_4"
        WHERE "Job_Task_Staff_ID"={wt} AND "Date"=DATE '{probe}'
    """)[0]
    add("16", "H35 (probe@TODAY)", 1.625, s16, "KPI03 per leave day.")

    # Step 17
    s17 = fetchone(conn, f"""
        SELECT "Allo_Hrs_perWorkDay_AdjLeaves_FIN01"
        FROM "2_Staff_Task_Allocation_byDay_base_4"
        WHERE "Job_Task_Staff_ID"={wt} AND "Date"=DATE '{probe}'
    """)[0]
    add("17", "I35 (probe@TODAY)", 1.625, s17,
        "FIN01 = KPI02 + KPI03 (KPI02 = 0 in this scenario).")

    # Steps 18, 19
    r3 = fetchone(conn, f"""
        SELECT "Task_Mins_Worked_Till_Date"/60.0, "Task_Mins_Remain_until_Due"/60.0
        FROM "1_Job_Task_Details_Table_base_3" WHERE "Job_Task_Staff_ID"={wt}
    """)
    add("18", "N6", 1, r3[0], "Task_Mins_Worked_Till_Date / 60")
    add("19", "O6", 64, r3[1], "Task_Mins_Remain_until_Due / 60")

    # Step 20
    s20 = fetchone(conn, f"""
        SELECT "Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04"
        FROM "2_Staff_Task_Allocation_byDay_base_4"
        WHERE "Job_Task_Staff_ID"={wt} AND "Date"=DATE '{probe}'
    """)[0]
    add("20", "J35 (probe@TODAY)", 1.625, s20, "KPI04 per leave day from TODAY → Due.")

    # Steps 21, 22, 23, 24, 25
    r4 = fetchone(conn, f"""
        SELECT "Allo_Mins_during_Remaining_workDays_WITH_leave"/60.0,
               "Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave"/60.0,
               "Remain_WorkDays_WITHOUT_Leave",
               "Avg_Remain_Mins_perRemainWorkday_WITHOUT_Leave"/60.0,
               "Is_Task_WITHIN_Allo_Time_IMP"
        FROM "1_Job_Task_Details_Table" WHERE "Job_Task_Staff_ID"={wt}
    """)
    add("21", "P6", 55.25, r4[0],
        "Allo_Mins_during_Remaining_workDays_WITH_leave / 60 (34 × 1.625).")
    add("22", "Q6", 8.75, r4[1], "Remain_Mins_Allo_to_Remain_WorkDays_WITHOUT_Leave / 60")
    add("23", "R6", 0, r4[2], "Remain_WorkDays_WITHOUT_Leave")
    s24_value = r4[3] if r4[3] is not None else 0
    add("24", "S6", 0, s24_value,
        "Excel IFERROR(…,0)=0; DB NULLIF→NULL. Same downstream effect.",
        actual_display="0 (NULL)" if r4[3] is None else fmt(r4[3]))
    add("25", "T6", False, bool(r4[4]),
        "Both flag 'cannot complete' (Step 23=0). DB Is_Task_WITHIN_Allo_Time_IMP=FALSE; "
        "Excel rule 'Step24>8 OR NULL' raises the flag.")

    # Step 26
    s26 = fetchone(conn, f"""
        SELECT COALESCE(b."Allo_Hrs_perRemainingWorkDay_WITH_LEAVE_KPI04", 0)
             + COALESCE(s."Allo_Hrs_perRemainingWorkDay_WITHOUT_LEAVE_KPI05", 0)
        FROM "2_Staff_Task_Allocation_byDay_base_4" b
        JOIN "2_Staff_Task_Allocation_byDay" s
          ON s."Job_Task_Staff_ID" = b."Job_Task_Staff_ID" AND s."Date" = b."Date"
        WHERE b."Job_Task_Staff_ID"={wt} AND b."Date"=DATE '{probe}'
    """)[0]
    add("26", "L35 (probe@TODAY)", 1.625, s26,
        "FIN02 = KPI04 + KPI05 (KPI05 = 0 because no remaining days WITHOUT leave).")

    breakdown = fetchdf(conn, f"""
        SELECT 'Workable days in task range (Carl, 2020-12-23 → 2021-03-31)' AS "Bucket",
               COUNT(*) AS "Count"
        FROM "2_Staff_Task_Allocation_byDay_base_1"
        WHERE "Job_Task_Staff_ID"={wt} AND "Is_Date_Between_Task_Days"
          AND "Is_Workable_Day" AND "Is_Staff_Workable_DayOfWeek"
        UNION ALL
        SELECT 'Workable days also flagged ''with leave''',
               COUNT(*) FROM "2_Staff_Task_Allocation_byDay_base_2"
        WHERE "Job_Task_Staff_ID"={wt} AND "Is_Date_Between_Task_Days"
          AND "Is_Workable_Day" AND "Is_Staff_Workable_DayOfWeek" AND "Is_Day_With_a_Leave"
        UNION ALL
        SELECT 'Leave-flagged workable days from TODAY (2021-01-11) onward',
               COUNT(*) FROM "2_Staff_Task_Allocation_byDay_base_4"
        WHERE "Job_Task_Staff_ID"={wt} AND "Is_Date_Between_Task_Days"
          AND "Is_Workable_Day" AND "Is_Staff_Workable_DayOfWeek" AND "Is_Day_With_a_Leave"
          AND "Date" >= DATE '{probe}'
    """)

    return rows, breakdown


# ---------------------------------------------------------------------------
# PDF rendering (same layout as DuckDB validator, with Postgres-specific labels)
# ---------------------------------------------------------------------------
ss = getSampleStyleSheet()
TITLE = ParagraphStyle("Title", parent=ss["Title"], fontSize=18, leading=22, spaceAfter=6)
SUBTITLE = ParagraphStyle("Subtitle", parent=ss["Heading2"], fontSize=10, leading=12,
                          textColor=colors.HexColor("#666"), spaceAfter=18)
H1 = ParagraphStyle("H1", parent=ss["Heading1"], fontSize=14, leading=18,
                    spaceBefore=14, spaceAfter=6)
H2 = ParagraphStyle("H2", parent=ss["Heading2"], fontSize=11, leading=14,
                    spaceBefore=10, spaceAfter=4)
BODY = ParagraphStyle("Body", parent=ss["BodyText"], fontSize=9.5, leading=13, spaceAfter=4)
SMALL = ParagraphStyle("Small", parent=ss["BodyText"], fontSize=8.5, leading=11)
TABLE_CELL = ParagraphStyle("TableCell", parent=ss["BodyText"], fontSize=8, leading=10)
TABLE_HEAD = ParagraphStyle("TableHead", parent=ss["BodyText"], fontSize=8.5, leading=11,
                            textColor=colors.white, alignment=1)


def P(text, style=BODY) -> Paragraph:
    return Paragraph(text, style)


def cell(text, style=TABLE_CELL) -> Paragraph:
    return Paragraph(str(text), style)


def build_pdf(rows: list[dict], breakdown: pd.DataFrame, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    story = []

    story.append(P("Excel vs PostgreSQL MV Validation", TITLE))
    story.append(P(
        "Comparing each of the 26 steps from <i>Logic_of_Capacity_Planning.xlsx</i> "
        "against values produced by the materialized-view SQL "
        "(<font face='Courier'>013/014_create_materialized_views.sql</font>), "
        f"executed against PostgreSQL (schema <font face='Courier'>{SCHEMA}</font>) "
        "on the same scenario data.",
        SUBTITLE,
    ))

    story.append(P("Scenario inputs", H1))
    scenario = [
        ["Input", "Value"],
        ["Work Task allocated", f"{WORK_TASK_ALLOC_HRS} hrs / {WORK_TASK_ALLOC_HRS * 60:,} mins"],
        ["Leave Task allocated", f"{LEAVE_TASK_ALLOC_HRS} hrs / {LEAVE_TASK_ALLOC_HRS * 60:,} mins"],
        ["Task date range", f"{TASK_START} → {TASK_DUE}"],
        ["Leave date range", f"{LEAVE_START} → {LEAVE_DUE} (per E114=1; A8 text says 03-30)"],
        ["TODAY", str(TODAY)],
        ["Public holidays", ", ".join(str(d) for d in sorted(PUBLIC_HOLIDAYS))],
        [f"Staff '{STAFF}' workable DOW", ", ".join(sorted(CARL_WORKABLE_DOW))],
        ["Timesheet recorded", f"60 mins (= {TASK_WORKED_HRS} hr) on Work Task before TODAY"],
    ]
    tbl = Table([[cell(c) for c in row] for row in scenario],
                colWidths=[55 * mm, 110 * mm])
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#34495e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("ALIGN", (0, 0), (-1, 0), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#bdc3c7")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#f8f9fa")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(tbl)

    story.append(P("Step-by-step comparison", H1))
    header = ["Step", "Excel cell", "Expected (Excel)", "Actual (SQL)", "Deviation", "Note"]
    data = [[cell(h, TABLE_HEAD) for h in header]] + [
        [cell(r[k]) for k in header] for r in rows
    ]
    t = Table(data,
              colWidths=[10 * mm, 30 * mm, 22 * mm, 22 * mm, 17 * mm, 72 * mm],
              repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2c3e50")),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("ALIGN", (0, 1), (0, -1), "CENTER"),
        ("ALIGN", (2, 1), (4, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#bdc3c7")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#f8f9fa")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(t)

    story.append(Spacer(1, 8))
    story.append(P("Workable-day breakdown (cross-check)", H2))
    bd_data = [["Bucket", "Count"]] + [
        [str(b), str(c)] for b, c in zip(breakdown["Bucket"], breakdown["Count"])
    ]
    t2 = Table([[cell(c) for c in row] for row in bd_data],
               colWidths=[120 * mm, 45 * mm])
    t2.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#34495e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("ALIGN", (1, 1), (1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#bdc3c7")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#f8f9fa")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t2)
    story.append(P(
        "These counts drive every number above. The probe date "
        f"{TODAY} (Excel row 35, TODAY) gives identical per-day broadcast "
        "values in PostgreSQL as in Excel.",
        SMALL,
    ))

    story.append(P("Sources of the only two non-zero-but-explained differences", H1))
    story.append(KeepTogether([
        P("1. Step 24 — Excel <b>0</b> vs DB <b>NULL</b>.", H2),
        P(
            "Excel <font face='Courier'>IFERROR((Q6/R6), 0)</font> returns 0 "
            "when R6=0. DB <font face='Courier'>NULLIF(R6, 0)</font> → "
            "division by NULL → NULL (= DAX BLANK). Same downstream behaviour "
            "(Step 25 raises the flag in both); only the sentinel differs.",
            BODY,
        ),
    ]))
    story.append(Spacer(1, 4))
    story.append(KeepTogether([
        P("2. Step 25 — boolean sense.", H2),
        P(
            "Excel's flag reads as 'raise an alarm' (TRUE = problem). DB's "
            "<font face='Courier'>Is_Task_WITHIN_Allo_Time_IMP</font> reads "
            "as 'within allocated time' (TRUE = OK). The underlying judgment "
            "('can/cannot fit') is identical — both correctly conclude "
            "<b>cannot fit</b> in this scenario.",
            BODY,
        ),
    ]))

    numeric_devs = [r["Deviation"] for r in rows if r["Deviation"] not in ("match", "MISMATCH")]
    all_numeric_zero = all(d == "0" for d in numeric_devs)
    boolean_matches = all(r["Deviation"] != "MISMATCH" for r in rows)
    story.append(P("Verdict", H1))
    if all_numeric_zero and boolean_matches:
        story.append(P(
            "<b>All 26 steps reconcile.</b> Every numeric step has zero "
            "deviation; the two boolean flags (Steps 01 and 25) match in "
            "interpretation. The Excel logic is fully captured by the "
            "PostgreSQL materialized-view chain.",
            BODY,
        ))
    else:
        story.append(P(
            "<b>Discrepancies detected.</b> Review the Deviation column above "
            "for the failing rows.",
            BODY,
        ))
    story.append(P(
        f"Tables left in schema <font face='Courier'>{SCHEMA}</font> on the test "
        "database for inspection. "
        "Re-run with <font face='Courier'>python gold/validation/01-logic-vs-sql/validate_excel_vs_postgres.py</font> "
        "in the <font face='Courier'>py313</font> conda env "
        "(requires <font face='Courier'>POSTGRES_CONNECTION_TEST</font> in .env).",
        SMALL,
    ))

    doc = SimpleDocTemplate(
        str(out), pagesize=A4,
        topMargin=18 * mm, bottomMargin=18 * mm,
        leftMargin=18 * mm, rightMargin=18 * mm,
        title="Excel vs PostgreSQL MV Validation",
        author="dinniss-capacity-performance",
    )
    doc.build(story)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    dsn = load_connection_string()
    print(f"Connecting to PostgreSQL…")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        print(f"Resetting schema '{SCHEMA}' (DROP CASCADE + CREATE)…")
        reset_schema(conn)
        print("Creating source tables and inserting scenario data…")
        create_source_tables(conn)
        insert_df(conn, "key01_calendar_date", build_calendar_df())
        insert_df(conn, "excel01_staff_workable_days", build_staff_workable_days_df())
        insert_df(conn, "1_Job_Task_Details_Table_base_1", build_tasks_df())
        insert_df(conn, "4_Timesheet_Table", build_timesheet_df())
        print("Running SQL pipeline…")
        run_pipeline(conn)
        print("Collecting step-by-step results…")
        rows, breakdown = collect_results(conn)
    finally:
        # Keep tables on disk; just close the network connection.
        conn.close()

    df = pd.DataFrame(rows)
    print(df.to_markdown(index=False))
    print()
    print("# Workable day counts breakdown:")
    print(breakdown.to_markdown(index=False))

    out_path = make_output_path()
    build_pdf(rows, breakdown, out_path)
    print(f"\nWrote PDF: {out_path}  ({out_path.stat().st_size:,} bytes)")
    print(f"Tables left in schema '{SCHEMA}' for inspection.")


if __name__ == "__main__":
    main()
