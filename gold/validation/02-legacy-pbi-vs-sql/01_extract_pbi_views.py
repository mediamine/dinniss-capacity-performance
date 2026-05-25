#!/usr/bin/env python3
"""
Extract Power BI views from PostgreSQL to CSV files for validation.

Connects to PostgreSQL, verifies the connection (following the pattern in
01_test_postgres_connection.py), then streams each of the four pbi_* final
user-facing views to a CSV file via `COPY ... TO STDOUT WITH CSV HEADER`.

COPY streams server-side directly into the file — no per-row Python overhead
and no memory pressure even for the 65M-row pbi_2_Staff_Task_Allocation_byDay.

Output filenames match the view names with .csv suffix, written under
gold/validation/02-legacy-pbi-vs-sql/extracted/ by default (i.e. an
`extracted/` folder next to this script). Output directory and view list
are overridable via CLI flags.

Usage:
  # Extract all 4 views to the default extracted/ folder
  python gold/validation/02-legacy-pbi-vs-sql/01_extract_pbi_views.py

  # Custom output directory
  python gold/validation/02-legacy-pbi-vs-sql/01_extract_pbi_views.py --output-dir /tmp/pbi_dump

  # Extract just one view
  python gold/validation/02-legacy-pbi-vs-sql/01_extract_pbi_views.py --views pbi_4_Timesheet_Table
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import List

import psycopg2
from dotenv import load_dotenv

load_dotenv()


# Force UTF-8 on stdout/stderr so log lines with non-ASCII characters work on
# Windows consoles (default cp1252 raises UnicodeEncodeError).
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "replace")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "replace")


SCRIPT_DIR = Path(__file__).resolve().parent

(SCRIPT_DIR / "logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(SCRIPT_DIR / "logs" / "01_extract_pbi_views.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# The 4 final user-facing pbi_ views that Power BI imports. Case is preserved
# because Postgres folds unquoted identifiers to lowercase — we MUST quote
# these to find them.
PBI_VIEWS = [
    "pbi_1_Job_Task_Details_Table",
    "pbi_2_Staff_Task_Allocation_byDay",
    "pbi_3_Staff_Performance_Table",
    "pbi_4_Timesheet_Table",
]


def _test_postgres_connection(conn_str: str) -> None:
    """Same shape as gold/scripts/py/01_test_postgres_connection.py — verify
    the connection string and surface the server version before doing real
    work. Exits on failure."""
    logger.info("Starting PostgreSQL connection test.")
    logger.info("Attempting to connect to PostgreSQL with provided connection string.")
    try:
        with psycopg2.connect(conn_str) as conn:
            conn.autocommit = True
            logger.info("Successfully connected to PostgreSQL.")
            with conn.cursor() as cursor:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                logger.info("PostgreSQL version: %s", version)
    except psycopg2.Error as exc:
        logger.exception("Failed to connect to PostgreSQL: %s", exc)
        sys.exit(1)


def _extract_view_to_csv(conn, view_name: str, output_path: Path) -> int:
    """Stream a view's contents to a CSV file via COPY. Returns the row count."""
    logger.info(f"Extracting {view_name} -> {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Quote the view name to preserve mixed-case identifiers.
    copy_sql = f'COPY (SELECT * FROM "{view_name}") TO STDOUT WITH (FORMAT CSV, HEADER TRUE)'

    with conn.cursor() as cursor:
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            cursor.copy_expert(copy_sql, f)

        # Separate row count query for the log line. Negligible cost on top
        # of the COPY since the view is already materialized.
        cursor.execute(f'SELECT COUNT(*) FROM "{view_name}"')
        row_count = cursor.fetchone()[0]

    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"  -> {view_name}: {row_count:,} rows, {size_mb:.2f} MB")
    return row_count


def main():
    parser = argparse.ArgumentParser(
        prog="01_extract_pbi_views.py",
        description="Extract the four pbi_* final views to CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(SCRIPT_DIR / "extracted"),
        help="Directory for CSV output (default: extracted/ next to this script).",
    )
    parser.add_argument(
        "--views",
        nargs="+",
        default=PBI_VIEWS,
        help="Specific view names to extract (default: all 4 pbi_ user-facing views).",
    )
    args = parser.parse_args()

    conn_str = os.getenv("POSTGRES_CONNECTION")
    if not conn_str:
        logger.error("POSTGRES_CONNECTION environment variable is required.")
        sys.exit(1)

    # Test connection first so a misconfigured DB fails loud before we touch
    # the output directory or run any extraction logic.
    _test_postgres_connection(conn_str)

    output_dir = Path(args.output_dir)
    logger.info("=" * 60)
    logger.info(f"Output directory: {output_dir.resolve()}")
    logger.info(f"Views to extract: {args.views}")
    logger.info("=" * 60)

    try:
        with psycopg2.connect(conn_str) as conn:
            conn.autocommit = True
            for view_name in args.views:
                output_path = output_dir / f"{view_name}.csv"
                _extract_view_to_csv(conn, view_name, output_path)
    except psycopg2.Error as exc:
        logger.exception(f"Extraction failed: {exc}")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info(f"All extractions complete. Files in {output_dir.resolve()}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
