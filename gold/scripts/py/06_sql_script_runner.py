#!/usr/bin/env python3
"""
Post-Sync SQL Script Runner

Runs SQL scripts in dependency order after data sync completes.

Phases (run in this order):
  010  keys         Key reference tables
  011  invoice      Invoice support tables
  012  excel        Excel imports
  013  timesheet    Job task & timesheet details
  014  allocation   Staff task allocation by day
  015  performance  Staff performance table
  020  refresh      Refresh all materialized views (daily)
  030  powerbi      Power BI wrapper views

Usage examples:
  # Run everything from scratch
  python 06_sql_script_runner.py --all

  # Rebuild only the create phase (010-015)
  python 06_sql_script_runner.py --create

  # Daily refresh only
  python 06_sql_script_runner.py --refresh

  # Refresh then expose to Power BI
  python 06_sql_script_runner.py --refresh --powerbi

  # Rebuild a single phase
  python 06_sql_script_runner.py --performance

  # Preview what would run without executing
  python 06_sql_script_runner.py --all --dry-run
"""

import argparse
import os
import sys
import logging
import re
from pathlib import Path
from typing import List

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/06_sql_script_runner.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Phase registry
# Each entry: (file_number_prefix, filename, description)
# Order here is the canonical execution order.
# ---------------------------------------------------------------------------
SQL_DIR = Path(__file__).parent.parent / "sql"

PHASES: dict[str, tuple[str, str]] = {
    "keys":        ("010_create_materialized_views.sql",  "Key reference tables"),
    "invoice":     ("011_create_materialized_views.sql",  "Invoice support tables"),
    "excel":       ("012_create_materialized_views.sql",  "Excel imports"),
    "timesheet":   ("013_create_materialized_views.sql",  "Job task & timesheet details"),
    "allocation":  ("014_create_materialized_views.sql",  "Staff task allocation by day"),
    "performance": ("015_create_materialized_views.sql",  "Staff performance table"),
    "refresh":     ("020_refresh_materialized_views.sql", "Refresh all materialized views"),
    "powerbi":     ("030_create_powerbi_views.sql",       "Power BI wrapper views"),
}

# Predefined groups (order matches PHASES key order above)
GROUPS: dict[str, List[str]] = {
    "create": ["keys", "invoice", "excel", "timesheet", "allocation", "performance"],
    "all":    ["keys", "invoice", "excel", "timesheet", "allocation", "performance",
               "refresh", "powerbi"],
}


# ---------------------------------------------------------------------------
# SQL splitting helper
# ---------------------------------------------------------------------------

def _split_statements(sql_content: str) -> List[str]:
    """
    Split SQL content into individual statements on semicolons.
    Strips comments first so that semicolons inside comments don't produce
    spurious chunks, then drops empty or whitespace-only results.
    """
    # Remove block comments before splitting so embedded semicolons are harmless
    stripped_content = re.sub(r"/\*.*?\*/", "", sql_content, flags=re.DOTALL)
    # Remove line comments before splitting so embedded semicolons are harmless
    stripped_content = re.sub(r"--[^\n]*", "", stripped_content)

    statements = []
    for chunk in stripped_content.split(";"):
        stmt = chunk.strip()
        if stmt:
            statements.append(stmt)
    return statements


# ---------------------------------------------------------------------------
# Runner class
# ---------------------------------------------------------------------------

class SQLScriptRunner:
    """Execute SQL scripts in dependency order."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.conn = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self):
        try:
            self.conn = psycopg2.connect(self.connection_string)
            # autocommit=True is required for REFRESH MATERIALIZED VIEW CONCURRENTLY
            # (which cannot run inside a transaction block). It also works cleanly
            # for all DDL statements (CREATE/DROP MATERIALIZED VIEW, CREATE INDEX).
            self.conn.autocommit = True
            logger.info("Connected to PostgreSQL")
        except psycopg2.Error as e:
            logger.error(f"Connection failed: {e}")
            raise

    def disconnect(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Disconnected from PostgreSQL")

    # ------------------------------------------------------------------
    # Core execution
    # ------------------------------------------------------------------

    def run_sql_file(self, file_path: Path, continue_on_error: bool = False) -> bool:
        """Execute all statements in a SQL file."""
        if not file_path.exists():
            logger.error(f"SQL file not found: {file_path}")
            return False

        try:
            sql_content = file_path.read_text(encoding="utf-8")
        except OSError as e:
            logger.error(f"Cannot read {file_path}: {e}")
            return False

        statements = _split_statements(sql_content)
        logger.info(f"  {file_path.name} — {len(statements)} statements")

        success = True
        executed = 0

        with self.conn.cursor() as cursor:
            for i, stmt in enumerate(statements, 1):
                try:
                    cursor.execute(stmt)
                    executed += 1
                except psycopg2.Error as e:
                    logger.error(f"  Statement {i} failed: {e}")
                    logger.debug(f"  Statement: {stmt[:300]}")
                    if continue_on_error:
                        logger.warning("  Continuing after error")
                        success = False
                    else:
                        logger.error("  Aborting file execution")
                        return False

        logger.info(f"  {file_path.name} — {executed}/{len(statements)} OK")
        return success

    # ------------------------------------------------------------------
    # Phase execution
    # ------------------------------------------------------------------

    def run_phase(self, phase: str, continue_on_error: bool = False) -> bool:
        """Execute a single named phase."""
        if phase not in PHASES:
            logger.error(f"Unknown phase '{phase}'. Available: {list(PHASES)}")
            return False

        filename, description = PHASES[phase]
        file_path = SQL_DIR / filename

        logger.info(f"Phase [{phase}] {description}")
        return self.run_sql_file(file_path, continue_on_error)

    def run_phases(self, phases: List[str], continue_on_error: bool = False) -> bool:
        """Execute a list of phases in order."""
        all_success = True
        for phase in phases:
            ok = self.run_phase(phase, continue_on_error)
            if not ok:
                all_success = False
                if not continue_on_error:
                    logger.error(f"Stopping at phase [{phase}]")
                    return False
        return all_success


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="06_sql_script_runner.py",
        description="Run SQL view-creation and refresh scripts in dependency order.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join([
            "Phases (run in this order):",
            *(f"  --{name:<14} {fname}  —  {desc}"
              for name, (fname, desc) in PHASES.items()),
            "",
            "Groups:",
            "  --create        runs: " + ", ".join(f"--{p}" for p in GROUPS["create"]),
            "  --all           runs: " + ", ".join(f"--{p}" for p in GROUPS["all"]),
        ]),
    )

    # Group flags
    parser.add_argument("--all",            action="store_true", help="Run all phases in order")
    parser.add_argument("--create",         action="store_true", help="Run create phases 010–015")
    parser.add_argument("--refresh",        action="store_true", help="Run refresh phase 020")
    parser.add_argument("--powerbi",        action="store_true", help="Run Power BI phase 030")
    parser.add_argument("--skip-refresh",   action="store_true", help="Exclude refresh phase (020) from any group or --all run")

    # Individual phase flags
    for name, (_, desc) in PHASES.items():
        if name not in ("refresh", "powerbi"):   # already added as group flags above
            parser.add_argument(f"--{name}", action="store_true", help=f"Run phase: {desc}")

    # Behaviour flags
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue to next phase/statement when an error occurs",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print which phases would run without executing anything",
    )

    return parser


def _resolve_phases(args: argparse.Namespace) -> List[str]:
    """Return the ordered list of phases to execute based on parsed args."""
    if args.all:
        phases = list(GROUPS["all"])
        if args.skip_refresh:
            phases = [p for p in phases if p != "refresh"]
        return phases

    selected: List[str] = []

    # Group: --create expands to 010-015
    if args.create:
        for p in GROUPS["create"]:
            if p not in selected:
                selected.append(p)

    # Individual phases (in canonical order)
    for name in PHASES:
        flag_val = getattr(args, name.replace("-", "_"), False)
        if flag_val and name not in selected:
            selected.append(name)

    if args.skip_refresh and "refresh" in selected:
        selected.remove("refresh")

    # Preserve canonical execution order
    canonical = list(PHASES.keys())
    return sorted(selected, key=lambda p: canonical.index(p))


def main():
    parser = _build_parser()
    args = parser.parse_args()

    phases = _resolve_phases(args)

    if not phases:
        parser.print_help()
        sys.exit(0)

    # Dry-run: just list what would run
    if args.dry_run:
        print("Dry run — phases that would execute:")
        for phase in phases:
            filename, description = PHASES[phase]
            path = SQL_DIR / filename
            exists = "✓" if path.exists() else "✗ NOT FOUND"
            print(f"  [{phase}]  {filename}  ({description})  {exists}")
        sys.exit(0)

    connection_string = os.getenv("POSTGRES_CONNECTION")
    if not connection_string:
        logger.error("POSTGRES_CONNECTION environment variable not set")
        sys.exit(1)

    runner = SQLScriptRunner(connection_string)
    try:
        runner.connect()
        ok = runner.run_phases(phases, continue_on_error=args.continue_on_error)
    finally:
        runner.disconnect()

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
