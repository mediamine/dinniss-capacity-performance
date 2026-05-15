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
  020  powerbi      Power BI wrapper views
  110  refresh      Refresh all materialized views (daily)

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
import time
from pathlib import Path
from typing import List

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# Force UTF-8 on stdout/stderr so box-drawing chars (└─) and em-dashes (—) in
# log lines work on Windows consoles (default cp1252 raises UnicodeEncodeError).
# Linux/macOS default to UTF-8 already, so this block is a no-op on those.
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "replace")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "replace")

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/06_sql_script_runner.log", encoding="utf-8"),
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
    "powerbi":     ("020_create_powerbi_views.sql",       "Power BI wrapper views"),
    "refresh":     ("110_refresh_materialized_views.sql", "Refresh all materialized views"),
}

# Predefined groups (order matches PHASES key order above)
GROUPS: dict[str, List[str]] = {
    "create": ["keys", "invoice", "excel", "timesheet", "allocation", "performance"],
    "all":    ["keys", "invoice", "excel", "timesheet", "allocation", "performance",
               "powerbi", "refresh"],
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


# Maps the leading keyword pattern to (kind label, regex capturing the object name).
# Order matters — longer/more specific patterns first.
_STMT_PATTERNS = [
    ("CREATE MATERIALIZED VIEW", re.compile(r"\s*CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)", re.IGNORECASE)),
    ("DROP MATERIALIZED VIEW",   re.compile(r"\s*DROP\s+MATERIALIZED\s+VIEW\s+(?:IF\s+EXISTS\s+)?([^\s,;]+)", re.IGNORECASE)),
    ("REFRESH MATERIALIZED VIEW",re.compile(r"\s*REFRESH\s+MATERIALIZED\s+VIEW\s+(?:CONCURRENTLY\s+)?([^\s;]+)", re.IGNORECASE)),
    ("CREATE UNIQUE INDEX",      re.compile(r"\s*CREATE\s+UNIQUE\s+INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\S+\s+)?ON\s+([^\s(]+)", re.IGNORECASE)),
    ("CREATE INDEX",             re.compile(r"\s*CREATE\s+INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\S+\s+)?ON\s+([^\s(]+)", re.IGNORECASE)),
    ("DROP INDEX",               re.compile(r"\s*DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?([^\s,;]+)", re.IGNORECASE)),
    ("CREATE VIEW",              re.compile(r"\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([^\s(]+)", re.IGNORECASE)),
    ("DROP VIEW",                re.compile(r"\s*DROP\s+VIEW\s+(?:IF\s+EXISTS\s+)?([^\s,;]+)", re.IGNORECASE)),
    ("VACUUM",                   re.compile(r"\s*VACUUM(?:\s+\w+)*\s+([^\s;]+)", re.IGNORECASE)),
    ("ANALYZE",                  re.compile(r"\s*ANALYZE\s+([^\s;]+)", re.IGNORECASE)),
    ("CHECKPOINT",               re.compile(r"\s*CHECKPOINT\b", re.IGNORECASE)),
    ("SET",                      re.compile(r"\s*SET\s+([^\s=]+)", re.IGNORECASE)),
]


def _describe_statement(stmt: str) -> tuple[str, str]:
    """Return (kind, target) for log readability.

    Falls back to first keyword + truncated preview when the pattern is unknown.
    """
    for kind, pattern in _STMT_PATTERNS:
        m = pattern.match(stmt)
        if m:
            target = m.group(1) if m.groups() else ""
            return kind, target
    # Unknown statement — show first 60 chars on one line
    preview = re.sub(r"\s+", " ", stmt)[:60]
    return "(other)", preview


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
            self.conn = psycopg2.connect(
                self.connection_string,
                # TCP keepalives — survives idle periods during long CREATE / REFRESH
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            # autocommit=True is required for REFRESH MATERIALIZED VIEW CONCURRENTLY
            # (which cannot run inside a transaction block). It also works cleanly
            # for all DDL statements (CREATE/DROP MATERIALIZED VIEW, CREATE INDEX).
            self.conn.autocommit = True
            self._tune_session()
            logger.info("Connected to PostgreSQL (tuned session)")
        except psycopg2.Error as e:
            logger.error(f"Connection failed: {e}")
            raise

    def _tune_session(self):
        """Apply session-level GUCs sized for analytical CREATE/REFRESH workloads.

        Defaults assume a VM with ~16 GB RAM. Override per environment via the
        listed PG_* env vars without editing this file.
        """
        gucs = {
            "work_mem":                       os.getenv("PG_WORK_MEM",         "512MB"),
            "maintenance_work_mem":           os.getenv("PG_MAINT_WORK_MEM",   "2GB"),
            "max_parallel_workers_per_gather": os.getenv("PG_PARALLEL_WORKERS", "2"),
            "statement_timeout":              os.getenv("PG_STATEMENT_TIMEOUT", "4h"),
            "lock_timeout":                   os.getenv("PG_LOCK_TIMEOUT",      "5min"),
            "idle_in_transaction_session_timeout": os.getenv("PG_IDLE_TIMEOUT", "10min"),
        }
        with self.conn.cursor() as cur:
            for name, value in gucs.items():
                cur.execute(f"SET {name} = %s", (value,))
        logger.info("Session GUCs: " + ", ".join(f"{k}={v}" for k, v in gucs.items()))

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
        file_t0 = time.perf_counter()
        slow_threshold = float(os.getenv("PG_SLOW_LOG_SECONDS", "5"))
        verbose = os.getenv("PG_VERBOSE_LOG", "0") == "1"

        with self.conn.cursor() as cursor:
            for i, stmt in enumerate(statements, 1):
                kind, target = _describe_statement(stmt)
                label = f"[{i:>3}/{len(statements)}] {kind:<22} {target}"

                # Pre-statement log so a hung statement is identifiable in the log
                logger.info(f"    {label} ...")

                t0 = time.perf_counter()
                try:
                    cursor.execute(stmt)
                    elapsed = time.perf_counter() - t0
                    executed += 1

                    # Post-statement summary — always logged if slow, only logged
                    # at INFO if verbose, else logged at DEBUG.
                    msg = f"    {label}  done in {elapsed:>7.1f}s"
                    if elapsed >= slow_threshold or verbose:
                        logger.info(msg)
                    else:
                        logger.debug(msg)

                    # For CREATE MATERIALIZED VIEW: log row count + auto-ANALYZE
                    # so the next phase's planner has accurate stats.
                    mv_match = re.match(
                        r"\s*CREATE\s+MATERIALIZED\s+VIEW\s+([^\s(]+)",
                        stmt, re.IGNORECASE,
                    )
                    if mv_match:
                        mv_name = mv_match.group(1)
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {mv_name}")
                            row_count = cursor.fetchone()[0]
                            logger.info(f"      └─ {mv_name}: {row_count:,} rows")
                        except psycopg2.Error as count_err:
                            logger.debug(f"      └─ row count failed: {count_err}")

                        ana_t0 = time.perf_counter()
                        cursor.execute(f"ANALYZE {mv_name}")
                        logger.info(f"      └─ ANALYZE {mv_name} ({time.perf_counter() - ana_t0:.1f}s)")
                except psycopg2.Error as e:
                    elapsed = time.perf_counter() - t0
                    preview = re.sub(r"\s+", " ", stmt)[:200]
                    logger.error(f"    {label}  FAILED after {elapsed:.1f}s")
                    logger.error(f"      └─ error: {e}")
                    logger.error(f"      └─ SQL:   {preview}")
                    if continue_on_error:
                        logger.warning("    Continuing after error")
                        success = False
                    else:
                        logger.error("    Aborting file execution")
                        return False

        total = time.perf_counter() - file_t0
        logger.info(f"  {file_path.name} — {executed}/{len(statements)} OK in {total:.1f}s")
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
    parser.add_argument("--refresh",        action="store_true", help="Run refresh phase 110")
    parser.add_argument("--powerbi",        action="store_true", help="Run Power BI phase 020")
    parser.add_argument("--skip-refresh",   action="store_true", help="Exclude refresh phase (110) from any group or --all run")

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
