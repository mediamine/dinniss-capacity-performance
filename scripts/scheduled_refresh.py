#!/usr/bin/env python3
"""
Scheduled refresh runner for the gold layer.

Runs `python scripts/run_all_gold.py --refresh-only` every N hours (default 2).
Designed to be left running in the background — keeps a single long-lived
process alive, captures output to a daily-rotated log file, and skips an
overlapping run if the previous one is still in progress.

Usage:
  # Run forever, every 2 hours, immediate first run on startup
  python scripts/scheduled_refresh.py

  # Custom interval (e.g. every 4 hours)
  python scripts/scheduled_refresh.py --interval-hours 4

  # Skip the immediate startup run, wait for the first scheduled interval
  python scripts/scheduled_refresh.py --no-startup-run

  # Different command (default is run_all_gold.py --refresh-only)
  python scripts/scheduled_refresh.py --command "python scripts/run_all_gold.py"

Behaviour:
  - Each run is invoked as a subprocess so its own logs go to its own file
    (gold/scripts/py/*.log) and a crash in the child does not kill the scheduler.
  - If a run is still in progress when the next interval fires, the scheduled
    run is SKIPPED with a warning (overlap protection).
  - Daily-rotated log at logs/scheduled_refresh.log, 30 days kept.

To stop: Ctrl+C, or kill the process. The current in-progress refresh, if any,
will complete before the process exits.

For production-grade scheduling (survives reboots, restarts on failure), use a
native OS scheduler instead. See SCHEDULED_REFRESH.md (or the README) for cron,
systemd, and Windows Task Scheduler examples.
"""

import argparse
import logging
import logging.handlers
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import schedule


# UTF-8 stdout/stderr on Windows so any non-ASCII chars in the child's output
# don't crash the parent.
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "replace")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "replace")


os.makedirs("logs", exist_ok=True)

# Daily-rotated log file so it doesn't grow unbounded
_rotating_file_handler = logging.handlers.TimedRotatingFileHandler(
    "logs/scheduled_refresh.log",
    when="midnight",
    backupCount=30,
    encoding="utf-8",
)
_rotating_file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[_rotating_file_handler, logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# Overlap guard. If a refresh is still running when the next interval fires,
# we skip the new one instead of stacking parallel runs (which would clash
# on REFRESH MATERIALIZED VIEW locks).
_running = False


def _run_refresh(command: list) -> None:
    """Invoke the configured command as a subprocess. Logs success/failure but
    never raises, so the scheduling loop survives any failure in the child."""
    global _running
    if _running:
        logger.warning(
            "Previous refresh still in progress — SKIPPING this scheduled run."
        )
        return

    _running = True
    t0 = time.perf_counter()
    try:
        logger.info("=" * 60)
        logger.info(f"Refresh starting at {datetime.now().isoformat(timespec='seconds')}")
        logger.info(f"Command: {' '.join(command)}")
        logger.info("=" * 60)
        result = subprocess.run(command, check=False)
        elapsed = time.perf_counter() - t0
        if result.returncode == 0:
            logger.info(f"Refresh OK in {elapsed:.1f}s (next run in scheduled interval).")
        else:
            logger.error(
                f"Refresh FAILED with exit code {result.returncode} after {elapsed:.1f}s. "
                "Scheduler continues — next interval will be attempted."
            )
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        logger.exception(f"Unexpected error invoking refresh after {elapsed:.1f}s: {exc}")
    finally:
        _running = False


def main():
    parser = argparse.ArgumentParser(
        prog="scheduled_refresh.py",
        description="Run the gold-layer refresh on a recurring schedule.",
    )
    parser.add_argument(
        "--interval-hours",
        type=float,
        default=2.0,
        help="Hours between runs (default: 2).",
    )
    parser.add_argument(
        "--no-startup-run",
        action="store_true",
        help="Skip the immediate first run on startup; wait for the first interval.",
    )
    parser.add_argument(
        "--command",
        default=None,
        help=(
            "Override the command to run on each tick. POSIX-style string "
            "(use forward slashes on Windows, or escape backslashes). "
            "Default (when omitted): the current Python interpreter invoking "
            "scripts/run_all_gold.py --refresh-only."
        ),
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=30,
        help="How often the scheduler wakes up to check for due jobs (default: 30s).",
    )
    args = parser.parse_args()

    # Resolve the command. For the default, we build the argv list directly
    # to avoid shell-quoting pitfalls on Windows (where shlex.split(posix=False)
    # keeps the quote characters in the output and breaks subprocess lookup).
    if args.command is None:
        command = [sys.executable, "scripts/run_all_gold.py", "--refresh-only"]
    else:
        # User-supplied override — parse with POSIX rules (strips quotes,
        # works the same on Linux/macOS/Windows). User is responsible for
        # using forward slashes or properly escaped backslashes in paths.
        command = shlex.split(args.command, posix=True)
        if not command:
            logger.error("--command resolved to an empty argv list.")
            sys.exit(1)

    schedule.every(args.interval_hours).hours.do(_run_refresh, command=command)
    logger.info(
        f"Scheduler started. Interval: {args.interval_hours} hour(s). "
        f"Logging to logs/scheduled_refresh.log (rotated daily, 30 days kept)."
    )
    logger.info(f"Command per tick: {' '.join(command)}")
    logger.info("Press Ctrl+C to stop.")

    if not args.no_startup_run:
        logger.info("Running initial refresh now (use --no-startup-run to skip).")
        _run_refresh(command)
    else:
        logger.info(
            f"Skipping startup run; first scheduled run in ~{args.interval_hours} hour(s)."
        )

    try:
        while True:
            schedule.run_pending()
            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Exiting scheduler.")
        sys.exit(0)


if __name__ == "__main__":
    main()
