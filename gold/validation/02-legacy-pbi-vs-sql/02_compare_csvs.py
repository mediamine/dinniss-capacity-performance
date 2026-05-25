#!/usr/bin/env python3
"""
Compare extracted pbi_* CSVs against legacy Power BI model exports.

For each CSV in the source directory (the ones produced by 01_extract_pbi_views.py),
find the corresponding CSV in the target directory and report differences:
  - Column set (missing or extra columns)
  - Row count
  - Row-level content (order-insensitive, via hash sets)
  - Sample of differing rows (first N, with their column values)

CSVs are streamed line-by-line via the csv module. Row hashes are stored as
16-byte MD5 digests in a set, so even pbi_2_Staff_Task_Allocation_byDay
(~65M rows) fits comfortably in 1-2 GB of memory.

Matching by filename prefix: e.g. extracted/pbi_1_Job_Task_Details_Table.csv
matches legacy/pbi_1_Job_Task_Details_Table_2021.csv (anything starting with
the same stem). If multiple legacy candidates match, the comparison is
skipped with a warning so the user can disambiguate.

Defaults resolve to the `extracted/` and `legacy/` folders next to this
script (gold/validation/02-legacy-pbi-vs-sql/).

Usage:
  # Compare extracted/ against legacy/ (using script-local defaults)
  python gold/validation/02-legacy-pbi-vs-sql/02_compare_csvs.py

  # Custom directories
  python gold/validation/02-legacy-pbi-vs-sql/02_compare_csvs.py \
      --source-dir gold/validation/02-legacy-pbi-vs-sql/extracted \
      --target-dir gold/validation/02-legacy-pbi-vs-sql/legacy

  # Compare a single explicit pair (overrides directory scan)
  python gold/validation/02-legacy-pbi-vs-sql/02_compare_csvs.py \
      --pair gold/validation/02-legacy-pbi-vs-sql/extracted/pbi_1_Job_Task_Details_Table.csv \
             gold/validation/02-legacy-pbi-vs-sql/legacy/pbi_1_Job_Task_Details_Table_2021.csv

  # Increase the number of sample diffs printed per file (default 5)
  python gold/validation/02-legacy-pbi-vs-sql/02_compare_csvs.py --sample-size 20
"""

import argparse
import csv
import hashlib
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# Force UTF-8 on stdout/stderr for Windows console compatibility.
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
        logging.FileHandler(SCRIPT_DIR / "logs" / "02_compare_csvs.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# Values that should all be treated as "missing" for the purposes of hashing.
# Power BI exports often use one of these instead of an empty cell.
NULL_TOKENS = {"", "NULL", "null", "(blank)", "Blank", "(empty)", "None", "NaN", "nan"}


def _normalize_value(v: str) -> str:
    """Normalize a CSV cell value for cross-export comparison.

    - Strips leading/trailing whitespace
    - Maps common NULL representations to a single sentinel
    Floats are NOT rounded here — caller decides whether to apply rounding.
    """
    if v is None:
        return "NULL"
    v = v.strip()
    if v in NULL_TOKENS:
        return "NULL"
    return v


def _row_digest(values: List[str]) -> bytes:
    """16-byte MD5 digest of a row. Bytes (not hex) to halve memory in big sets."""
    return hashlib.md5("|".join(values).encode("utf-8")).digest()


def _detect_encoding(path: Path) -> str:
    """Probe a CSV to decide encoding. UTF-8 first, cp1252 fallback.

    Power BI / Excel exports on Windows commonly use cp1252 (Windows-1252),
    which differs from UTF-8 for bytes 0x80–0x9F (e.g., 0x96 = en-dash).
    A 1 MB probe catches header + a large sample of body rows.
    """
    with open(path, "rb") as f:
        probe = f.read(1024 * 1024)
    try:
        probe.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "cp1252"


def _read_csv_header(path: Path) -> List[str]:
    """Return only the header row of a CSV — instant, no body parsing."""
    encoding = _detect_encoding(path)
    with open(path, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f)
        return next(reader, [])


def _stream_row_digests(
    path: Path, common_columns: List[str],
) -> Tuple[Set[bytes], Dict[bytes, Dict[str, str]], int]:
    """Stream-read a CSV and return:
      - the set of row digests restricted to common_columns (sorted alphabetically),
      - a dict mapping a small sample of digests -> the original row (as
        dict of column -> value) for use in printed diffs,
      - the total row count.
    """
    digests: Set[bytes] = set()
    samples: Dict[bytes, Dict[str, str]] = {}
    row_count = 0

    sample_quota = 200  # cap on samples retained for diff printing
    sorted_cols = sorted(common_columns)
    encoding = _detect_encoding(path)
    logger.info(f"    encoding: {encoding}")

    with open(path, "r", encoding=encoding, newline="", errors="replace") as f:
        reader = csv.DictReader(f)
        for raw_row in reader:
            row_count += 1
            normalized = {col: _normalize_value(raw_row.get(col, "")) for col in sorted_cols}
            digest = _row_digest([normalized[col] for col in sorted_cols])
            digests.add(digest)
            if len(samples) < sample_quota:
                samples[digest] = normalized

    return digests, samples, row_count


def _resolve_target(source_path: Path, target_dir: Path) -> Optional[Path]:
    """Find a CSV in target_dir whose filename starts with the source's stem.

    - Exact match wins if present (legacy/foo.csv for source/foo.csv).
    - Otherwise look for legacy/foo*.csv.
    - If multiple non-exact matches, return None (caller skips with warning).
    """
    exact = target_dir / source_path.name
    if exact.exists():
        return exact

    stem = source_path.stem  # filename without .csv
    candidates = sorted(target_dir.glob(f"{stem}*.csv"))
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        logger.warning(
            f"  Multiple target candidates for {source_path.name}: "
            f"{[c.name for c in candidates]}. Use --pair to disambiguate."
        )
        return None
    return None


def _compare_pair(source_path: Path, target_path: Path, sample_size: int) -> bool:
    """Compare two CSVs and log the diff. Returns True if they match exactly."""
    logger.info("-" * 60)
    logger.info(f"Comparing {source_path.name}")
    logger.info(f"  source: {source_path}")
    logger.info(f"  target: {target_path}")

    source_cols = _read_csv_header(source_path)
    target_cols = _read_csv_header(target_path)

    source_set = set(source_cols)
    target_set = set(target_cols)
    common = sorted(source_set & target_set)
    only_in_source = sorted(source_set - target_set)
    only_in_target = sorted(target_set - source_set)

    if only_in_source:
        logger.warning(f"  Columns only in source: {only_in_source}")
    if only_in_target:
        logger.warning(f"  Columns only in target: {only_in_target}")
    logger.info(f"  Common columns ({len(common)}): {common[:10]}{'...' if len(common) > 10 else ''}")

    if not common:
        logger.error("  No common columns — cannot compare row content.")
        return False

    logger.info(f"  Streaming source rows...")
    src_digests, src_samples, src_rows = _stream_row_digests(source_path, common)
    logger.info(f"    {src_rows:,} rows, {len(src_digests):,} unique row hashes")

    logger.info(f"  Streaming target rows...")
    tgt_digests, tgt_samples, tgt_rows = _stream_row_digests(target_path, common)
    logger.info(f"    {tgt_rows:,} rows, {len(tgt_digests):,} unique row hashes")

    if src_rows == tgt_rows:
        logger.info(f"  Row counts match: {src_rows:,}")
    else:
        logger.warning(f"  Row counts differ: source={src_rows:,}  target={tgt_rows:,}")

    only_src = src_digests - tgt_digests
    only_tgt = tgt_digests - src_digests
    in_both = src_digests & tgt_digests
    logger.info(f"  Matching unique rows: {len(in_both):,}")

    if only_src:
        logger.warning(f"  Unique rows in source but not target: {len(only_src):,}")
        for digest in list(only_src)[:sample_size]:
            row = src_samples.get(digest)
            if row is not None:
                logger.warning(f"    + {row}")
    if only_tgt:
        logger.warning(f"  Unique rows in target but not source: {len(only_tgt):,}")
        for digest in list(only_tgt)[:sample_size]:
            row = tgt_samples.get(digest)
            if row is not None:
                logger.warning(f"    - {row}")

    match = not (only_in_source or only_in_target or only_src or only_tgt or src_rows != tgt_rows)
    if match:
        logger.info(f"  Result: MATCH")
    else:
        logger.warning(f"  Result: DIFFER")
    return match


def main():
    parser = argparse.ArgumentParser(
        prog="02_compare_csvs.py",
        description="Compare extracted pbi_* CSVs against legacy exports.",
    )
    parser.add_argument(
        "--source-dir",
        default=str(SCRIPT_DIR / "extracted"),
        help="Directory containing the freshly extracted CSVs (default: extracted/ next to this script).",
    )
    parser.add_argument(
        "--target-dir",
        default=str(SCRIPT_DIR / "legacy"),
        help="Directory containing the legacy Power BI exports (default: legacy/ next to this script).",
    )
    parser.add_argument(
        "--pair",
        nargs=2,
        metavar=("SOURCE_CSV", "TARGET_CSV"),
        action="append",
        help="Compare a single explicit pair of files. Repeatable. Overrides directory scan.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=5,
        help="Number of sample diff rows to print per file (default: 5).",
    )
    args = parser.parse_args()

    pairs: List[Tuple[Path, Path]] = []

    if args.pair:
        # Explicit pairs override directory scan
        for src, tgt in args.pair:
            src_p, tgt_p = Path(src), Path(tgt)
            if not src_p.is_file():
                logger.error(f"Source file not found: {src_p}")
                sys.exit(1)
            if not tgt_p.is_file():
                logger.error(f"Target file not found: {tgt_p}")
                sys.exit(1)
            pairs.append((src_p, tgt_p))
    else:
        source_dir = Path(args.source_dir)
        target_dir = Path(args.target_dir)
        if not source_dir.is_dir():
            logger.error(f"Source directory not found: {source_dir}")
            sys.exit(1)
        if not target_dir.is_dir():
            logger.error(f"Target directory not found: {target_dir}")
            sys.exit(1)

        source_csvs = sorted(source_dir.glob("*.csv"))
        if not source_csvs:
            logger.error(f"No CSV files found in source directory: {source_dir}")
            sys.exit(1)

        for src in source_csvs:
            tgt = _resolve_target(src, target_dir)
            if tgt is None:
                logger.warning(f"No matching target for {src.name}, skipping.")
                continue
            pairs.append((src, tgt))

    if not pairs:
        logger.error("No CSV pairs to compare.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info(f"Comparing {len(pairs)} CSV pair(s)")
    logger.info("=" * 60)

    results = [(src.name, _compare_pair(src, tgt, args.sample_size)) for src, tgt in pairs]

    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    for name, ok in results:
        logger.info(f"  {'MATCH ' if ok else 'DIFFER'}  {name}")

    if any(not ok for _, ok in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
