#!/usr/bin/env python3
"""
Public Excel to PostgreSQL Data Sync Script

This script reads publicly accessible Excel files (from SharePoint, OneDrive, or web URLs)
and persists the data to a PostgreSQL database. Each Excel tab becomes a separate table.
Works with files that can be accessed without authentication (incognito browser works).
"""

import os
import sys
import logging
import time
import hashlib
import io
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import schedule
import requests
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/05_sharepoint_to_db.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class PublicFileDownloader:
    """Download publicly accessible files from web URLs"""

    @staticmethod
    def convert_sharepoint_url_to_download(share_url: str) -> str:
        """
        Convert SharePoint sharing URL to direct download URL

        Args:
            share_url: SharePoint sharing URL (e.g., https://company.sharepoint.com/:x:/...?e=xxx)

        Returns:
            Direct download URL
        """
        # If it's already a direct download URL, return as-is
        if "download=1" in share_url or "_layouts/15/download.aspx" in share_url:
            return share_url

        # SharePoint sharing link pattern
        if "sharepoint.com" in share_url and ":x:" in share_url:
            # Extract the sharing token
            # Format: https://company.sharepoint.com/:x:/s/site/EabC123.../xyz?e=token
            # Convert to: https://company.sharepoint.com/personal/.../file.xlsx?download=1

            # For now, try adding download=1 parameter
            if "?" in share_url:
                return share_url.split("?")[0] + "?download=1"
            else:
                return share_url + "?download=1"

        # OneDrive sharing link pattern
        if "onedrive.live.com" in share_url or "1drv.ms" in share_url:
            # OneDrive links can be converted by replacing 'view' with 'download'
            return share_url.replace("view.aspx", "download.aspx")

        return share_url

    @staticmethod
    def download_file(url: str, timeout: int = 60) -> bytes:
        """
        Download file from URL

        Args:
            url: URL to download from
            timeout: Request timeout in seconds

        Returns:
            File content as bytes
        """
        try:
            logger.info(f"Downloading file from: {url[:100]}...")

            # Try to convert SharePoint URLs to direct download
            download_url = PublicFileDownloader.convert_sharepoint_url_to_download(url)

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

            response = requests.get(
                download_url, headers=headers, timeout=timeout, allow_redirects=True
            )
            response.raise_for_status()

            # Check if we got HTML instead of Excel (common with sharing links)
            content_type = response.headers.get("Content-Type", "")
            if "text/html" in content_type:
                logger.warning(
                    "Received HTML instead of Excel file. This usually means:"
                )
                logger.warning("1. The URL requires authentication")
                logger.warning("2. The sharing link format is not supported")
                logger.warning(
                    "3. Try using the 'Copy direct link' option in SharePoint"
                )
                raise ValueError(
                    "URL returned HTML instead of Excel file - authentication may be required"
                )

            logger.info(f"Successfully downloaded {len(response.content)} bytes")
            return response.content

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading file: {e}")
            raise


class ExcelProcessor:
    """Process Excel files and extract data from multiple tabs"""

    @staticmethod
    def read_excel_tabs(
        file_content: bytes, skip_empty_sheets: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Read all tabs from an Excel file

        Args:
            file_content: Excel file content as bytes
            skip_empty_sheets: Skip sheets with no data

        Returns:
            Dictionary mapping sheet names to DataFrames
        """

        # Add a size check to prevent processing very large files that could cause memory issues
        if len(file_content) > 50 * 1024 * 1024:  # 50MB limit
            raise ValueError("Excel file too large (>50MB)")

        try:
            # Read Excel file from bytes
            excel_file = pd.ExcelFile(io.BytesIO(file_content))

            sheets_data = {}
            for sheet_name in excel_file.sheet_names:
                logger.info(f"Reading sheet: {sheet_name}")

                # Read the sheet
                df = pd.read_excel(excel_file, sheet_name=sheet_name)

                # Skip empty sheets if requested
                if skip_empty_sheets and df.empty:
                    logger.warning(f"Skipping empty sheet: {sheet_name}")
                    continue

                # Clean column names
                df.columns = [
                    ExcelProcessor._clean_column_name(col) for col in df.columns
                ]

                # Remove completely empty rows
                df = df.dropna(how="all")

                # Reset index
                df = df.reset_index(drop=True)

                sheets_data[sheet_name] = df
                logger.info(
                    f"Sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns"
                )

            return sheets_data

        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            raise

    @staticmethod
    def _clean_column_name(column_name: str) -> str:
        """Clean column name for PostgreSQL compatibility"""
        column_name = str(column_name)
        cleaned = "".join(c if c.isalnum() else "_" for c in column_name)
        while "__" in cleaned:
            cleaned = cleaned.replace("__", "_")
        cleaned = cleaned.strip("_").lower()
        if cleaned and cleaned[0].isdigit():
            cleaned = "col_" + cleaned
        if not cleaned:
            cleaned = "unnamed_column"
        return cleaned


class PostgresDestination:
    """Handle PostgreSQL database synchronization"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.conn = None

    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.conn.autocommit = False
            logger.info("Successfully connected to PostgreSQL")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from PostgreSQL")

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                )
            """,
                (table_name,),
            )
            return cursor.fetchone()[0]

    def is_scd2_table(self, table_name: str) -> bool:
        """Check if table has SCD2 metadata columns (specifically _scd_is_current)."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = %s
                    AND column_name = '_scd_is_current'
                )
                """,
                (table_name,),
            )
            return cursor.fetchone()[0]

    def drop_table(self, table_name: str):
        """Drop a table if it exists"""
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                        sql.Identifier(table_name)
                    )
                )
                logger.info(f"Dropped table: {table_name}")
            except psycopg2.Error as e:
                logger.error(f"Error dropping table {table_name}: {e}")
                raise

    def create_table_from_dataframe(self, table_name: str, df: pd.DataFrame):
        """Create a table based on DataFrame structure (no SCD2 columns)."""
        if df.empty:
            logger.warning(f"DataFrame is empty for table {table_name}")
            return

        col_definitions = []
        for col_name, dtype in df.dtypes.items():
            pg_type = self._pandas_dtype_to_postgres(dtype)
            col_definitions.append(
                sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(pg_type))
            )

        create_sql = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name), sql.SQL(", ").join(col_definitions)
        )

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(create_sql)
                logger.info(f"Created table: {table_name}")
            except psycopg2.Error as e:
                logger.error(f"Error creating table {table_name}: {e}")
                raise

    def create_scd2_table(self, table_name: str, df: pd.DataFrame):
        """Create a table with SCD2 metadata columns appended."""
        if df.empty:
            logger.warning(f"DataFrame is empty for table {table_name}")
            return

        col_definitions = []
        for col_name, dtype in df.dtypes.items():
            pg_type = self._pandas_dtype_to_postgres(dtype)
            col_definitions.append(
                sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(pg_type))
            )

        # SCD Type 2 metadata columns (same shape as 04_synchub_to_db.py)
        scd_columns = [
            sql.SQL("_scd_id BIGSERIAL PRIMARY KEY"),
            sql.SQL("_scd_valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"),
            sql.SQL("_scd_valid_to TIMESTAMP"),
            sql.SQL("_scd_is_current BOOLEAN NOT NULL DEFAULT TRUE"),
            sql.SQL("_scd_source_hash VARCHAR(64)"),
        ]

        create_sql = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(col_definitions + scd_columns),
        )

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(create_sql)
                # Partial index for fast current-row lookups
                cursor.execute(
                    sql.SQL(
                        "CREATE INDEX {} ON {} (_scd_is_current) WHERE _scd_is_current = TRUE"
                    ).format(
                        sql.Identifier(f"idx_{table_name}_current"),
                        sql.Identifier(table_name),
                    )
                )
                # Index on source hash for the membership check in sync_scd2_table_full_row
                cursor.execute(
                    sql.SQL("CREATE INDEX {} ON {} (_scd_source_hash)").format(
                        sql.Identifier(f"idx_{table_name}_hash"),
                        sql.Identifier(table_name),
                    )
                )
                logger.info(f"Created SCD2 table: {table_name}")
            except psycopg2.Error as e:
                logger.error(f"Error creating SCD2 table {table_name}: {e}")
                raise

    def _pandas_dtype_to_postgres(self, dtype) -> str:
        """Map pandas dtype to PostgreSQL type"""
        dtype_str = str(dtype)
        if dtype_str.startswith("int"):
            return "BIGINT"
        elif dtype_str.startswith("float"):
            return "DOUBLE PRECISION"
        elif dtype_str.startswith("bool"):
            return "BOOLEAN"
        elif dtype_str.startswith("datetime"):
            return "TIMESTAMP"
        elif dtype_str.startswith("date"):
            return "DATE"
        else:
            return "TEXT"

    def insert_dataframe(self, table_name: str, df: pd.DataFrame):
        """Insert DataFrame data into a table (no SCD2 columns)."""
        if df.empty:
            logger.warning(f"No data to insert into {table_name}")
            return

        df = df.where(pd.notnull(df), None)
        data = [tuple(row) for row in df.values]
        columns = list(df.columns)

        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join([sql.Identifier(col) for col in columns]),
        )

        with self.conn.cursor() as cursor:
            try:
                execute_values(cursor, insert_sql, data)
                logger.info(f"Inserted {len(data)} rows into {table_name}")
            except psycopg2.Error as e:
                logger.error(f"Error inserting data into {table_name}: {e}")
                raise

    @staticmethod
    def calculate_row_hash(row) -> str:
        """MD5 hash of a row's values. Treats None and NaN identically as 'NULL'."""
        parts = []
        for v in row:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                parts.append("NULL")
            else:
                parts.append(str(v))
        return hashlib.md5("|".join(parts).encode()).hexdigest()

    def sync_scd2_table_full_row(self, table_name: str, df: pd.DataFrame):
        """SCD Type 2 sync using full-row hash as the natural key.

        Excel data has no explicit primary key, so we treat each unique row as
        an entity identified by the hash of all its column values. On each run:
          - Rows present in source AND in destination (matching hash) → stay current.
          - Rows in destination but not in source → marked _scd_is_current=FALSE,
            _scd_valid_to=CURRENT_TIMESTAMP.
          - Rows in source but not in destination → inserted as _scd_is_current=TRUE.

        An edit to an existing row therefore appears as one expire + one insert,
        since the hash changes when any column value changes.
        """
        if df.empty:
            logger.warning(f"No data to sync into {table_name}")
            return

        # Replace NaN with None for consistent hashing and insertion
        df = df.where(pd.notnull(df), None)
        columns = list(df.columns)

        # Build hash → row-tuple map for the source. Duplicate source rows
        # collapse naturally (same hash → same key).
        source_hashes = {}
        for row_tuple in df.itertuples(index=False, name=None):
            row_hash = self.calculate_row_hash(row_tuple)
            source_hashes[row_hash] = row_tuple

        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    "SELECT _scd_source_hash, _scd_id FROM {} WHERE _scd_is_current = TRUE"
                ).format(sql.Identifier(table_name))
            )
            current_dest = {row[0]: row[1] for row in cursor.fetchall()}

            source_hash_set = set(source_hashes.keys())
            dest_hash_set = set(current_dest.keys())

            to_insert_hashes = source_hash_set - dest_hash_set
            to_expire_hashes = dest_hash_set - source_hash_set
            unchanged_count = len(source_hash_set & dest_hash_set)

            # Expire rows that disappeared from source
            if to_expire_hashes:
                expire_ids = [current_dest[h] for h in to_expire_hashes]
                cursor.execute(
                    sql.SQL(
                        "UPDATE {} SET _scd_is_current = FALSE, _scd_valid_to = CURRENT_TIMESTAMP "
                        "WHERE _scd_id = ANY(%s)"
                    ).format(sql.Identifier(table_name)),
                    (expire_ids,),
                )
                logger.info(f"Expired {len(to_expire_hashes)} rows no longer present in source")

            # Insert rows newly seen in source
            if to_insert_hashes:
                insert_data = [
                    list(source_hashes[h]) + [h] for h in to_insert_hashes
                ]
                insert_sql = sql.SQL(
                    "INSERT INTO {} ({}, _scd_source_hash) VALUES %s"
                ).format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join([sql.Identifier(col) for col in columns]),
                )
                execute_values(cursor, insert_sql, insert_data)
                logger.info(f"Inserted {len(to_insert_hashes)} new rows")

            logger.info(
                f"SCD2 sync summary for {table_name}: "
                f"unchanged={unchanged_count}, new={len(to_insert_hashes)}, "
                f"expired={len(to_expire_hashes)}"
            )

    def create_view(self, table_name: str, df: pd.DataFrame, scd2: bool = False):
        """Create a view over the table using the last segment of the table name.

        When scd2=True, the view filters _scd_is_current = TRUE so downstream
        consumers see only the latest snapshot. Otherwise it is a plain SELECT.
        """
        view_name = table_name.removeprefix("excel_")
        columns = list(df.columns)

        with self.conn.cursor() as cursor:
            try:
                if scd2:
                    create_view_sql = sql.SQL(
                        "CREATE OR REPLACE VIEW {} AS SELECT {} FROM {} WHERE _scd_is_current = TRUE"
                    ).format(
                        sql.Identifier(view_name),
                        sql.SQL(", ").join([sql.Identifier(col) for col in columns]),
                        sql.Identifier(table_name),
                    )
                else:
                    create_view_sql = sql.SQL(
                        "CREATE OR REPLACE VIEW {} AS SELECT {} FROM {}"
                    ).format(
                        sql.Identifier(view_name),
                        sql.SQL(", ").join([sql.Identifier(col) for col in columns]),
                        sql.Identifier(table_name),
                    )
                cursor.execute(create_view_sql)
                logger.info(f"Created view: {view_name} (scd2={scd2})")
            except psycopg2.Error as e:
                logger.error(f"Error creating view {view_name}: {e}")
                raise

    def sync_table(self, table_name: str, df: pd.DataFrame, drop_and_recreate: bool = False):
        """Sync DataFrame to table.

        Args:
            table_name: destination table name (excel_<sheet_name>)
            df: DataFrame to sync
            drop_and_recreate:
              - True:  drop + recreate as a plain table, INSERT all rows. No SCD2
                       history is kept. Use for initial bulk load or resets.
              - False: SCD Type 2 mode (default). Tracks row history via full-row
                       MD5 hash; only the latest snapshot is exposed via the view.
                       Existing plain (non-SCD2) tables are auto-rebuilt as SCD2.
        """
        try:
            table_name = self._normalize_table_name(table_name)
            logger.info(f"Syncing table: {table_name} (drop_and_recreate={drop_and_recreate})")

            if drop_and_recreate:
                self.drop_table(table_name)
                self.create_table_from_dataframe(table_name, df)
                self.insert_dataframe(table_name, df)
                self.create_view(table_name, df, scd2=False)
            else:
                # If a non-SCD2 table exists from a prior run, rebuild it as SCD2.
                # This is one-time data loss but converts the table to the new shape.
                if self.table_exists(table_name) and not self.is_scd2_table(table_name):
                    logger.warning(
                        f"Table {table_name} exists without SCD2 columns. "
                        "Dropping and recreating as SCD2 — existing rows will be lost."
                    )
                    self.drop_table(table_name)

                if not self.table_exists(table_name):
                    self.create_scd2_table(table_name, df)

                self.sync_scd2_table_full_row(table_name, df)
                self.create_view(table_name, df, scd2=True)

            self.conn.commit()
            logger.info(f"Successfully synced table: {table_name} ({len(df)} source rows)")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to sync table {table_name}: {e}")
            raise

    def _normalize_table_name(self, table_name: str) -> str:
        """Normalize table name for PostgreSQL compatibility"""
        normalized = table_name.lower()
        normalized = "".join(c if c.isalnum() or c == "_" else "_" for c in normalized)
        while "__" in normalized:
            normalized = normalized.replace("__", "_")
        normalized = normalized.strip("_")

        if len(normalized) > 63:
            hash_suffix = hashlib.md5(table_name.encode()).hexdigest()[:8]
            normalized = normalized[:54] + "_" + hash_suffix
            logger.warning(
                f"Table name '{table_name}' exceeds 63 chars, truncating to '{normalized}'"
            )

        return normalized


def sync_excel_to_postgres(
    file_url: str,
    postgres_dest: PostgresDestination,
    table_prefix: str = "",
    drop_and_recreate: bool = False,
):
    """
    Sync an Excel file from public URL to PostgreSQL

    Args:
        file_url: Public URL to Excel file
        postgres_dest: PostgreSQL destination instance
        table_prefix: Optional prefix for table names
        drop_and_recreate: If True, drop and recreate tables (default: True). If False, only create if missing.
    """
    logger.info("=" * 80)
    logger.info(f"Starting Excel sync at {datetime.now()}")
    logger.info(f"URL: {file_url}")
    logger.info("=" * 80)

    try:
        # Download Excel file
        file_content = PublicFileDownloader.download_file(file_url)

        # Read all tabs from Excel
        sheets_data = ExcelProcessor.read_excel_tabs(file_content)

        if not sheets_data:
            logger.warning(f"No data found in Excel file: {file_url}")
            return

        # Sync each sheet to a table
        for sheet_name, df in sheets_data.items():
            table_name = f"{table_prefix}{sheet_name}"
            postgres_dest.sync_table(table_name, df, drop_and_recreate)

        logger.info("=" * 80)
        logger.info(f"Excel sync completed successfully at {datetime.now()}")
        logger.info(f"Synced {len(sheets_data)} sheets")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Excel sync failed: {e}")
        raise


def sync_multiple_files(
    file_urls: List[str],
    postgres_dest: PostgresDestination,
    table_prefix: str = "",
    drop_and_recreate: bool = False,
):
    """Sync multiple Excel files to PostgreSQL"""
    total_files = len(file_urls)
    successful = 0
    failed = 0

    for i, file_url in enumerate(file_urls, 1):
        logger.info(f"Processing file {i}/{total_files}: {file_url[:100]}...")
        try:
            sync_excel_to_postgres(file_url, postgres_dest, table_prefix, drop_and_recreate)
            successful += 1
        except Exception as e:
            logger.error(f"Failed to sync file {file_url}: {e}")
            failed += 1
            continue

    logger.info(
        f"Batch sync completed: {successful} successful, {failed} failed out of {total_files} files"
    )


def main():
    """Main entry point"""

    # Configuration from environment variables
    POSTGRES_CONNECTION = os.getenv("POSTGRES_CONNECTION")

    # Excel file URL(s) - supports multiple comma-separated URLs
    EXCEL_FILE_URLS = os.getenv("EXCEL_FILE_URLS", "")

    TABLE_PREFIX_EXCEL = os.getenv("TABLE_PREFIX_EXCEL", "")
    # Default false → SCD2 mode (track history). Set true to force a fresh rebuild.
    DROP_AND_RECREATE = os.getenv("DROP_AND_RECREATE", "false").lower() == "true"
    SYNC_SCHEDULE = os.getenv("SYNC_SCHEDULE", "0 2 * * *")  # Default: 2 AM daily

    # Validate configuration
    if not POSTGRES_CONNECTION:
        logger.error("POSTGRES_CONNECTION environment variable is required")
        sys.exit(1)

    if not EXCEL_FILE_URLS:
        logger.error("EXCEL_FILE_URLS environment variable is required")
        sys.exit(1)

    # Parse file URLs (comma-separated)
    file_urls = [url.strip() for url in EXCEL_FILE_URLS.split(",") if url.strip()]

    if not file_urls:
        logger.error("No valid Excel file URLs provided")
        sys.exit(1)

    logger.info(f"Configured to sync {len(file_urls)} file(s)")
    logger.info(f"Drop and recreate tables: {DROP_AND_RECREATE}")

    # Initialize PostgreSQL connection
    postgres_dest = PostgresDestination(POSTGRES_CONNECTION)

    # Define sync function
    def run_sync():
        try:
            postgres_dest.connect()
            if len(file_urls) == 1:
                sync_excel_to_postgres(
                    file_urls[0], postgres_dest, TABLE_PREFIX_EXCEL, DROP_AND_RECREATE
                )
            else:
                sync_multiple_files(
                    file_urls, postgres_dest, TABLE_PREFIX_EXCEL, DROP_AND_RECREATE
                )
        finally:
            postgres_dest.disconnect()

    # Parse schedule format
    schedule_parts = SYNC_SCHEDULE.split()

    if len(schedule_parts) == 1 and schedule_parts[0].endswith(("m", "h", "d")):
        interval = schedule_parts[0]
        if interval.endswith("m"):
            minutes = int(interval[:-1])
            schedule.every(minutes).minutes.do(run_sync)
            logger.info(f"Scheduled sync every {minutes} minutes")
        elif interval.endswith("h"):
            hours = int(interval[:-1])
            schedule.every(hours).hours.do(run_sync)
            logger.info(f"Scheduled sync every {hours} hours")
        elif interval.endswith("d"):
            days = int(interval[:-1])
            schedule.every(days).days.do(run_sync)
            logger.info(f"Scheduled sync every {days} days")
    else:
        schedule.every().day.at("02:00").do(run_sync)
        logger.info(f"Scheduled sync daily at 02:00")

    # Run initial sync
    logger.info("Running initial synchronization...")
    run_sync()

    # TODO: Use a loop to run pending scheduled tasks
    # Start scheduled execution
    # logger.info("Starting scheduled execution. Press Ctrl+C to stop.")

    # try:
    #     while True:
    #         schedule.run_pending()
    #         time.sleep(60)
    # except KeyboardInterrupt:
    #     logger.info("Shutting down scheduler...")
    #     sys.exit(0)


if __name__ == "__main__":
    main()
