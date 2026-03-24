#!/usr/bin/env python3
"""
SyncHub (SQL Server) to PostgreSQL SCD Type 2 Sync

Connects to SyncHub's SQL Server database and syncs to PostgreSQL with full history tracking.
"""

import os
import sys
import logging
import time
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional
import pyodbc
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import schedule
from dotenv import load_dotenv

load_dotenv()

# Fix for Windows console encoding issues
if sys.platform == "win32":
    import codecs

    if sys.stdout.encoding != "utf-8":
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
    if sys.stderr.encoding != "utf-8":
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("synchub_scd2_sync.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class SyncHubSQLServerSource:
    """Client for reading data from SyncHub's SQL Server database"""

    def __init__(self, server: str, database: str, username: str, password: str):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.conn = None

    def connect(self):
        """Establish database connection"""
        try:
            # Build connection string for Azure SQL
            connection_string = (
                f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server={self.server},1433;"
                f"Database={self.database};"
                f"Uid={self.username};"
                f"Pwd={self.password};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=yes;"
                f"Connection Timeout=30;"
            )

            self.conn = pyodbc.connect(connection_string)
            logger.info(
                f"Successfully connected to SyncHub SQL Server: {self.database}"
            )
        except pyodbc.Error as e:
            logger.error(f"Error connecting to SyncHub SQL Server: {e}")
            raise

    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from SyncHub SQL Server")

    def get_all_tables(
        self, schemas: Optional[List[str]] = None
    ) -> List[Dict[str, str]]:
        """Get list of all tables from specified schemas"""
        cursor = self.conn.cursor()

        if schemas:
            schema_filter = f"AND TABLE_SCHEMA IN ({','.join(['?']*len(schemas))})"
            query = f"""
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                {schema_filter}
                AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
            cursor.execute(query, schemas)
        else:
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)

        tables = []
        for row in cursor.fetchall():
            tables.append({"schema": row[0], "table": row[1]})

        logger.info(f"Found {len(tables)} tables in SyncHub database")
        cursor.close()
        return tables

    def get_table_data(self, schema: str, table: str) -> List[tuple]:
        """Fetch all data from a specific table"""
        cursor = self.conn.cursor()
        query = f"SELECT * FROM [{schema}].[{table}]"
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        return data

    def get_table_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get column definitions for a table"""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """,
            (schema, table),
        )

        columns = []
        for row in cursor.fetchall():
            columns.append(
                {
                    "name": row[0],
                    "data_type": row[1],
                    "max_length": row[2],
                    "numeric_precision": row[3],
                    "numeric_scale": row[4],
                    "nullable": row[5] == "YES",
                    "default": row[6],
                }
            )
        cursor.close()
        return columns

    def get_primary_key_columns(self, schema: str, table: str) -> List[str]:
        """Get primary key columns for a table"""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
            AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """,
            (schema, table),
        )

        pks = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return pks


class PostgresSCD2Destination:
    """Handle PostgreSQL SCD Type 2 operations"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.conn = None

    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.conn.autocommit = False
            logger.info("Successfully connected to destination PostgreSQL")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to destination PostgreSQL: {e}")
            raise

    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from destination PostgreSQL")

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

    def create_scd2_table(self, table_name: str, columns: List[Dict[str, Any]]):
        """Create SCD Type 2 table with versioning columns"""
        if not columns:
            logger.warning(f"No columns provided for table {table_name}")
            return

        # Build source column definitions
        col_definitions = []
        for col in columns:
            pg_type = self._map_sqlserver_to_postgres(col)
            col_definitions.append(
                sql.SQL("{} {}").format(sql.Identifier(col["name"]), sql.SQL(pg_type))
            )

        # Add SCD Type 2 metadata columns
        scd_columns = [
            sql.SQL("_scd_id BIGSERIAL PRIMARY KEY"),
            sql.SQL("_scd_valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"),
            sql.SQL("_scd_valid_to TIMESTAMP"),
            sql.SQL("_scd_is_current BOOLEAN NOT NULL DEFAULT TRUE"),
            sql.SQL("_scd_source_hash VARCHAR(64)"),
        ]

        all_columns = col_definitions + scd_columns

        create_sql = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name), sql.SQL(", ").join(all_columns)
        )

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(create_sql)

                # Create indexes
                cursor.execute(
                    sql.SQL(
                        "CREATE INDEX {} ON {} (_scd_is_current) WHERE _scd_is_current = TRUE"
                    ).format(
                        sql.Identifier(f"idx_{table_name}_current"),
                        sql.Identifier(table_name),
                    )
                )

                cursor.execute(
                    sql.SQL("CREATE INDEX {} ON {} (_scd_valid_from)").format(
                        sql.Identifier(f"idx_{table_name}_valid_from"),
                        sql.Identifier(table_name),
                    )
                )

                logger.info(f"Created SCD Type 2 table: {table_name}")
            except psycopg2.Error as e:
                logger.error(f"Error creating table {table_name}: {e}")
                raise

    def _map_sqlserver_to_postgres(self, col: Dict[str, Any]) -> str:
        """Map SQL Server data types to PostgreSQL types"""
        data_type = col["data_type"].lower()

        # String types
        if data_type in ("varchar", "nvarchar"):
            if col["max_length"] and col["max_length"] > 0 and col["max_length"] != -1:
                return f"VARCHAR({col['max_length']})"
            return "TEXT"
        elif data_type in ("char", "nchar"):
            if col["max_length"] and col["max_length"] > 0:
                return f"CHAR({col['max_length']})"
            return "CHAR(1)"
        elif data_type in ("text", "ntext"):
            return "TEXT"

        # Numeric types
        elif data_type in ("int", "integer"):
            return "INTEGER"
        elif data_type == "bigint":
            return "BIGINT"
        elif data_type == "smallint":
            return "SMALLINT"
        elif data_type == "tinyint":
            return "SMALLINT"
        elif data_type == "bit":
            return "BOOLEAN"
        elif data_type in ("decimal", "numeric"):
            precision = col.get("numeric_precision", 18)
            scale = col.get("numeric_scale", 0)
            return f"NUMERIC({precision},{scale})"
        elif data_type in ("money", "smallmoney"):
            return "NUMERIC(19,4)"
        elif data_type in ("float", "real"):
            return "DOUBLE PRECISION"

        # Date/Time types
        elif data_type in ("datetime", "datetime2", "smalldatetime"):
            return "TIMESTAMP"
        elif data_type == "date":
            return "DATE"
        elif data_type == "time":
            return "TIME"
        elif data_type == "datetimeoffset":
            return "TIMESTAMP WITH TIME ZONE"

        # Binary types
        elif data_type in ("binary", "varbinary", "image"):
            return "BYTEA"

        # Other types
        elif data_type == "uniqueidentifier":
            return "UUID"
        elif data_type == "xml":
            return "XML"

        else:
            logger.warning(f"Unknown SQL Server type '{data_type}', using TEXT")
            return "TEXT"

    def calculate_row_hash(self, row: tuple) -> str:
        """Calculate hash of row data for change detection"""
        row_str = "|".join([str(v) if v is not None else "NULL" for v in row])
        return hashlib.md5(row_str.encode()).hexdigest()

    def create_current_view(self, table_name: str, columns: List[Dict[str, Any]]):
        """
        Create a view showing only current records (where _scd_is_current = TRUE)

        Args:
            table_name: Name of the base table
            columns: Column definitions from source
        """
        # view_name = f"vw_{table_name}_current"
        # For better readability, we can remove the prefix if it exists
        view_name = f"{table_name.split('_')[-1]}"

        # Get list of source columns (exclude SCD metadata columns)
        source_columns = [col["name"] for col in columns]

        with self.conn.cursor() as cursor:
            try:
                # Create view with only current records
                create_view_sql = sql.SQL("""
                    CREATE OR REPLACE VIEW {} AS
                    SELECT {}
                    FROM {}
                    WHERE _scd_is_current = TRUE
                """).format(
                    sql.Identifier(view_name),
                    sql.SQL(", ").join([sql.Identifier(col) for col in source_columns]),
                    sql.Identifier(table_name),
                )

                cursor.execute(create_view_sql)
                logger.info(f"Created current records view: {view_name}")

            except psycopg2.Error as e:
                logger.error(f"Error creating view {view_name}: {e}")
                raise

    def sync_scd2_table(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        source_data: List[tuple],
        primary_keys: List[str],
    ):
        """Sync data using SCD Type 2 pattern"""
        column_names = [col["name"] for col in columns]

        # Calculate hashes for source data
        source_with_hash = []
        for row in source_data:
            row_hash = self.calculate_row_hash(row)
            source_with_hash.append((row, row_hash))

        logger.info(f"Processing {len(source_with_hash)} records for {table_name}")

        with self.conn.cursor() as cursor:
            # Get current records from destination
            cursor.execute(
                sql.SQL("""
                SELECT {}, _scd_source_hash, _scd_id
                FROM {}
                WHERE _scd_is_current = TRUE
            """).format(
                    sql.SQL(", ").join([sql.Identifier(col) for col in column_names]),
                    sql.Identifier(table_name),
                )
            )

            current_records = cursor.fetchall()

            # Build lookup of current records by primary key
            if primary_keys:
                pk_indices = [column_names.index(pk) for pk in primary_keys]
                current_by_pk = {}
                for record in current_records:
                    pk_values = tuple(record[i] for i in pk_indices)
                    current_by_pk[pk_values] = {
                        "hash": record[-2],
                        "scd_id": record[-1],
                    }
            else:
                current_by_pk = {}

            # Process source records
            new_records = []
            updated_records = []
            unchanged_count = 0

            for source_row, source_hash in source_with_hash:
                if primary_keys:
                    pk_values = tuple(source_row[i] for i in pk_indices)

                    if pk_values in current_by_pk:
                        current_hash = current_by_pk[pk_values]["hash"]
                        if source_hash != current_hash:
                            updated_records.append(
                                {
                                    "old_scd_id": current_by_pk[pk_values]["scd_id"],
                                    "new_row": source_row,
                                    "new_hash": source_hash,
                                }
                            )
                        else:
                            unchanged_count += 1
                    else:
                        new_records.append((source_row, source_hash))
                else:
                    new_records.append((source_row, source_hash))

            # Mark old records as not current
            if updated_records:
                old_ids = [r["old_scd_id"] for r in updated_records]
                cursor.execute(
                    sql.SQL("""
                    UPDATE {}
                    SET _scd_is_current = FALSE,
                        _scd_valid_to = CURRENT_TIMESTAMP
                    WHERE _scd_id = ANY(%s)
                """).format(sql.Identifier(table_name)),
                    (old_ids,),
                )
                logger.info(f"Marked {len(updated_records)} old records as not current")

            # Insert new versions of updated records
            if updated_records:
                insert_data = [
                    (list(rec["new_row"]) + [rec["new_hash"]])
                    for rec in updated_records
                ]

                insert_sql = sql.SQL("""
                    INSERT INTO {} ({}, _scd_source_hash)
                    VALUES %s
                """).format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join([sql.Identifier(col) for col in column_names]),
                )
                execute_values(cursor, insert_sql, insert_data)
                logger.info(f"Inserted {len(updated_records)} new versions")

            # Insert completely new records
            if new_records:
                insert_data = [
                    (list(row) + [row_hash]) for row, row_hash in new_records
                ]

                insert_sql = sql.SQL("""
                    INSERT INTO {} ({}, _scd_source_hash)
                    VALUES %s
                """).format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join([sql.Identifier(col) for col in column_names]),
                )
                execute_values(cursor, insert_sql, insert_data)
                logger.info(f"Inserted {len(new_records)} new records")

            logger.info(
                f"Summary - New: {len(new_records)}, Updated: {len(updated_records)}, Unchanged: {unchanged_count}"
            )

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

        return normalized


def scd2_sync(
    synchub_source: SyncHubSQLServerSource,
    postgres_dest: PostgresSCD2Destination,
    schemas: Optional[List[str]] = None,
    table_prefix: str = "",
    default_primary_key: str = "id",
    create_views: bool = True,
):
    """
    Perform SCD Type 2 sync from SyncHub to PostgreSQL

    Args:
        synchub_source: SyncHub source connection
        postgres_dest: PostgreSQL destination connection
        schemas: Optional list of schemas to sync
        table_prefix: Prefix for destination table names
        default_primary_key: Default primary key column name if none found
        create_views: If True, create views for current records (default: True)
    """
    logger.info("=" * 80)
    logger.info(f"Starting SCD Type 2 sync at {datetime.now()}")
    logger.info(f"Create current views: {create_views}")
    logger.info("=" * 80)

    try:
        synchub_source.connect()
        postgres_dest.connect()

        tables = synchub_source.get_all_tables(schemas)

        if not tables:
            logger.warning("No tables found to sync")
            return

        for table_info in tables:
            source_schema = table_info["schema"]
            source_table = table_info["table"]

            dest_table = postgres_dest._normalize_table_name(
                f"{table_prefix}{source_schema}_{source_table}"
            )

            try:
                logger.info(
                    f"\nProcessing {source_schema}.{source_table} -> {dest_table}"
                )

                columns = synchub_source.get_table_columns(source_schema, source_table)
                primary_keys = synchub_source.get_primary_key_columns(
                    source_schema, source_table
                )

                if not primary_keys:
                    column_names = [col["name"] for col in columns]
                    if default_primary_key in column_names:
                        primary_keys = [default_primary_key]
                    else:
                        logger.warning(
                            f"No primary key found for {source_schema}.{source_table}"
                        )
                        primary_keys = []

                if not postgres_dest.table_exists(dest_table):
                    postgres_dest.create_scd2_table(dest_table, columns)

                source_data = synchub_source.get_table_data(source_schema, source_table)

                if not source_data:
                    logger.info(f"No data in {source_schema}.{source_table}")
                    continue

                postgres_dest.sync_scd2_table(
                    dest_table, columns, source_data, primary_keys
                )

                # Create view for current records if enabled
                if create_views:
                    postgres_dest.create_current_view(dest_table, columns)

                postgres_dest.conn.commit()
                logger.info(f"[SUCCESS] Synced {source_schema}.{source_table}")

            except Exception as e:
                logger.error(f"Error syncing {source_schema}.{source_table}: {e}")
                postgres_dest.conn.rollback()
                continue

        logger.info("=" * 80)
        logger.info(f"SCD Type 2 sync completed at {datetime.now()}")
        logger.info("=" * 80)

    finally:
        synchub_source.disconnect()
        postgres_dest.disconnect()


def main():
    """Main entry point"""

    # Parse SyncHub SQL Server connection details
    SYNCHUB_SERVER = os.getenv(
        "SYNCHUB_SERVER"
    )  # e.g., synchub-io.database.windows.net
    SYNCHUB_DATABASE = os.getenv("SYNCHUB_DATABASE")
    SYNCHUB_USERNAME = os.getenv("SYNCHUB_USERNAME")
    SYNCHUB_PASSWORD = os.getenv("SYNCHUB_PASSWORD")

    POSTGRES_CONNECTION = os.getenv("POSTGRES_CONNECTION")

    SCHEMAS_TO_SYNC = (
        os.getenv("SCHEMAS_TO_SYNC", "").split(",")
        if os.getenv("SCHEMAS_TO_SYNC")
        else None
    )
    TABLE_PREFIX = os.getenv("TABLE_PREFIX", "synchub_")
    DEFAULT_PRIMARY_KEY = os.getenv("DEFAULT_PRIMARY_KEY", "id")
    CREATE_VIEWS = os.getenv("CREATE_VIEWS", "true").lower() == "true"
    SYNC_SCHEDULE = os.getenv("SYNC_SCHEDULE", "1d")

    if not all(
        [
            SYNCHUB_SERVER,
            SYNCHUB_DATABASE,
            SYNCHUB_USERNAME,
            SYNCHUB_PASSWORD,
            POSTGRES_CONNECTION,
        ]
    ):
        logger.error(
            "All SyncHub SQL Server credentials and POSTGRES_CONNECTION are required"
        )
        sys.exit(1)

    synchub_source = SyncHubSQLServerSource(
        SYNCHUB_SERVER, SYNCHUB_DATABASE, SYNCHUB_USERNAME, SYNCHUB_PASSWORD
    )
    postgres_dest = PostgresSCD2Destination(POSTGRES_CONNECTION)

    def run_sync():
        scd2_sync(
            synchub_source,
            postgres_dest,
            SCHEMAS_TO_SYNC,
            TABLE_PREFIX,
            DEFAULT_PRIMARY_KEY,
            CREATE_VIEWS,
        )

    # Parse schedule
    if SYNC_SCHEDULE.endswith(("m", "h", "d")):
        interval = SYNC_SCHEDULE
        if interval.endswith("m"):
            minutes = int(interval[:-1])
            schedule.every(minutes).minutes.do(run_sync)
        elif interval.endswith("h"):
            hours = int(interval[:-1])
            schedule.every(hours).hours.do(run_sync)
        elif interval.endswith("d"):
            days = int(interval[:-1])
            schedule.every(days).days.do(run_sync)
    else:
        schedule.every().day.at("02:00").do(run_sync)

    logger.info("Running initial sync...")
    run_sync()

    # TODO: Use a loop to run pending scheduled tasks
    # logger.info("Starting scheduled execution. Press Ctrl+C to stop.")
    # try:
    #     while True:
    #         schedule.run_pending()
    #         time.sleep(60)
    # except KeyboardInterrupt:
    #     logger.info("Shutting down...")
    #     sys.exit(0)


if __name__ == "__main__":
    main()
