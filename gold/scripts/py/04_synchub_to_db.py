#!/usr/bin/env python3
"""
SyncHub (SQL Server) to PostgreSQL Data Sync Script

This script copies data from SyncHub's SQL Server database to your PostgreSQL database.
SyncHub syncs data from your cloud services into a SQL Azure database,
and this script copies that data to your own PostgreSQL database on a schedule.
Existing tables are dropped and recreated with fresh data on each sync.
"""

import os
import sys
import logging
import time
import re
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/04_synchub_to_db.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def sanitize_table_name(schema: str, table: str, prefix: str = "") -> str:
    """
    Sanitize SQL Server table names to be valid PostgreSQL identifiers
    
    PostgreSQL rules:
    - Must start with letter or underscore
    - Can contain letters, digits, underscores
    - Max 63 characters
    - Best practice: lowercase
    """
    # Combine schema and table
    if prefix:
        full_name = f"{prefix}{schema}_{table}"
    else:
        full_name = f"{schema}_{table}"
    
    # Convert to lowercase
    full_name = full_name.lower()
    
    # Replace problematic characters with underscore
    full_name = re.sub(r'[\s\-\.\(\)\[\]\{\}/\\]', '_', full_name)
    
    # Remove any remaining special characters except underscore
    full_name = re.sub(r'[^a-z0-9_]', '', full_name)
    
    # Remove consecutive underscores
    full_name = re.sub(r'_+', '_', full_name)
    
    # Ensure starts with letter or underscore
    if full_name and full_name[0].isdigit():
        full_name = f"t_{full_name}"
    
    # Trim to 63 characters (PostgreSQL limit)
    if len(full_name) > 63:
        # Keep first 40 and last 20 for uniqueness
        full_name = f"{full_name[:40]}_{full_name[-20:]}"
        full_name = full_name[:63]
    
    # Remove trailing underscores
    full_name = full_name.rstrip('_')
    
    return full_name


class SyncHubSource:
    """Client for reading data from SyncHub's SQL Server database"""
    
    def __init__(self, server: str, database: str, username: str, password: str):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.conn = None
    
    def connect(self):
        """Establish database connection to SQL Server"""
        connection_string = (
            f'DRIVER={{ODBC Driver 18 for SQL Server}};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'UID={self.username};'
            f'PWD={self.password};'
            f'Encrypt=yes;'
            f'TrustServerCertificate=yes;'  # Required for Azure SQL connections
            f'Connection Timeout=30;'
        )
        
        try:
            self.conn = pyodbc.connect(connection_string)
            logger.info("Successfully connected to SyncHub SQL Server database")
        except pyodbc.Error as e:
            logger.error(f"Error connecting to SyncHub database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from SyncHub database")
    
    def get_schemas(self) -> List[str]:
        """Get list of non-system schemas"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT name 
            FROM sys.schemas 
            WHERE name NOT IN ('sys', 'guest', 'INFORMATION_SCHEMA', 'db_owner', 
                               'db_accessadmin', 'db_securityadmin', 'db_ddladmin',
                               'db_backupoperator', 'db_datareader', 'db_datawriter',
                               'db_denydatareader', 'db_denydatawriter')
            ORDER BY name
        """)
        
        schemas = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return schemas
    
    def get_tables(self, schemas: Optional[List[str]] = None) -> List[Dict[str, str]]:
        """
        Get list of tables from SyncHub database
        
        Args:
            schemas: Optional list of schema names to filter
            
        Returns:
            List of dicts with 'schema' and 'table' keys
        """
        cursor = self.conn.cursor()
        
        if schemas:
            schema_placeholders = ','.join(['?' for _ in schemas])
            query = f"""
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' 
                AND TABLE_SCHEMA IN ({schema_placeholders})
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
            tables.append({'schema': row[0], 'table': row[1]})
        
        cursor.close()
        logger.info(f"Found {len(tables)} tables in SyncHub database")
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
        cursor.execute("""
            SELECT 
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
        """, (schema, table))
        
        columns = []
        for row in cursor.fetchall():
            columns.append({
                'name': row[0],
                'data_type': row[1],
                'max_length': row[2],
                'numeric_precision': row[3],
                'numeric_scale': row[4],
                'nullable': row[5] == 'YES',
                'default': row[6]
            })
        
        cursor.close()
        return columns


class PostgresDestination:
    """Handle PostgreSQL destination database synchronization"""
    
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
    
    def map_sql_server_type_to_postgres(self, col: Dict[str, Any]) -> str:
        """Map SQL Server data types to PostgreSQL types"""
        data_type = col['data_type'].lower()
        
        # String types
        if data_type in ('varchar', 'nvarchar'):
            if col['max_length'] and col['max_length'] > 0:
                return f"VARCHAR({col['max_length']})"
            return "TEXT"
        elif data_type in ('char', 'nchar'):
            if col['max_length'] and col['max_length'] > 0:
                return f"CHAR({col['max_length']})"
            return "CHAR(1)"
        elif data_type in ('text', 'ntext'):
            return "TEXT"
        
        # Numeric types
        elif data_type in ('int', 'integer'):
            return "INTEGER"
        elif data_type == 'bigint':
            return "BIGINT"
        elif data_type == 'smallint':
            return "SMALLINT"
        elif data_type == 'tinyint':
            return "SMALLINT"
        elif data_type == 'bit':
            return "BOOLEAN"
        elif data_type in ('decimal', 'numeric'):
            precision = col.get('numeric_precision', 18)
            scale = col.get('numeric_scale', 0)
            return f"NUMERIC({precision},{scale})"
        elif data_type in ('float', 'real'):
            return "DOUBLE PRECISION"
        elif data_type == 'money':
            return "NUMERIC(19,4)"
        elif data_type == 'smallmoney':
            return "NUMERIC(10,4)"
        
        # Date/Time types
        elif data_type in ('datetime', 'datetime2', 'smalldatetime'):
            return "TIMESTAMP"
        elif data_type == 'date':
            return "DATE"
        elif data_type == 'time':
            return "TIME"
        elif data_type == 'datetimeoffset':
            return "TIMESTAMP WITH TIME ZONE"
        
        # Binary types
        elif data_type in ('binary', 'varbinary', 'image'):
            return "BYTEA"
        
        # GUID
        elif data_type == 'uniqueidentifier':
            return "UUID"
        
        # XML/JSON
        elif data_type == 'xml':
            return "XML"
        
        # Default to TEXT for unknown types
        else:
            logger.warning(f"Unknown SQL Server type '{data_type}', using TEXT")
            return "TEXT"
    
    def create_table(self, table_name: str, columns: List[Dict[str, Any]]):
        """Create a table with specified columns"""
        if not columns:
            logger.warning(f"No columns provided for table {table_name}")
            return
        
        col_definitions = []
        for col in columns:
            pg_type = self.map_sql_server_type_to_postgres(col)
            
            # Use sql.Identifier for column name, then add type
            col_def = sql.SQL("{} {}").format(
                sql.Identifier(col['name']),
                sql.SQL(pg_type)
            )
            
            # Add NOT NULL constraint
            if not col.get('nullable', True):
                col_def = sql.SQL("{} NOT NULL").format(col_def)
            
            col_definitions.append(col_def)
        
        create_sql = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(col_definitions)
        )
        
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(create_sql)
                logger.info(f"Created table: {table_name}")
            except psycopg2.Error as e:
                logger.error(f"Error creating table {table_name}: {e}")
                raise
    
    def insert_data(self, table_name: str, columns: List[str], data: List[tuple]):
        """Insert data into a table"""
        if not data:
            logger.warning(f"No data to insert into {table_name}")
            return
        
        # Quote column names to preserve case and handle special characters
        column_identifiers = [sql.Identifier(col) for col in columns]
        
        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(column_identifiers)
        )
        
        with self.conn.cursor() as cursor:
            try:
                execute_values(cursor, insert_sql, data)
                logger.info(f"Inserted {len(data)} rows into {table_name}")
            except psycopg2.Error as e:
                logger.error(f"Error inserting data into {table_name}: {e}")
                raise
    
    def sync_table(self, source_schema: str, source_table: str, 
                   dest_table: str, columns: List[Dict[str, Any]], data: List[tuple]):
        """Drop, recreate, and populate a table with new data"""
        try:
            # Normalize table name for PostgreSQL compatibility
            dest_table = self._normalize_table_name(dest_table)
            logger.info(f"Syncing {source_schema}.{source_table} -> {dest_table}")
            
            self.drop_table(dest_table)
            self.create_table(dest_table, columns)
            
            if data:
                column_names = [col['name'] for col in columns]
                self.insert_data(dest_table, column_names, data)
            else:
                logger.warning(f"No data to sync for {dest_table}")
            
            self.conn.commit()
            logger.info(f"Successfully synced table: {dest_table} ({len(data)} rows)")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to sync table {dest_table}: {e}")
            raise
    
    def _normalize_table_name(self, table_name: str) -> str:
        """
        Normalize table name for PostgreSQL compatibility
        - Convert to lowercase to avoid quoting issues
        - Truncate to 63 characters (PostgreSQL limit)
        - Replace invalid characters
        
        Args:
            table_name: Original table name
            
        Returns:
            Normalized table name
        """
        # Convert to lowercase
        normalized = table_name.lower()
        
        # Replace invalid characters with underscore
        normalized = ''.join(c if c.isalnum() or c == '_' else '_' for c in normalized)
        
        # Remove consecutive underscores
        while '__' in normalized:
            normalized = normalized.replace('__', '_')
        
        # Strip leading/trailing underscores
        normalized = normalized.strip('_')
        
        # Truncate to 63 characters (PostgreSQL limit)
        if len(normalized) > 63:
            # Keep a hash of the original name to avoid collisions
            import hashlib
            hash_suffix = hashlib.md5(table_name.encode()).hexdigest()[:8]
            # Take first 54 chars + underscore + 8 char hash = 63 chars
            normalized = normalized[:54] + '_' + hash_suffix
            logger.warning(f"Table name '{table_name}' exceeds 63 chars, truncating to '{normalized}'")
        
        return normalized


def sync_data(synchub_source: SyncHubSource, postgres_dest: PostgresDestination, 
              schemas: Optional[List[str]] = None, table_prefix: str = ""):
    """Main synchronization function"""
    logger.info("=" * 80)
    logger.info(f"Starting data synchronization at {datetime.now()}")
    logger.info("=" * 80)
    
    try:
        # Connect to both databases
        synchub_source.connect()
        postgres_dest.connect()
        
        # Get list of tables to sync
        tables = synchub_source.get_tables(schemas)
        
        if not tables:
            logger.warning("No tables found to sync")
            return
        
        # Sync each table
        success_count = 0
        error_count = 0
        
        for table_info in tables:
            source_schema = table_info['schema']
            source_table = table_info['table']
            
            # Sanitize table name for PostgreSQL
            dest_table = sanitize_table_name(source_schema, source_table, table_prefix)
            
            # Log the mapping
            logger.info(f"Mapping: {source_schema}.{source_table} -> {dest_table}")
            
            try:
                # Get table structure and data
                columns = synchub_source.get_table_columns(source_schema, source_table)
                data = synchub_source.get_table_data(source_schema, source_table)
                
                # Sync to destination
                postgres_dest.sync_table(source_schema, source_table, dest_table, columns, data)
                success_count += 1
                
            except Exception as e:
                logger.error(f"Error syncing {source_schema}.{source_table}: {e}")
                error_count += 1
                # Continue with other tables
                continue
        
        logger.info("=" * 80)
        logger.info(f"Synchronization completed at {datetime.now()}")
        logger.info(f"Success: {success_count} tables, Errors: {error_count} tables")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Synchronization failed: {e}")
        raise
    finally:
        synchub_source.disconnect()
        postgres_dest.disconnect()


def main():
    """Main entry point"""
    
    # Configuration from environment variables
    SYNCHUB_SERVER = os.getenv('SYNCHUB_SERVER')  # e.g., server.database.windows.net
    SYNCHUB_DATABASE = os.getenv('SYNCHUB_DATABASE')
    SYNCHUB_USERNAME = os.getenv('SYNCHUB_USERNAME')
    SYNCHUB_PASSWORD = os.getenv('SYNCHUB_PASSWORD')
    
    POSTGRES_CONNECTION = os.getenv('POSTGRES_CONNECTION')
    SYNC_SCHEDULE = os.getenv('SYNC_SCHEDULE', '0 2 * * *')
    TABLE_PREFIX = os.getenv('TABLE_PREFIX', 'synchub_')
    
    # Optional: Specify which schemas to sync (comma-separated)
    SCHEMAS_TO_SYNC = os.getenv('SCHEMAS_TO_SYNC', '').split(',') if os.getenv('SCHEMAS_TO_SYNC') else None
    if SCHEMAS_TO_SYNC:
        SCHEMAS_TO_SYNC = [s.strip() for s in SCHEMAS_TO_SYNC if s.strip()]
    
    # Validate configuration
    if not all([SYNCHUB_SERVER, SYNCHUB_DATABASE, SYNCHUB_USERNAME, SYNCHUB_PASSWORD]):
        logger.error("SyncHub credentials are required (SYNCHUB_SERVER, SYNCHUB_DATABASE, SYNCHUB_USERNAME, SYNCHUB_PASSWORD)")
        sys.exit(1)
    
    if not POSTGRES_CONNECTION:
        logger.error("POSTGRES_CONNECTION environment variable is required")
        sys.exit(1)
    
    # Initialize clients
    synchub_source = SyncHubSource(SYNCHUB_SERVER, SYNCHUB_DATABASE, SYNCHUB_USERNAME, SYNCHUB_PASSWORD)
    postgres_dest = PostgresDestination(POSTGRES_CONNECTION)
    
    # Parse schedule format
    schedule_parts = SYNC_SCHEDULE.split()
    
    if len(schedule_parts) == 1 and schedule_parts[0].endswith(('m', 'h', 'd')):
        # Simple interval format
        interval = schedule_parts[0]
        if interval.endswith('m'):
            minutes = int(interval[:-1])
            schedule.every(minutes).minutes.do(
                sync_data, synchub_source, postgres_dest, SCHEMAS_TO_SYNC, TABLE_PREFIX
            )
            logger.info(f"Scheduled sync every {minutes} minutes")
        elif interval.endswith('h'):
            hours = int(interval[:-1])
            schedule.every(hours).hours.do(
                sync_data, synchub_source, postgres_dest, SCHEMAS_TO_SYNC, TABLE_PREFIX
            )
            logger.info(f"Scheduled sync every {hours} hours")
        elif interval.endswith('d'):
            days = int(interval[:-1])
            schedule.every(days).days.do(
                sync_data, synchub_source, postgres_dest, SCHEMAS_TO_SYNC, TABLE_PREFIX
            )
            logger.info(f"Scheduled sync every {days} days")
    else:
        # Daily at specific time
        schedule.every().day.at("02:00").do(
            sync_data, synchub_source, postgres_dest, SCHEMAS_TO_SYNC, TABLE_PREFIX
        )
        logger.info("Scheduled sync daily at 02:00")
    
    # Run initial sync
    logger.info("Running initial synchronization...")
    sync_data(synchub_source, postgres_dest, SCHEMAS_TO_SYNC, TABLE_PREFIX)
    
    # TODO: Use a loop to run pending scheduled tasks
    # Start scheduled execution
    # logger.info("Starting scheduled execution. Press Ctrl+C to stop.")
    
    # try:
    #     while True:
            # schedule.run_pending()
    #         time.sleep(60)
    # except KeyboardInterrupt:
    #     logger.info("Shutting down scheduler...")
    #     sys.exit(0)


if __name__ == '__main__':
    main()