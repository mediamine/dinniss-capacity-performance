#!/usr/bin/env python3
"""
Post-Sync SQL Script Runner

This module runs SQL scripts after data sync completes.
Use it to create views, indexes, materialized views, or run any post-processing SQL.
"""

import os
import sys
import logging
from typing import List, Optional

# import sqlparse  # For more robust SQL splitting if needed
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/06_sql_script_runner.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class SQLScriptRunner:
    """Execute SQL scripts after data synchronization"""

    def __init__(self, connection_string: str):
        """
        Initialize SQL script runner

        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self.conn = None

    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.conn.autocommit = False
            logger.info("SQL Script Runner connected to PostgreSQL")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("SQL Script Runner disconnected from PostgreSQL")

    def run_sql_file(self, file_path: str, continue_on_error: bool = False) -> bool:
        """
        Execute SQL from a file

        Args:
            file_path: Path to SQL file
            continue_on_error: If True, continue executing even if errors occur

        Returns:
            True if successful, False if errors occurred
        """
        if not os.path.exists(file_path):
            logger.error(f"SQL file not found: {file_path}")
            return False

        logger.info(f"Executing SQL file: {file_path}")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                sql_content = f.read()

            return self.run_sql_script(sql_content, file_path, continue_on_error)

        except Exception as e:
            logger.error(f"Error reading SQL file {file_path}: {e}")
            return False

    def run_sql_script(
        self,
        sql_content: str,
        script_name: str = "inline",
        continue_on_error: bool = False,
    ) -> bool:
        """
        Execute SQL script content

        Args:
            sql_content: SQL script as string
            script_name: Name for logging purposes
            continue_on_error: If True, continue executing even if errors occur

        Returns:
            True if successful, False if errors occurred
        """
        # TODO: Use sqlparse for more robust splitting if needed
        # Parse statements properly
        # statements = sqlparse.split(sql_content)

        # Add dry-run option
        # def validate_sql(self, sql_content: str) -> bool:
        #     try:
        #         parsed = sqlparse.parse(sql_content)
        #         return True
        #     except Exception as e:
        #         logger.error(f"SQL validation failed: {e}")
        #         return False

        # Split by semicolons to get individual statements
        # Note: This simple split won't handle semicolons in strings correctly
        # For production, consider using a proper SQL parser
        statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

        logger.info(f"Executing {len(statements)} SQL statements from {script_name}")

        success = True
        executed = 0
        failed = 0

        with self.conn.cursor() as cursor:
            for i, statement in enumerate(statements, 1):
                try:
                    logger.debug(f"Executing statement {i}/{len(statements)}")
                    cursor.execute(statement)
                    executed += 1

                except psycopg2.Error as e:
                    failed += 1
                    logger.error(f"Error in statement {i}: {e}")
                    logger.error(f"Statement: {statement[:200]}...")

                    if continue_on_error:
                        self.conn.rollback()
                        logger.warning("Continuing despite error...")
                        success = False
                    else:
                        self.conn.rollback()
                        logger.error("Aborting due to error")
                        return False

        # Commit if we got here
        try:
            self.conn.commit()
            logger.info(
                f"SQL script {script_name} completed: {executed} successful, {failed} failed"
            )
            return success
        except psycopg2.Error as e:
            logger.error(f"Error committing transaction: {e}")
            self.conn.rollback()
            return False

    def run_multiple_files(
        self, file_paths: List[str], continue_on_error: bool = False
    ) -> bool:
        """
        Execute multiple SQL files in order

        Args:
            file_paths: List of paths to SQL files
            continue_on_error: If True, continue to next file even if current fails

        Returns:
            True if all successful, False if any errors occurred
        """
        logger.info(f"Executing {len(file_paths)} SQL files")

        all_success = True
        for file_path in file_paths:
            success = self.run_sql_file(file_path, continue_on_error)
            if not success:
                all_success = False
                if not continue_on_error:
                    logger.error(f"Stopping execution due to error in {file_path}")
                    return False

        return all_success

    def run_sql_directory(
        self,
        directory_path: str,
        pattern: str = "*.sql",
        continue_on_error: bool = False,
    ) -> bool:
        """
        Execute all SQL files in a directory

        Args:
            directory_path: Path to directory containing SQL files
            pattern: File pattern to match (default: *.sql)
            continue_on_error: If True, continue even if files fail

        Returns:
            True if all successful, False if any errors occurred
        """
        import glob

        if not os.path.isdir(directory_path):
            logger.error(f"Directory not found: {directory_path}")
            return False

        # Find all matching SQL files
        search_pattern = os.path.join(directory_path, pattern)
        sql_files = sorted(glob.glob(search_pattern))

        if not sql_files:
            logger.warning(f"No SQL files found matching {search_pattern}")
            return True

        logger.info(f"Found {len(sql_files)} SQL files in {directory_path}")

        return self.run_multiple_files(sql_files, continue_on_error)

    def create_view_from_query(
        self,
        view_name: str,
        query: str,
        replace: bool = True,
        materialized: bool = False,
    ) -> bool:
        """
        Create a view or materialized view from a query

        Args:
            view_name: Name of the view to create
            query: SELECT query for the view
            replace: If True, use CREATE OR REPLACE (regular views only)
            materialized: If True, create materialized view

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.conn.cursor() as cursor:
                if materialized:
                    # Materialized views don't support OR REPLACE
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                    create_sql = f"CREATE MATERIALIZED VIEW {view_name} AS {query}"
                else:
                    if replace:
                        create_sql = f"CREATE OR REPLACE VIEW {view_name} AS {query}"
                    else:
                        create_sql = f"CREATE VIEW {view_name} AS {query}"

                logger.info(
                    f"Creating {'materialized ' if materialized else ''}view: {view_name}"
                )
                cursor.execute(create_sql)
                self.conn.commit()
                logger.info(
                    f"Successfully created {'materialized ' if materialized else ''}view: {view_name}"
                )
                return True

        except psycopg2.Error as e:
            logger.error(f"Error creating view {view_name}: {e}")
            self.conn.rollback()
            return False

    def refresh_materialized_view(
        self, view_name: str, concurrently: bool = False
    ) -> bool:
        """
        Refresh a materialized view

        Args:
            view_name: Name of the materialized view
            concurrently: If True, refresh concurrently (requires unique index)

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.conn.cursor() as cursor:
                refresh_sql = f"REFRESH MATERIALIZED VIEW {'CONCURRENTLY ' if concurrently else ''}{view_name}"
                logger.info(f"Refreshing materialized view: {view_name}")
                cursor.execute(refresh_sql)
                self.conn.commit()
                logger.info(f"Successfully refreshed materialized view: {view_name}")
                return True

        except psycopg2.Error as e:
            logger.error(f"Error refreshing materialized view {view_name}: {e}")
            self.conn.rollback()
            return False


def run_post_sync_sql(
    connection_string: str,
    sql_files: Optional[List[str]] = None,
    sql_directory: Optional[str] = None,
    refresh_materialized_views: Optional[List[str]] = None,
) -> bool:
    """
    Convenience function to run post-sync SQL operations

    Args:
        connection_string: PostgreSQL connection string
        sql_files: List of SQL file paths to execute
        sql_directory: Directory containing SQL files to execute
        refresh_materialized_views: List of materialized view names to refresh

    Returns:
        True if all operations successful, False otherwise
    """
    runner = SQLScriptRunner(connection_string)

    try:
        runner.connect()

        all_success = True

        # Run individual SQL files
        if sql_files:
            logger.info("Executing individual SQL files")
            for sql_file in sql_files:
                if not runner.run_sql_file(sql_file, continue_on_error=False):
                    all_success = False
                    break

        # Run SQL directory
        if sql_directory and all_success:
            logger.info(f"Executing SQL files from directory: {sql_directory}")
            if not runner.run_sql_directory(sql_directory, continue_on_error=False):
                all_success = False

        # Refresh materialized views
        if refresh_materialized_views and all_success:
            logger.info("Refreshing materialized views")
            for view_name in refresh_materialized_views:
                if not runner.refresh_materialized_view(view_name):
                    all_success = False
                    break

        return all_success

    finally:
        runner.disconnect()


# Example usage functions
def example_create_views():
    """Example: Create views after data sync"""

    connection_string = os.getenv("POSTGRES_CONNECTION")
    runner = SQLScriptRunner(connection_string)

    try:
        runner.connect()

        # Example 1: Create a simple view
        # runner.create_view_from_query(
        #     view_name='vw_active_customers',
        #     query='''
        #         SELECT customer_id, customer_name, email, created_date
        #         FROM excel_customers
        #         WHERE status = 'Active'
        #         ORDER BY created_date DESC
        #     '''
        # )

        # Example 2: Create a materialized view for reporting
        # runner.create_view_from_query(
        #     view_name='mvw_sales_summary',
        #     query='''
        #         SELECT
        #             DATE_TRUNC('month', sale_date) as month,
        #             SUM(amount) as total_sales,
        #             COUNT(*) as transaction_count,
        #             AVG(amount) as avg_sale
        #         FROM excel_sales
        #         GROUP BY DATE_TRUNC('month', sale_date)
        #     ''',
        #     materialized=True
        # )

        # Example 3: Run SQL file
        runner.run_sql_file("gold/scripts/sql/01_create_views.sql")

    finally:
        runner.disconnect()


if __name__ == "__main__":
    # This allows the module to be run standalone for testing
    example_create_views()
