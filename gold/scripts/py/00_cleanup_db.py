#!/usr/bin/env python3
"""
Cleanup script to remove user-defined views, materialized views, and tables
from the PostgreSQL database.

Usage:
  # Drop everything (views + matviews + tables), prompted for confirmation
  python 00_cleanup_db.py

  # Drop only views and materialized views (keep tables and SCD2 history)
  python 00_cleanup_db.py --views --matviews

  # Drop only materialized views (rebuild matview layer without touching source)
  python 00_cleanup_db.py --matviews

  # Drop only tables (will cascade-drop views that depend on them)
  python 00_cleanup_db.py --tables

  # Skip confirmation prompt (for automation)
  python 00_cleanup_db.py --views --matviews --yes

If no scope flag is given, the script drops everything (preserves prior behaviour).
Use with caution — operations are irreversible.
"""

import argparse
import os
import sys
import logging
from typing import List
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/00_cleanup_db.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def get_connection():
    """Establish connection to PostgreSQL database."""
    try:
        conn_str = os.getenv("POSTGRES_CONNECTION")
        if not conn_str:
            raise ValueError("POSTGRES_CONNECTION environment variable not set")
        conn = psycopg2.connect(conn_str)
        conn.autocommit = True  # Enable autocommit for DDL operations
        logger.info("Connected to PostgreSQL database")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        sys.exit(1)


def get_views(conn) -> List[str]:
    """Get list of all user-defined views in the database."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT schemaname, viewname
                FROM pg_views
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schemaname, viewname
            """)
            views = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
            logger.info(f"Found {len(views)} views to drop")
            return views
    except psycopg2.Error as e:
        logger.error(f"Failed to retrieve views: {e}")
        return []


def get_tables(conn) -> List[str]:
    """Get list of all user-defined tables in the database."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schemaname, tablename
            """)
            tables = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
            logger.info(f"Found {len(tables)} tables to drop")
            return tables
    except psycopg2.Error as e:
        logger.error(f"Failed to retrieve tables: {e}")
        return []


def get_matviews(conn) -> List[str]:
    """Get list of all user-defined materialized views in the database.

    pg_views and pg_tables do not list materialized views, so self-contained MVs
    (built from VALUES/unnest with no raw-table dependency) survive table-CASCADE
    drops. Querying pg_matviews ensures they get cleaned up too.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT schemaname, matviewname
                FROM pg_matviews
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schemaname, matviewname
            """)
            matviews = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
            logger.info(f"Found {len(matviews)} materialized views to drop")
            return matviews
    except psycopg2.Error as e:
        logger.error(f"Failed to retrieve materialized views: {e}")
        return []


def drop_objects(conn, objects: List[str], object_type: str):
    """Drop a list of database objects (views or tables)."""
    for obj in objects:
        try:
            with conn.cursor() as cursor:
                query = sql.SQL("DROP {} IF EXISTS {} CASCADE").format(
                    sql.SQL(object_type.upper()), sql.Identifier(*obj.split("."))
                )
                cursor.execute(query)
                logger.info(f"Dropped {object_type}: {obj}")
        except psycopg2.Error as e:
            logger.error(f"Failed to drop {object_type} {obj}: {e}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="00_cleanup_db.py",
        description="Drop user-defined views, materialized views, and/or tables.",
    )
    # Scope flags — pick any combination. If none are passed, everything is dropped
    # (preserves the script's original behaviour for callers that don't know about
    # the flags).
    parser.add_argument("--views",    action="store_true", help="Drop regular views")
    parser.add_argument("--matviews", action="store_true", help="Drop materialized views")
    parser.add_argument("--tables",   action="store_true", help="Drop tables (cascades to dependent views)")
    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip the interactive confirmation prompt (for automation).",
    )
    return parser.parse_args()


def main():
    """Main function to perform database cleanup."""
    args = _parse_args()

    # If no scope flag is set, default to dropping everything (backward compatible).
    if not (args.views or args.matviews or args.tables):
        args.views = args.matviews = args.tables = True

    scope = [name for name, flag in
             [("views", args.views), ("materialized views", args.matviews), ("tables", args.tables)]
             if flag]
    logger.info(f"Starting database cleanup — scope: {', '.join(scope)}")

    # Confirm action unless --yes
    if not args.yes:
        confirm = input(
            f"This will drop ALL {', '.join(scope)} in the database. Are you sure? (yes/no): "
        )
        if confirm.lower() != "yes":
            logger.info("Cleanup cancelled by user")
            return

    conn = None
    try:
        conn = get_connection()

        # Drop regular views first (to avoid dependency issues)
        if args.views:
            views = get_views(conn)
            if views:
                drop_objects(conn, views, "view")

        # Drop materialized views — pg_views and pg_tables miss these, so
        # self-contained MVs survive table-CASCADE drops without this step.
        if args.matviews:
            matviews = get_matviews(conn)
            if matviews:
                drop_objects(conn, matviews, "materialized view")

        # Then drop tables (cascade will catch any remaining dependent views)
        if args.tables:
            tables = get_tables(conn)
            if tables:
                drop_objects(conn, tables, "table")

        logger.info("Database cleanup completed successfully")

    except Exception as e:
        logger.error(f"Unexpected error during cleanup: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()
