#!/usr/bin/env python3
"""
Cleanup script to remove all tables and views from the PostgreSQL database.
This script drops all user-defined views and tables in the database to start fresh.
Use with caution as this operation is irreversible.
"""

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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/00_cleanup_db.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def get_connection():
    """Establish connection to PostgreSQL database."""
    try:
        conn_str = os.getenv('POSTGRES_CONNECTION')
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

def drop_objects(conn, objects: List[str], object_type: str):
    """Drop a list of database objects (views or tables)."""
    for obj in objects:
        try:
            with conn.cursor() as cursor:
                query = sql.SQL("DROP {} IF EXISTS {} CASCADE").format(
                    sql.SQL(object_type.upper()),
                    sql.Identifier(*obj.split('.'))
                )
                cursor.execute(query)
                logger.info(f"Dropped {object_type}: {obj}")
        except psycopg2.Error as e:
            logger.error(f"Failed to drop {object_type} {obj}: {e}")

def main():
    """Main function to perform database cleanup."""
    logger.info("Starting database cleanup...")

    # Confirm action
    confirm = input("This will drop all tables and views in the database. Are you sure? (yes/no): ")
    if confirm.lower() != 'yes':
        logger.info("Cleanup cancelled by user")
        return

    conn = None
    try:
        conn = get_connection()

        # Drop views first (to avoid dependency issues)
        views = get_views(conn)
        if views:
            drop_objects(conn, views, "view")

        # Then drop tables
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