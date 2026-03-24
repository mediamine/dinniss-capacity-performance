#!/usr/bin/env python3
"""
Simple PostgreSQL connectivity check for the SyncHub sync destination.

Uses the same connection settings as `PostgresDestination` in `synchub-to-db.py`
to verify that the `POSTGRES_CONNECTION` string is valid and reachable.
"""

import os
import sys
import logging

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/01_test_postgres_connection.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def test_postgres_connection() -> None:
    """Test connecting to the PostgreSQL destination database."""
    conn_str = os.getenv("POSTGRES_CONNECTION")

    logger.info("Starting PostgreSQL connection test.")

    if not conn_str:
        logger.error("POSTGRES_CONNECTION environment variable is required.")
        sys.exit(1)

    logger.info("Attempting to connect to PostgreSQL with provided connection string.")

    try:
        with psycopg2.connect(conn_str) as conn:
            conn.autocommit = True
            logger.info("Successfully connected to PostgreSQL.")

            with conn.cursor() as cursor:
                # Run a lightweight test query to confirm everything works
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                logger.info("PostgreSQL version: %s", version)

    except psycopg2.Error as exc:
        logger.exception("Failed to connect to PostgreSQL: %s", exc)
        sys.exit(1)

    # TODO: Use connection pool for better handling
    # pool = psycopg2.pool.SimpleConnectionPool(1, 5, conn_str)
    # if pool:
    #     conn = pool.getconn()
    #     try:
    #         conn.autocommit = True
    #         with conn.cursor() as cursor:
    #             # More comprehensive test
    #             cursor.execute("SELECT version(), current_user, current_database()")
    #             version, user, db = cursor.fetchone()
    #             logger.info("PostgreSQL version: %s, User: %s, Database: %s", version, user, db)
    #     finally:
    #         pool.putconn(conn)
    #         pool.closeall()
    # else:
    #     logger.error("Failed to create connection pool.")
    #     sys.exit(1)


if __name__ == "__main__":
    test_postgres_connection()
