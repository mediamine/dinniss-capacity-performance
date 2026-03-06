#!/usr/bin/env python3
"""
Simple SyncHub SQL Server connectivity check.

Uses the same connection settings as `SyncHubSource` in `synchub-to-db.py`
to verify that your environment variables and network access are correct.
"""

import os
import sys
import logging
import pyodbc
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/02_test_synchub_connection.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def test_synchub_connection() -> None:
    """Test connecting to the SyncHub SQL Server database."""
    server = os.getenv("SYNCHUB_SERVER")
    database = os.getenv("SYNCHUB_DATABASE")
    username = os.getenv("SYNCHUB_USERNAME")
    password = os.getenv("SYNCHUB_PASSWORD")

    logger.info("Starting SyncHub connection test.")

    if not all([server, database, username, password]):
        logger.error(
            "SyncHub credentials are required "
            "(SYNCHUB_SERVER, SYNCHUB_DATABASE, SYNCHUB_USERNAME, SYNCHUB_PASSWORD)."
        )
        sys.exit(1)

    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )

    logger.info("Attempting to connect to SyncHub SQL Server at %s.", server)

    try:
        # Check ODBC driver
        drivers = pyodbc.drivers()
        if "ODBC Driver 18 for SQL Server" not in drivers:
            logger.error("ODBC Driver 18 for SQL Server not found. Install it from Microsoft.")
            sys.exit(1)

        with pyodbc.connect(connection_string, timeout=10) as conn:
            logger.info("Successfully connected to SyncHub SQL Server.")

            with conn.cursor() as cursor:
                # Run a lightweight test query to confirm everything works
                cursor.execute("SELECT TOP 1 name FROM sys.tables")
                row = cursor.fetchone()
                if row:
                    logger.info("Test query succeeded. Example table: %s", row[0])
                else:
                    logger.info(
                        "Test query returned no rows, but the connection is valid."
                    )

    except pyodbc.Error as exc:
        logger.exception("Failed to connect to SyncHub SQL Server: %s", exc)
        sys.exit(1)

    # Add warning for SSL bypass
    logger.warning("TrustServerCertificate=yes is used; enable certificate validation in production for security.")
    
    # TODO: Add retry logic
    # max_retries = 3
    # repeat the above connection logic with retries if needed (not implemented here for brevity)


if __name__ == "__main__":
    test_synchub_connection()

