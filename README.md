# Dinniss Capacity & Performance

This project includes a gold layer for data processing and storage using PostgreSQL. Below are the steps to set up the gold layer, including Python environment, ODBC driver, Docker-based PostgreSQL instance, and running the required scripts.

## Gold Layer Setup

### 1. Set Up Python Version 3.13

Ensure you have Python 3.13 installed. You can download it from the official Python website (https://www.python.org/downloads/) or use a version manager like pyenv or conda.

- **Windows**: Download the installer and follow the prompts.
- **macOS/Linux**: Use pyenv:
  ```bash
  pyenv install 3.13.0
  pyenv global 3.13.0
  ```
  Or use conda:
  ```bash
  conda create -n myenv python=3.13
  conda activate myenv
  ```
- Verify installation:
  ```bash
  python --version
  # Should output: Python 3.13.x
  ```

### 2. Install ODBC Driver

The project requires the Microsoft ODBC Driver for SQL Server to connect to SyncHub databases.

- **Windows**: Download and install from https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server.
- **macOS**: Use Homebrew:
  ```bash
  brew install msodbcsql18
  ```
- **Linux**: Follow the instructions at https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server.

Verify installation by checking available drivers in Python:
```python
import pyodbc
print(pyodbc.drivers())
```

### 3. Set Up PostgreSQL Instance Using Docker

Use Docker Compose to create a PostgreSQL instance in the gold layer.

1. Navigate to the gold/db directory:
   ```bash
   cd gold/db
   ```

2. Ensure Docker is installed and running. If not, download from https://www.docker.com/get-started.

3. Start the PostgreSQL container:
   ```bash
   docker-compose up -d
   ```

4. Verify the container is running:
   ```bash
   docker ps
   ```

5. The PostgreSQL instance will be available at `localhost:5432` with default credentials (check docker-compose.yml for details, typically user: postgres, password: postgres, database: postgres).

#### Alternative: Regular PostgreSQL Installation

If you prefer not to use Docker, install PostgreSQL directly:

1. **Windows**: Download and install from https://www.postgresql.org/download/windows/.
2. **macOS**: Use Homebrew:
   ```bash
   brew install postgresql
   brew services start postgresql
   ```
3. **Linux**: Use your package manager, e.g., Ubuntu:
   ```bash
   sudo apt update
   sudo apt install postgresql postgresql-contrib
   sudo systemctl start postgresql
   sudo systemctl enable postgresql
   ```

4. Create a database and user:
   ```bash
   sudo -u postgres psql
   CREATE DATABASE your_database;
   CREATE USER your_user WITH PASSWORD 'your_password';
   GRANT ALL PRIVILEGES ON DATABASE your_database TO your_user;
   \q
   ```

5. The instance will be available at `localhost:5432`.

### 4. Install Python Dependencies

1. Navigate to the gold directory:
   ```bash
   cd gold
   ```

2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

### 5. Run Python Scripts Sequentially

The gold layer includes Python scripts to test connections, sync data, and create tables/views. Run them in order using the provided runner script.

1. Ensure environment variables are set (create a `.env` file based on examples in gold/ for SyncHub and PostgreSQL connections).

2. Run all scripts at once using the runner script:
   ```bash
   python scripts/run_all_gold.py
   ```

   This will execute the following scripts in order:
   - `gold/scripts/py/01_test_synchub_connection.py`: Test SyncHub connection.
   - `gold/scripts/py/02_test_postgres_connection.py`: Test PostgreSQL connection.
   - `gold/scripts/py/03_test_sharepoint_connection.py`: Test SharePoint connection.
   - `gold/scripts/py/04_synchub_to_db.py`: Sync data from SyncHub to PostgreSQL.
   - `gold/scripts/py/05_sharepoint_to_db.py`: Sync data from SharePoint to PostgreSQL.
   - `gold/scripts/py/06_sql_script_runner.py`: Run SQL scripts to create views.

   **Cleanup Script**: To remove all tables and views and start fresh, run:
   ```bash
   python gold/scripts/py/07_cleanup_db.py
   ```
   This script will prompt for confirmation before dropping objects.

3. Monitor logs in the logs/ directory for any issues.

4. Verify tables and views in PostgreSQL:
   ```sql
   \dt  -- List tables
   \dv  -- List views
   ```
