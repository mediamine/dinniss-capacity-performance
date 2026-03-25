#!/usr/bin/env python3
"""
Generate PostgreSQL Schema Documentation for Claude AI

Creates a human-readable text file with:
- Table structures (columns, types, constraints)
- View definitions
- Indexes
- Sample data (first few rows)
- Statistics

Perfect for sharing with Claude AI to get help with views and queries.
"""

import os
import sys
import logging
import psycopg2
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/generate_schema_docs.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

def generate_schema_documentation(connection_string: str, output_file: str = 'schema_documentation.txt'):
    """Generate comprehensive schema documentation"""
    logger.info("Starting schema documentation generation.")

    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    logger.info("Successfully connected to PostgreSQL. Database: %s", conn.info.dbname)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        # Header
        f.write("=" * 80 + "\n")
        f.write("PostgreSQL Database Schema Documentation\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Database: {conn.info.dbname}\n")
        f.write("=" * 80 + "\n\n")
        
        # Table of Contents
        f.write("TABLE OF CONTENTS\n")
        f.write("-" * 80 + "\n")
        f.write("1. Tables\n")
        f.write("2. Views\n")
        f.write("3. Indexes\n")
        f.write("4. Statistics\n")
        f.write("\n\n")
        
        # Get all tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        logger.info("Found %d tables to document.", len(tables))

        # Document each table
        f.write("=" * 80 + "\n")
        f.write("1. TABLES\n")
        f.write("=" * 80 + "\n\n")
        
        for table_name in tables:
            logger.info("Documenting table: %s", table_name)
            f.write(f"\nTable: {table_name}\n")
            f.write("-" * 80 + "\n")
            
            # Get columns
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            f.write("\nColumns:\n")
            f.write(f"{'Column Name':<30} {'Type':<20} {'Nullable':<10} {'Default':<20}\n")
            f.write("-" * 80 + "\n")
            
            for col in cursor.fetchall():
                col_name = col[0]
                data_type = col[1]
                
                # Format data type
                if col[2]:  # character_maximum_length
                    data_type += f"({col[2]})"
                elif col[3]:  # numeric_precision
                    if col[4]:  # numeric_scale
                        data_type += f"({col[3]},{col[4]})"
                    else:
                        data_type += f"({col[3]})"
                
                nullable = "YES" if col[5] == 'YES' else "NO"
                default = str(col[6])[:20] if col[6] else "-"
                
                f.write(f"{col_name:<30} {data_type:<20} {nullable:<10} {default:<20}\n")
            
            # Get primary key
            cursor.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = %s
                AND tc.constraint_type = 'PRIMARY KEY'
                ORDER BY kcu.ordinal_position
            """, (table_name,))
            
            pk_cols = [row[0] for row in cursor.fetchall()]
            if pk_cols:
                f.write(f"\nPrimary Key: {', '.join(pk_cols)}\n")
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            f.write(f"Row Count: {row_count:,}\n")
            
            # Get sample data (first 3 rows)
            if row_count > 0:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                sample_data = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description]
                
                f.write("\nSample Data (first 3 rows):\n")
                f.write(", ".join(col_names[:5]) + ("..." if len(col_names) > 5 else "") + "\n")
                for row in sample_data:
                    sample_row = [str(v)[:20] if v is not None else "NULL" for v in row[:5]]
                    f.write(", ".join(sample_row) + ("..." if len(row) > 5 else "") + "\n")
            
            f.write("\n" + "=" * 80 + "\n")
        
        # Get all views
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        views = [row[0] for row in cursor.fetchall()]
        logger.info("Found %d views to document.", len(views))

        # Document each view
        f.write("\n\n")
        f.write("=" * 80 + "\n")
        f.write("2. VIEWS\n")
        f.write("=" * 80 + "\n\n")
        
        for view_name in views:
            logger.info("Documenting view: %s", view_name)
            f.write(f"\nView: {view_name}\n")
            f.write("-" * 80 + "\n")
            
            # Get view definition
            cursor.execute("""
                SELECT view_definition 
                FROM information_schema.views 
                WHERE table_name = %s
            """, (view_name,))
            
            view_def = cursor.fetchone()[0]
            f.write("\nDefinition:\n")
            f.write(view_def)
            f.write("\n\n")
            
            # Get columns in view
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (view_name,))
            
            f.write("Columns:\n")
            for col in cursor.fetchall():
                f.write(f"  - {col[0]} ({col[1]})\n")
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM \"{view_name}\"")
            row_count = cursor.fetchone()[0]
            f.write(f"\nRow Count: {row_count:,}\n")
            
            f.write("\n" + "=" * 80 + "\n")
        
        # Document indexes
        f.write("\n\n")
        f.write("=" * 80 + "\n")
        f.write("3. INDEXES\n")
        f.write("=" * 80 + "\n\n")
        
        cursor.execute("""
            SELECT 
                tablename,
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname
        """)
        
        current_table = None
        for idx in cursor.fetchall():
            if idx[0] != current_table:
                f.write(f"\nTable: {idx[0]}\n")
                f.write("-" * 80 + "\n")
                current_table = idx[0]
            
            f.write(f"  {idx[1]}\n")
            f.write(f"    {idx[2]}\n\n")
        
        # Statistics
        f.write("\n\n")
        f.write("=" * 80 + "\n")
        f.write("4. DATABASE STATISTICS\n")
        f.write("=" * 80 + "\n\n")
        
        # Table sizes
        cursor.execute("""
            SELECT 
                schemaname,
                relname,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as size,
                n_live_tup as row_count
            FROM pg_stat_user_tables
            WHERE schemaname = 'public'
            ORDER BY pg_total_relation_size(schemaname||'.'||relname) DESC
            LIMIT 20
        """)
        
        f.write("Table Sizes (Top 20):\n")
        f.write(f"{'Table Name':<40} {'Size':<15} {'Rows':<15}\n")
        f.write("-" * 80 + "\n")
        
        for row in cursor.fetchall():
            f.write(f"{row[1]:<40} {row[2]:<15} {row[3]:>14,}\n")
        
        # Summary
        f.write("\n\n")
        f.write("SUMMARY\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Tables: {len(tables)}\n")
        f.write(f"Total Views: {len(views)}\n")
        
        # Get database size
        cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
        db_size = cursor.fetchone()[0]
        f.write(f"Database Size: {db_size}\n")
    
    cursor.close()
    conn.close()

    logger.info(
        "Schema documentation generated: %s (%d tables, %d views documented).",
        output_file,
        len(tables),
        len(views),
    )


if __name__ == '__main__':
    # Configuration
    POSTGRES_CONNECTION = os.getenv('POSTGRES_CONNECTION')
    OUTPUT_FILE = os.getenv('OUTPUT_FILE', 'schema_documentation.txt')
    
    if not POSTGRES_CONNECTION:
        logger.error("POSTGRES_CONNECTION environment variable is required.")
        sys.exit(1)

    generate_schema_documentation(POSTGRES_CONNECTION, OUTPUT_FILE)
