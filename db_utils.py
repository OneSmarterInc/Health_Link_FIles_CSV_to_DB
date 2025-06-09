import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    driver = os.getenv("DB_DRIVER")
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    trusted_connection = os.getenv("DB_TRUSTED_CONNECTION", "yes")

    if not all([driver, server, database]):
        print("❌ Missing one or more DB config variables in .env")
        return None

    # connection_str = (
    #     f"DRIVER={driver};"
    #     f"SERVER={server};"
    #     f"DATABASE={database};"
    #     f"Trusted_Connection={trusted_connection};"
    # )
    
    # For Production==>
    connection_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=ABCCOLUMBUSSQL2;'
        'DATABASE=HealthLinkFilesDb;'
        'UID=sa;'
        'PWD=ChangeMe#2024;'
        )

    try:
        conn = pyodbc.connect(connection_str)
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return None


def ensure_table_and_columns(cursor, table_name, columns):
    # Ensure table exists
    cursor.execute(f"""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ?
        )
        BEGIN
            EXEC('CREATE TABLE [{0}] (id INT IDENTITY(1,1) PRIMARY KEY)')
        END
    """.format(table_name), table_name)

    # Get existing columns
    cursor.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = ?
    """, table_name)

    existing_cols = {row[0] for row in cursor.fetchall()}

    # Add any missing columns
    for col in columns:
        if col not in existing_cols:
            cursor.execute(f'ALTER TABLE [{table_name}] ADD [{col}] NVARCHAR(MAX)')
