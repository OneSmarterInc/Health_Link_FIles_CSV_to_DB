import os
import pandas as pd
import re
import shutil
from db_utils import get_db_connection, ensure_table_and_columns

# Base directories (For local)
# BASE_MEDIA_DIR = r"E:\CSV_DB_Saver_Script"

# for production
BASE_MEDIA_DIR = r"S:\HealthLinkFiles_InsertDB_Code\Health_Link_FIles_CSV_to_DB" 

# Folders for processed and failed CSVs
PROCESSED_CSVS = os.path.join(BASE_MEDIA_DIR, "processed_csvs")
FAILED_CSVS = os.path.join(BASE_MEDIA_DIR, "failed_csvs")

os.makedirs(PROCESSED_CSVS, exist_ok=True)
os.makedirs(FAILED_CSVS, exist_ok=True)

def extract_file_info(filename):
    match = re.match(r"\d+_(\w+)_([\d]{8})_\d+\.csv", filename)
    if match:
        return match.group(1), match.group(2)
    return None, None

def process_file(file_path, table_name="uploaded_data"):
    filename = os.path.basename(file_path)
    try:
        df = pd.read_csv(file_path)

        logical_name, filedate = extract_file_info(filename)
        if not logical_name:
            raise ValueError(f"Invalid file name format: {filename}")

        df["filename"] = logical_name
        df["filedate"] = filedate

        # DB connection
        conn = get_db_connection()
        if conn is None:
            raise ConnectionError("Failed to connect to the database.")

        cursor = conn.cursor()
        ensure_table_and_columns(cursor, table_name, df.columns)

        for _, row in df.iterrows():
            columns = ", ".join(f"[{col}]" for col in df.columns)
            placeholders = ", ".join("?" for _ in df.columns)
            values = [str(val) if pd.notnull(val) else None for val in row]
            cursor.execute(
                f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
                values
            )

        conn.commit()
        cursor.close()
        conn.close()

        # Move to processed folder
        dest_path = os.path.join(PROCESSED_CSVS, filename)
        shutil.move(file_path, dest_path)
        print(f"✅ Inserted and moved file to: {dest_path}")

    except Exception as e:
        print(f"❌ Error processing file {file_path}: {e}")

        # Move to failed folder
        failed_path = os.path.join(FAILED_CSVS, filename)
        if os.path.exists(file_path):  # Only move if file still exists
            shutil.move(file_path, failed_path)
            print(f"⚠️ Moved failed file to: {failed_path}")
