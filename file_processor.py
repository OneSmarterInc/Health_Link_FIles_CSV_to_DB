import os
import pandas as pd
import re
import shutil
from db_utils import get_db_connection, ensure_table_and_columns

# Setup destination folder
INSERTED_FOLDER = os.path.join(os.getcwd(), "inserted_folder")
os.makedirs(INSERTED_FOLDER, exist_ok=True)

def extract_file_info(filename):
    match = re.match(r"\d+_(\w+)_([\d]{8})_\d+\.csv", filename)
    if match:
        return match.group(1), match.group(2)
    return None, None

def process_file(file_path, table_name="uploaded_data"):
    try:
        df = pd.read_csv(file_path)
        filename = os.path.basename(file_path)
        logical_name, filedate = extract_file_info(filename)

        if not logical_name:
            print(f"❌ Invalid file name format: {filename}")
            return

        df["filename"] = logical_name
        df["filedate"] = filedate

        conn = get_db_connection()
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

        # Move file to inserted folder after success
        dest_path = os.path.join(INSERTED_FOLDER, filename)
        shutil.move(file_path, dest_path)
        print(f"✅ Inserted and moved file to: {dest_path}")

    except Exception as e:
        print(f"❌ Error processing file {file_path}: {e}")
