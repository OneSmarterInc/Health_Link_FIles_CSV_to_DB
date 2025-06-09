import os
import zipfile
import tempfile
from file_processor import process_file
from db_utils import get_db_connection 

def process_zip_file(zip_path):
    # First check DB connection before processing
    try:
        conn = get_db_connection()
        conn.close()
    except Exception as e:
        print(f"❌ Cannot connect to DB. Skipping ZIP file {zip_path}: {e}")
        return False  # Don't proceed or move the ZIP

    try:
        print(f"📦 Extracting ZIP: {zip_path}")
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # Walk through extracted files
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    if file.lower().endswith('.csv'):
                        full_path = os.path.join(root, file)
                        print(f"🔄 Processing CSV: {full_path}")
                        try:
                            process_file(full_path)
                        except Exception as e:
                            print(f"❌ Error processing file {full_path}: {e}")

        print("✅ ZIP processed")
        return True  # Always return True if ZIP was readable and DB was okay

    except Exception as e:
        print(f"❌ Failed to extract or read ZIP {zip_path}: {e}")
        return False
