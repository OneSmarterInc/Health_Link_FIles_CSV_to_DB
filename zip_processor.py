import os
import zipfile
import tempfile
from file_processor import process_file

def process_zip_file(zip_path):
    try:
        print(f"📦 Extracting ZIP: {zip_path}")
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # Walk through and find .csv files
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    if file.endswith('.csv'):
                        full_path = os.path.join(root, file)
                        print(f"🔄 Processing CSV: {full_path}")
                        process_file(full_path)

        print(f"✅ Processed ZIP: {os.path.basename(zip_path)}")

    except Exception as e:
        print(f"❌ Failed to process ZIP {os.path.basename(zip_path)}: {e}")
        raise  # 🔁 Re-raise to let the caller handle retry or log
