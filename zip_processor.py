import os
import zipfile
import tempfile
from file_processor import process_file

def process_zip_file(zip_path):
    success = True  # Flag to check if all files processed successfully

    try:
        print(f"📦 Extracting ZIP: {zip_path}")
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # Walk through extracted files
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    if file.endswith('.csv'):
                        full_path = os.path.join(root, file)
                        print(f"🔄 Processing CSV: {full_path}")
                        try:
                            process_file(full_path)
                        except Exception as e:
                            print(f"❌ Error processing file {full_path}: {e}")
                            success = False

        # If all files processed, return True (let caller move the zip)
        if not success:
            raise Exception("One or more files failed to process.")

    except Exception as e:
        print(f"❌ Failed to process ZIP: {e}")
        raise  # Reraise so the watcher doesn't move it
