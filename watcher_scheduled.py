import os
import time
import shutil
import schedule
from datetime import datetime
from zip_processor import process_zip_file

# Base directories (For local)
# BASE_MEDIA_DIR = r"E:\CSV_DB_Saver_Script"

# for production
BASE_MEDIA_DIR = r"S:\HealthLinkFiles_InsertDB_Code\Health_Link_FIles_CSV_to_DB" 

WATCHED_FOLDER = os.path.join(BASE_MEDIA_DIR, "watched_folder")
INSERTED_FOLDER = os.path.join(BASE_MEDIA_DIR, "inserted_folder")

# Create folders if they don't exist
os.makedirs(WATCHED_FOLDER, exist_ok=True)
os.makedirs(INSERTED_FOLDER, exist_ok=True)

retry_times = []  # ["21:00", "22:00"] 

def wait_until_ready(file_path, timeout=30):
    """Wait until the ZIP file is fully written (i.e., file size doesn't change)."""
    last_size = -1
    for _ in range(timeout):
        try:
            current_size = os.path.getsize(file_path)
            if current_size == last_size:
                return True
            last_size = current_size
        except FileNotFoundError:
            pass
        time.sleep(1)
    return False

def safe_process(zip_path):
    """Process a ZIP file if it's fully written. Move it if processing succeeds."""
    filename = os.path.basename(zip_path)

    if not os.path.exists(zip_path):
        print(f"❌ File not found: {zip_path}")
        return False

    try:
        print(f"📦 Detected ZIP: {filename}")
        if wait_until_ready(zip_path):
            success = process_zip_file(zip_path)
            if success:
                shutil.move(zip_path, os.path.join(INSERTED_FOLDER, filename))
                print(f"✅ Successfully processed and moved: {filename}")
                return True
            else:
                print(f"❌ Failed to process ZIP: {filename}. It was not moved.")
                return False
        else:
            print(f"⚠️ Timeout waiting for file to finish writing: {filename}")
            return False
    except Exception as e:
        print(f"❌ Failed to process {filename}: {e}")
        return False

def check_and_process_zips():
    """Scan the watched folder and process all ZIP files."""
    print(f"🕗 Checking ZIPs at {datetime.now().strftime('%H:%M')}")
    found = False
    for filename in os.listdir(WATCHED_FOLDER):
        if filename.endswith(".zip"):
            full_path = os.path.join(WATCHED_FOLDER, filename)
            if safe_process(full_path):
                found = True

    if not found:
        global retry_times
        retry_times = ["21:00", "22:00"]
        print("⚠️ No ZIPs successfully processed. Scheduling retries at 9:00 PM and 10:00 PM.")
    else:
        retry_times.clear()
        print("✅ All ZIPs processed. Next scheduled scan will continue as normal.")

def check_retry():
    """Run retry jobs if it's one of the retry times."""
    if retry_times:
        current_time = datetime.now().strftime("%H:%M")
        if current_time in retry_times:
            print(f"⏰ Running retry attempt scheduled at {current_time}")
            check_and_process_zips()
            retry_times.remove(current_time)

# Schedule jobs
schedule.every().day.at("12:40").do(check_and_process_zips)
schedule.every().minute.do(check_retry)  # Check each minute for retry matches

if __name__ == "__main__":
    print("📅 ZIP watcher with scheduled scanning started.")
    while True:
        schedule.run_pending()
        time.sleep(1)
