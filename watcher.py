import os
import time
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from zip_processor import process_zip_file

# Base directories (For local)
# BASE_MEDIA_DIR = r"E:\CSV_DB_Saver_Script"

# for production
BASE_MEDIA_DIR = r"S:\HealthLinkFiles" 

# WATCHED_FOLDER = os.path.join(BASE_MEDIA_DIR, "zipped")
INSERTED_FOLDER = os.path.join(BASE_MEDIA_DIR, "inserted_folder")

# Create folders don't exist
# os.makedirs(WATCHED_FOLDER, exist_ok=True)
os.makedirs(INSERTED_FOLDER, exist_ok=True)

def wait_until_ready(file_path, timeout=30):
    """Waits until the ZIP file is fully written (no size change)."""
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
    filename = os.path.basename(zip_path)
    
    if not os.path.exists(zip_path):
        print(f"❌ File not found: {zip_path}")
        return

    try:
        print(f"📦 Detected ZIP: {filename}")
        if wait_until_ready(zip_path):
            process_zip_file(zip_path)  # Unzips + processes CSVs
            shutil.move(zip_path, os.path.join(INSERTED_FOLDER, filename))
            print(f"✅ Successfully processed and moved: {filename}")
        else:
            print(f"⚠️ Timeout waiting for file to finish writing: {filename}")
    except Exception as e:
        print(f"❌ Failed to process {filename}: {e}")


class ZipHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".zip"):
            safe_process(event.src_path)

if __name__ == "__main__":
    print("🔎 Scanning existing ZIPs in watched_folder...")
    for filename in os.listdir(WATCHED_FOLDER):
        if filename.endswith(".zip"):
            full_path = os.path.join(WATCHED_FOLDER, filename)
            safe_process(full_path)

    print(f"👀 Watching folder: {WATCHED_FOLDER}")
    observer = Observer()
    observer.schedule(ZipHandler(), WATCHED_FOLDER, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Interrupted by user.")
        observer.stop()
    observer.join()
