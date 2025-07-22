import time
import cloudinary
import cloudinary.api
from datetime import datetime, timezone, timedelta
import json
import os
import re
from generate_caption_schedule import generate_caption
from supabase import create_client, Client
import logging

# === Logging Setup ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "New_scheduler.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# === Supabase Initialization ===
# with open(os.path.join(BASE_DIR, "config.json")) as f:
#     raw_config = json.load(f)
SUPABASE_URL = "https://rorltqhtdwvylyqpillg.supabase.co"
SUPABASE_KEY ="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJvcmx0cWh0ZHd2eWx5cXBpbGxnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5NjQxOTYsImV4cCI6MjA2NzU0MDE5Nn0.BeoonWsAXWwzZsnl3zcSVwAKOh5YKYAPvI2XHXW4Its"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_key(key_name):
    res = supabase.table("keys").select("key_value").eq("key_name", key_name).execute()
    return res.data[0]["key_value"] if res.data else None

CLOUD_NAME = get_key("CLOUD_NAME")
API_KEY = get_key("API_KEY")
API_SECRET = get_key("API_SECRET")

cloudinary.config(cloud_name=CLOUD_NAME, api_key=API_KEY, api_secret=API_SECRET)

# === Constants ===
PROCESSED_FILE = os.path.join(BASE_DIR, "processed_images.json")
LOG_UPLOADS_FILE = os.path.join(BASE_DIR, "uploads_log.txt")

# === Helpers ===
def load_processed_images():
    if os.path.exists(PROCESSED_FILE):
        try:
            with open(PROCESSED_FILE, "r") as f:
                return json.load(f) if f.read().strip() else {}
        except json.JSONDecodeError:
            logging.warning("Malformed processed_images.json, starting fresh.")
    return {}

def save_processed_images(data):
    with open(PROCESSED_FILE, "w") as f:
        json.dump(data, f, indent=2)

def log_upload(timestamp, public_id, url, is_duplicate):
    with open(LOG_UPLOADS_FILE, "a") as f:
        f.write(f"{timestamp} | {public_id} | {url} {'| DUPLICATE' if is_duplicate else ''}\n")

def fetch_new_images(since_time):
    resources = cloudinary.api.resources(type="upload", resource_type="image", max_results=100, direction="asc")
    return sorted([
        (datetime.fromisoformat(res["created_at"].replace("Z", "+00:00")), res)
        for res in resources.get("resources", [])
        if datetime.fromisoformat(res["created_at"].replace("Z", "+00:00")) > since_time
    ], key=lambda x: x[0])

def fetch_posting_configs():
    result = supabase.table("config").select("config_name", "config_value").execute()
    return {item["config_name"]: item["config_value"] for item in result.data}

def get_furthest_scheduled_date():
    result = supabase.table("postsdb").select("scheduled_time").order("scheduled_time", desc=True).limit(1).execute()
    return datetime.fromisoformat(result.data[0]["scheduled_time"]) if result.data else datetime.now()

def get_next_schedule_time(base_time, frequency):
    if frequency.lower() == "daily":
        return base_time + timedelta(days=1)
    elif frequency.lower() == "weekly":
        return base_time + timedelta(weeks=1)
    elif frequency.lower() == "monthly":
        return base_time + timedelta(days=30)
    return base_time + timedelta(days=1)

def available_pictures_count():
    all_images = cloudinary.api.resources(type="upload", resource_type="image", max_results=500)["resources"]
    used_ids = {x["image_path"] for x in supabase.table("postsdb").select("image_path").execute().data}
    count = len([img for img in all_images if img["public_id"] not in used_ids])
    supabase.table("config").upsert({
        "config_name": "available_pictures",
        "config_value": str(count)
    }, on_conflict=["config_name"]).execute()
    return count

def add_post(image_path, caption, scheduled_time, url, dontuse):
    supabase.table("postsdb").insert({
        "image_path": image_path,
        "caption": caption,
        "scheduled_time": scheduled_time.isoformat(),
        "posted": "Pending",
        "image_url": url,
        "dont_use_until": dontuse.isoformat()
    }).execute()

# === Main Watcher ===
def main():
    used_hours = set()
    processed = load_processed_images()
    last_time = max([datetime.fromisoformat(item["created_at"]) for item in processed.values()] + [datetime.min.replace(tzinfo=timezone.utc)])

    while True:
        print("🔍 Checking for new uploads...")
        new_images = fetch_new_images(last_time)
        configs = fetch_posting_configs()

        num_posts = int(configs.get("num_of_posts", 2))
        frequency = configs.get("frequency", "daily")
        dont_use_until_days = int(configs.get("dontuseuntil", 0))

        available = available_pictures_count()
        print("Available pictures:", available)
        if available == 0:
            print("⚠️ No available pictures to schedule.")
            time.sleep(5)
            continue

        for created_at, image in new_images:
            public_id = image["public_id"]
            public_url = image.get("secure_url")
            file_signature = f"{image['bytes']}_{image['format']}"
            timestamp = created_at.isoformat()

            is_duplicate = any(file_signature == item["signature"] for item in processed.values())
            log_upload(timestamp, public_id, public_url, is_duplicate)
            processed[public_id] = {
                "created_at": timestamp,
                "url": public_url,
                "signature": file_signature,
                "duplicate": is_duplicate
            }
            save_processed_images(processed)

            if is_duplicate:
                print(f"🟡 Duplicate skipped: {public_id}")
                continue

            print(f"🆕 New image: {public_id}")
            caption_output = generate_caption(public_id, used_hours, public_url)
            caption = caption_output.split("Recommended Time:")[0].strip()

            # Extract hour from recommended time
            match = re.search(r"Recommended Time: (\d{1,2}):\d{2} (AM|PM)", caption_output, re.IGNORECASE)
            if match:
                hour = int(match.group(1)) % 12 + (12 if match.group(2).upper() == "PM" else 0)
            else:
                hour = 12
            used_hours.add(hour)

            # Determine next available schedule time
            last_scheduled = get_furthest_scheduled_date()
            scheduled_time = get_next_schedule_time(last_scheduled, frequency).replace(hour=hour, minute=0)
            dont_use_until = datetime.now() + timedelta(days=dont_use_until_days)

            print(f"📅 Scheduling {public_id} for {scheduled_time} EST")
            add_post(public_id, caption, scheduled_time, public_url, dont_use_until)

        time.sleep(5)

if __name__ == "__main__":
    main()
