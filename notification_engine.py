import time
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone

MONGO_URI = "mongodb://127.0.0.1:27017/"
CHECK_INTERVAL_SECONDS = 300
REMINDER_DAYS_BEFORE_EVENT = 7 

client = MongoClient(MONGO_URI)
db = client["event_pulse_db"]
events_collection = db["events"]
users_collection = db["users"]
notifications_collection = db["notifications"]

def check_new_events_for_prefs():
    print("--- Checking for new events based on preferences...")
    all_users = list(users_collection.find({}, {"_id": 0, "username": 1, "favorite_segments": 1}))
    if not all_users:
        print("No users with preferences found. Skipping new event check.")
        return

    time_threshold = datetime.now(timezone.utc) - timedelta(seconds=CHECK_INTERVAL_SECONDS + 60)
    new_events = list(events_collection.find({"ingestion_timestamp": {"$gte": time_threshold.isoformat()}}))
    
    if not new_events:
        print("No new events found in the last interval.")
        return

    print(f"Found {len(new_events)} new events to process.")
    for user in all_users:
        username = user.get("username")
        prefs = user.get("favorite_segments", [])
        if not username or not prefs:
            continue
            
        for event in new_events:
            if event.get("segment") in prefs:

                if not notifications_collection.find_one({"username": username, "event_id": event.get("id"), "notification_type": "new_event_match"}):
                    new_notification = {
                        "username": username, "event_id": event.get("id"), "event_name": event.get("name"),
                        "message": f"New event in your preferred segment '{event.get('segment')}'!",
                        "timestamp": datetime.now(timezone.utc), "is_read": False,
                        "notification_type": "new_event_match"
                    }
                    notifications_collection.insert_one(new_notification)
                    print(f"  -> Created 'New Event' notification for user '{username}' for event '{event.get('name')}'")


def check_favorite_event_reminders():
    print("--- Checking for upcoming favorited events...")

    all_users = list(users_collection.find({"favorite_event_ids": {"$exists": True, "$ne": []}}))
    if not all_users:
        print("No users with favorited events found. Skipping reminder check.")
        return

    today = datetime.now(timezone.utc).date()

    for user in all_users:
        username = user.get("username")
        favorite_ids = user.get("favorite_event_ids", [])
        if not favorite_ids:
            continue


        fav_events = list(events_collection.find({"id": {"$in": favorite_ids}}))
        
        for event in fav_events:
            try:
                event_date_str = event.get("event_date")
                if not event_date_str: continue
                
                event_date = datetime.strptime(event_date_str, "%Y-%m-%d").date()
                days_until = (event_date - today).days


                if 0 <= days_until <= REMINDER_DAYS_BEFORE_EVENT:

                    if not notifications_collection.find_one({"username": username, "event_id": event.get("id"), "notification_type": "date_approaching"}):
                        if days_until > 0:
                            message = f"Reminder: Your favorited event '{event.get('name')}' is in {days_until} days!"
                        else:
                            message = f"Reminder: Your favorited event '{event.get('name')}' is today!"
                        
                        new_notification = {
                            "username": username, "event_id": event.get("id"), "event_name": event.get("name"),
                            "message": message,
                            "timestamp": datetime.now(timezone.utc), "is_read": False,
                            "notification_type": "date_approaching"
                        }
                        notifications_collection.insert_one(new_notification)
                        print(f"  -> Created 'Date Approaching' notification for user '{username}' for event '{event.get('name')}'")
            except (ValueError, TypeError):

                continue

def run_notification_engine():
    print("Notification engine connected to MongoDB.")
    while True:
        print(f"\n--- Running notification cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
        

        check_new_events_for_prefs()
        check_favorite_event_reminders()

        print(f"--- Cycle complete. Sleeping for {CHECK_INTERVAL_SECONDS} seconds... ---")
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_notification_engine()