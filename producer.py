import requests
import time
import json
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# --- Configuration ---
API_KEY = '2dvrz11AAYNGDNiOft9zcwXD8T65cS9I'
BASE_URL = 'https://app.ticketmaster.com/discovery/v2/events.json'

# <--- CHANGED: Expanded the list of segments to search, including Sports --->
SEGMENTS_TO_SEARCH = ['Music', 'Sports', 'Arts & Theatre', 'Film', 'Comedy', 'Family', 'Miscellaneous']

# --- Kafka Producer Setup ---
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Successfully connected to Kafka producer.")
except Exception as e:
    print(f"Could not connect to Kafka producer: {e}")
    producer = None

def run_producer_continuously():
    """
    Main function to orchestrate fetching events and sending them to Kafka.
    This function will run in an infinite loop.
    """
    if not producer:
        print("Exiting: Kafka producer is not available.")
        return

    # <--- CHANGED: Added an infinite loop to run the producer continuously --->
    while True:
        print("\n=================================================")
        print(f"=== Starting new fetch cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        print("=================================================")

        for segment in SEGMENTS_TO_SEARCH:
            print(f"\n--- Searching for events in segment: {segment} ---")
            fetch_and_send_events(segment)
        
        producer.flush()
        print("\nAll tasks for this cycle are complete. Producer has flushed all messages.")

        # <--- CHANGED: Wait for 1 hour (3600 seconds) before starting the next cycle --->
        wait_duration_seconds = 3600
        print(f"--- Sleeping for {wait_duration_seconds / 60} minutes before the next cycle... ---")
        time.sleep(wait_duration_seconds)


def fetch_and_send_events(segment_name):
    """
    Fetches events for a segment and sends each one to a Kafka topic.
    (This function's internal logic remains the same)
    """
    start_date = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date = (datetime.now(timezone.utc) + timedelta(days=60)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    params = {
        'apikey': API_KEY, 'countryCode': 'US', 'classificationName': segment_name,
        'startDateTime': start_date, 'endDateTime': end_date, 'size': 200
    }
    
    current_page = 0
    total_pages = 1

    while current_page < total_pages:
        try:
            params['page'] = current_page
            print(f"Fetching page {current_page + 1} of {total_pages} for {segment_name}...")
            
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            if current_page == 0:
                total_pages = data.get('page', {}).get('totalPages', 1)
                if total_pages == 0: break
                if total_pages > 5: total_pages = 5 # Limit to 5 pages per segment to be API-friendly

            if '_embedded' in data:
                for event in data['_embedded']['events']:
                    if 'priceRanges' in event and event['priceRanges']:
                        event_details = process_event_data(event)
                        producer.send('ticketmaster_events', value=event_details)
                        print(f"  -> Sent event '{event_details['name']}' to Kafka.")
            
            current_page += 1
            if current_page < total_pages: time.sleep(0.25)

        except Exception as e:
            print(f"An error occurred for {segment_name}: {e}")
            break

def process_event_data(event):
    """
    (This function remains the same)
    """
    price_range = event.get('priceRanges', [{}])[0]
    min_price = price_range.get('min')
    max_price = price_range.get('max')
    
    avg_price = None
    if min_price is not None and max_price is not None:
        avg_price = (min_price + max_price) / 2

    return {
        'id': event.get('id', 'N/A'),
        'name': event.get('name', 'N/A'),
        'url': event.get('url', 'N/A'),
        'event_date': event.get('dates', {}).get('start', {}).get('localDate', 'N/A'),
        'segment': event.get('classifications', [{}])[0].get('segment', {}).get('name', 'N/A'),
        'genre': event.get('classifications', [{}])[0].get('genre', {}).get('name', 'N/A'),
        'venue_name': event.get('_embedded', {}).get('venues', [{}])[0].get('name', 'N/A'),
        'city': event.get('_embedded', {}).get('venues', [{}])[0].get('city', {}).get('name', 'N/A'),
        'state': event.get('_embedded', {}).get('venues', [{}])[0].get('state', {}).get('stateCode', 'N/A'),
        'min_price': min_price,
        'max_price': max_price,
        'avg_price': avg_price,
        'ingestion_timestamp': datetime.now(timezone.utc).isoformat()
    }

if __name__ == "__main__":
    # <--- CHANGED: Call the new continuous function --->
    run_producer_continuously()