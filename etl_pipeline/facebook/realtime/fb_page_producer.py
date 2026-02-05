import sys
import os
import time
import json
import copy
from datetime import datetime, timezone
from confluent_kafka import Producer

# --- 1. SETUP PATHS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
facebook_dir = os.path.dirname(current_dir)
sys.path.append(facebook_dir)

try:
    from extract_facebook import load_config, fetch_posts
except ImportError:
    print("CRITICAL ERROR: Could not import 'extract_facebook.py'.")
    sys.exit(1)

# --- GLOBAL STATE ---
page_states = {}
topic_name = "fb_realtime_events"

def get_kafka_producer():
    """Initializes and returns the Kafka Producer."""
    conf = {
        "bootstrap.servers": "localhost:9092" # Fixed: 'services' -> 'servers'
    }
    return Producer(conf)

def delivery_report(err, msg):
    """Callback for Kafka delivery."""
    if err is not None:
        print(f"Message delivery failed: {err}")

def get_target_pages(config):
    """Extracts list of page IDs from config."""
    pages = config.get("my_pages", [])
    single_page = config.get("page_id")
    
    if single_page and single_page not in pages:
        pages.append(single_page)
        
    if not pages:
        print("ERROR: No page IDs found in config.")
        sys.exit(1)
        
    return pages

def create_kafka_payload(page_id, post):
    """Transforms raw Facebook post into Kafka message format."""
    post_time = post.get("created_time")
    
    return {
        "source_page": page_id,
        "post_id": post.get('id'),
        "message": post.get('message', ''),
        "created_time": post_time,
        "metrics": {
            "likes": post.get('likes', {}).get('summary', {}).get('total_count', 0),
            "comments": post.get('comments', {}).get('summary', {}).get('total_count', 0)
        },
        # Fixed: utcnow removed, using timezone-aware object
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }

def process_page(page_id, base_config, producer):
    """Fetches and processes posts for a single page."""
    global page_states
    new_posts_count = 0
    
    # Clone config for this specific page
    current_config = copy.deepcopy(base_config)
    current_config['page_id'] = page_id

    posts = fetch_posts(current_config)

    for post in reversed(posts):
        post_time = post.get("created_time")
        last_seen = page_states.get(page_id)

        if post_time > last_seen:
            payload = create_kafka_payload(page_id, post)
            
            producer.produce(
                topic_name,
                key=post.get("id"),
                value=json.dumps(payload),
                callback=delivery_report
            )
            
            page_states[page_id] = post_time
            new_posts_count += 1
            print(f"[{page_id}] New Post Ingested: {payload['post_id']}")
            
    return new_posts_count

def main():
    print("Starting Modular Page Producer...")
    
    # Initialization
    producer = get_kafka_producer()
    config = load_config()
    target_pages = get_target_pages(config)
    
    # Initialize State
    start_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+0000')
    for pid in target_pages:
        page_states[pid] = start_time
        
    print(f"Tracking {len(target_pages)} pages from: {start_time}")
    print("-" * 50)

    try:
        while True:
            total_new = 0
            
            for page_id in target_pages:
                total_new += process_page(page_id, config, producer)

            producer.poll(0)

            if total_new > 0:
                print(f"Cycle finished. Total new: {total_new}")
            else:
                print(".", end="", flush=True)

            time.sleep(60)

    except KeyboardInterrupt:
        print("\nStopping...")
        producer.flush()

if __name__ == "__main__":
    main()