import os
import json
import requests
import datetime
import sys

# --- Configuration & Constants ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'facebook_token.json')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'raw')
API_VERSION = "v24.0"

def load_config():
    """
    Loads the configuration file safely.
    """
    if not os.path.exists(CONFIG_PATH):
        print(f"Critical Error: Configuration file not found at {CONFIG_PATH}")
        sys.exit(1)
    
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Critical Error: Failed to load config file. {e}")
        sys.exit(1)

def fetch_posts(config):
    """
    Fetches the latest posts with engagement metrics (Likes, Comments, Shares).
    """
    token = config.get('page_access_token')
    page_id = config.get('page_id')
    
    if not token or not page_id:
        print("Error: Missing token or page_id.")
        return []

    url = f"https://graph.facebook.com/{API_VERSION}/{page_id}/posts"
    
    # Standard engagement fields
    fields = "id,message,created_time,permalink_url,likes.summary(true),comments.summary(true),shares"
    
    params = {
        'access_token': token,
        'fields': fields,
        'limit': 50
    }
    
    print(f"Fetching Facebook posts (API {API_VERSION})...")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"Error fetching posts: {response.status_code} - {response.text}")
            return []
            
        data = response.json()
        posts = data.get('data', [])
        print(f"Successfully retrieved {len(posts)} posts.")
        return posts

    except Exception as e:
        print(f"Exception while fetching posts: {e}")
        return []

def fetch_revenue_breakdown(config):
    """
    Fetches revenue from specific sources (Reels, Subscriptions) instead of aggregate.
    This avoids the 'metric not found' error on newer API versions.
    """
    token = config.get('page_access_token')
    page_id = config.get('page_id')
    url = f"https://graph.facebook.com/{API_VERSION}/{page_id}/insights"
    
    # Metrics based on your Business Suite dashboard (Reels + Subscriptions)
    target_metrics = [
        "ads_on_reels_earnings",      # Earnings from Reels ads
        "fan_support_earnings",       # Earnings from Subscriptions/Stars
        "video_monetization_earnings" # In-stream ads (if any)
    ]
    
    daily_revenue_map = {}
    print(f"Fetching revenue breakdown metrics for {API_VERSION}: {', '.join(target_metrics)}...")
    
    for metric in target_metrics:
        params = {
            'access_token': token,
            'metric': metric,
            'period': 'day',
            'since': '2026-01-01',
            'until': 'now'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            
            if 'error' in data:
                # Log warning but continue to next metric
                print(f"Warning for '{metric}': {data['error']['message']}")
                continue
                
            if 'data' in data:
                for item in data['data']:
                    for value in item.get('values', []):
                        date = value.get('end_time', '').split('T')[0]
                        amount = float(value.get('value', 0))
                        
                        # Initialize date entry if not exists
                        if date not in daily_revenue_map:
                            daily_revenue_map[date] = {
                                'date': date,
                                'total_usd': 0.0,
                                'breakdown': {}
                            }
                        
                        # Accumulate total and store breakdown
                        if amount > 0:
                            daily_revenue_map[date]['total_usd'] += amount
                            daily_revenue_map[date]['breakdown'][metric] = amount
                        
        except Exception as e:
            print(f"Error fetching metric {metric}: {e}")

    # Convert map to list and sort by date
    revenue_list = sorted(daily_revenue_map.values(), key=lambda x: x['date'], reverse=True)
    
    if revenue_list:
        print(f"Successfully compiled revenue data for {len(revenue_list)} days.")
        # Print a sample to verify
        print(f"Sample (Latest Day): {revenue_list[0]}")
    else:
        print("No revenue data found for the specified period.")
        
    return revenue_list

def save_data(data, file_suffix):
    """
    Saves data to a JSON file.
    """
    if not data:
        return

    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        filename = os.path.join(OUTPUT_DIR, f"facebook_raw_{file_suffix}_{date_str}.json")
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"Saved: {filename}")
        
    except Exception as e:
        print(f"Error saving file: {e}")

def main():
    print("--- FACEBOOK DATA PIPELINE ---")
    
    config = load_config()
    
    # 1. Fetch and Save Posts
    posts = fetch_posts(config)
    save_data(posts, "posts")
    
    print("-" * 30)
    
    # 2. Fetch and Save Revenue (Breakdown Strategy)
    revenue = fetch_revenue_breakdown(config)
    save_data(revenue, "revenue")
    
    print("--- COMPLETED ---")

if __name__ == "__main__":
    main()