import os
import json
import datetime
from dotenv import load_dotenv
from apify_client import ApifyClient

# --- CONFIGURATION ---
load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# File Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
CONFIG_FILE = os.path.join(BASE_DIR, "config", "competitor_pages.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "raw", "facebook")

# Initialize Client
client = ApifyClient(APIFY_TOKEN)

def load_competitor_pages():
    """Load list of competitor Facebook pages from config file"""
    if not os.path.exists(CONFIG_FILE):
        print(f"[ERROR] Config file not found: {CONFIG_FILE}")
        return []
    
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    return config.get("competitor_pages", [])

def scrape_facebook_posts(page_urls, max_posts_per_page=5):
    """
    Scrape posts from Facebook pages using Apify
    """
    print("-" * 60)
    print("STARTING FACEBOOK POSTS SCRAPER")
    print("-" * 60)
    print(f"[INFO] Pages to scrape: {len(page_urls)}")
    print(f"[INFO] Posts per page: {max_posts_per_page}")
    
    # Actor Input Configuration
    run_input = {
        "startUrls": [{"url": url} for url in page_urls],
        "resultsLimit": max_posts_per_page,
        "scrapeAbout": False,
        "scrapeReviews": False,
        "scrapeServices": False,
        "proxyConfiguration": { "useApifyProxy": True }  # CRITICAL: Must use Proxy
    }
    
    print("\n[INFO] Sending request to Apify (apify/facebook-posts-scraper)...")
    
    try:
        # Run the Actor
        run = client.actor("apify/facebook-posts-scraper").call(run_input=run_input)
        
        print("[INFO] Scraping job finished. Fetching results...")
        
        # Fetch results
        items = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            items.append(item)
        
        print(f"[SUCCESS] Retrieved {len(items)} posts total.")
        return items
    
    except Exception as e:
        print(f"[ERROR] Scraping failed: {e}")
        return []

def scrape_page_info(page_urls):
    """
    Scrape basic page details (Follower count, etc.)
    """
    print("\n" + "-" * 60)
    print("STARTING PAGE INFO SCRAPER")
    print("-" * 60)
    
    run_input = {
        "startUrls": [{"url": url} for url in page_urls],
        "proxyConfiguration": { "useApifyProxy": True } # CRITICAL: Must use Proxy
    }
    
    print("[INFO] Sending request to Apify (apify/facebook-pages-scraper)...")
    
    try:
        run = client.actor("apify/facebook-pages-scraper").call(run_input=run_input)
        
        items = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            items.append(item)
        
        print(f"[SUCCESS] Retrieved info for {len(items)} pages.")
        return items
    
    except Exception as e:
        print(f"[ERROR] Page info scraping failed: {e}")
        return []

def save_results(posts, page_info):
    """Save scraped data to JSON files in data/raw/facebook"""
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # 1. Save Posts
    if posts:
        posts_file = os.path.join(OUTPUT_DIR, f"facebook_posts_{timestamp}.json")
        with open(posts_file, "w", encoding="utf-8") as f:
            json.dump(posts, f, ensure_ascii=False, indent=2)
        print(f"\n[FILE SAVED] Posts: {os.path.basename(posts_file)}")

    # 2. Save Page Info
    if page_info:
        info_file = os.path.join(OUTPUT_DIR, f"facebook_pages_info_{timestamp}.json")
        with open(info_file, "w", encoding="utf-8") as f:
            json.dump(page_info, f, ensure_ascii=False, indent=2)
        print(f"[FILE SAVED] Page Info: {os.path.basename(info_file)}")

    # 3. Print Summary
    print("\n" + "=" * 60)
    print("SUMMARY REPORT")
    print("=" * 60)
    
    # Simple aggregation logic
    posts_by_page = {}
    for post in posts:
        # Some scrapers return 'url' or 'pageUrl', we handle both safely
        user = post.get("user", {}).get("name") or "Unknown Page"
        if user not in posts_by_page:
            posts_by_page[user] = []
        posts_by_page[user].append(post)
    
    for page_name, page_posts in posts_by_page.items():
        total_likes = sum(p.get("likes", 0) for p in page_posts)
        total_comments = sum(p.get("comments", 0) for p in page_posts)
        
        print(f"\nPage: {page_name}")
        print(f" - Posts Fetched: {len(page_posts)}")
        print(f" - Total Likes: {total_likes}")
        print(f" - Total Comments: {total_comments}")

if __name__ == "__main__":
    print("=" * 60)
    print("FACEBOOK COMPETITOR MONITOR (APIFY)")
    print("=" * 60)
    
    # 1. Load Config
    competitor_urls = load_competitor_pages()
    if not competitor_urls:
        print("[STOP] No competitor pages found in config. Exiting.")
        exit(1)
    
    # 2. Scrape Posts
    posts = scrape_facebook_posts(competitor_urls, max_posts_per_page=5)
    
    # 3. Scrape Page Info (Optional, but useful)
    page_info = scrape_page_info(competitor_urls)
    
    # 4. Save Everything
    if posts or page_info:
        save_results(posts, page_info)
    else:
        print("\n[WARNING] No data retrieved from any source.")
    
    print("\n[SYSTEM] Job Completed.")