import os
import json
import psycopg2
from urllib.parse import urlparse
import glob
from datetime import datetime

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')

# DATABASE CONNECTION (Port 5434)
DB_CONNECTION_STR = "postgresql://metehan_user:super_secret_password@localhost:5434/unified_media_dw"

def get_db_config(connection_str):
    result = urlparse(connection_str)
    return {
        "database": result.path[1:],
        "user": result.username,
        "password": result.password,
        "host": result.hostname,
        "port": result.port
    }

def get_latest_file():
    pattern = os.path.join(RAW_DATA_DIR, "facebook_raw_posts_*.json")
    list_of_files = glob.glob(pattern)
    
    if not list_of_files:
        return None
    
    list_of_files.sort(reverse=True)
    return list_of_files[0]

def load_facebook_posts_bronze():
    print("--- FACEBOOK BRONZE LOAD STARTED ---")

    try:
        db_config = get_db_config(DB_CONNECTION_STR)

    except Exception as e:
        print(f"CONFIG ERROR: {e}")
        return

    latest_file = get_latest_file()
    if not latest_file:
        print("ERROR: No file found.")
        return
    
    print(f"INFO: Processing -> {os.path.basename(latest_file)}")

    try:
        with open(latest_file, 'r', encoding='utf-8') as f:
            posts = json.load(f)

    except Exception as e:
        print(f"ERROR: File read failed: {e}")
        return

    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # 1. ENSURE 'BRONZE' SCHEMA EXISTS
        # This is the critical fix!
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        
        # 2. CREATE TABLE IN 'BRONZE' SCHEMA
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze.facebook_posts (
                post_id VARCHAR(50) PRIMARY KEY,
                message TEXT,
                created_at TIMESTAMP,
                permalink_url TEXT,
                like_count INT DEFAULT 0,
                comment_count INT DEFAULT 0,
                share_count INT DEFAULT 0,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        inserted_count = 0
        
        print(f"INFO: Loading {len(posts)} rows into 'bronze.facebook_posts'...")

        for post in posts:
            try:
                p_id = post.get('id')
                msg = post.get('message', '')
                link = post.get('permalink_url', '')
                
                raw_date = post.get('created_time')
                created_at = None
                if raw_date:
                    created_at = raw_date.replace('T', ' ').split('+')[0]

                likes = post.get('likes', {}).get('summary', {}).get('total_count', 0)
                comments = post.get('comments', {}).get('summary', {}).get('total_count', 0)
                shares = post.get('shares', {}).get('count', 0)

                # 3. INSERT INTO 'BRONZE' TABLE
                sql = """
                    INSERT INTO bronze.facebook_posts 
                    (post_id, message, created_at, permalink_url, like_count, comment_count, share_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (post_id) 
                    DO UPDATE SET 
                        like_count = EXCLUDED.like_count,
                        comment_count = EXCLUDED.comment_count,
                        share_count = EXCLUDED.share_count,
                        loaded_at = CURRENT_TIMESTAMP;
                """
                cur.execute(sql, (p_id, msg, created_at, link, likes, comments, shares))
                inserted_count += 1
                
            except Exception as row_e:
                print(f"Skipped row: {row_e}")
                continue

        conn.commit()
        cur.close()
        conn.close()
        
        print(f"SUCCESS: {inserted_count} posts loaded into 'bronze.facebook_posts'.")
        print("IMPORTANT: Please Refresh your 'bronze' schema in PgAdmin to see the table.")

    except Exception as db_e:
        print(f"DATABASE ERROR: {db_e}")

if __name__ == "__main__":
    load_facebook_posts_bronze()