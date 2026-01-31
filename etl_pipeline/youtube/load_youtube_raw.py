import os
import json
import pandas as pd
from sqlalchemy import create_engine
import glob

#Connecitng to the DB
#Attention Port 5434 due to other ports in use for other projects
DB_CONNECTION_STR = "postgresql://metehan_user:super_secret_password@localhost:5434/unified_media_dw"

def load_data_to_db():
    print("Raw data is loading to db please wait....")

    #Finding the downloaded youtube JSON data
    list_of_files = glob.glob("data/raw/youtube_videos_*.json")

    if not list_of_files:
        print("Error there is no JSON file found")
        return
   
    #Having the last file
    latest_file = max(list_of_files, key=os.path.getatime)
    print(f"Latest file found {latest_file}")

    try:
        df = pd.read_json(latest_file)

        if df.empty:
            print("JSON file is empty")
            return
        else:
            print(f"{len(df)} rows is inserting")
        
        engine = create_engine(DB_CONNECTION_STR)
        df.to_sql("youtube_videos", 
                  engine, 
                  schema = "bronze",
                  if_exists="append",
                  index = False)
        print("All data inserted into 'bronze.youtube_videos'")

    except Exception as e:
        #There might be duplicated videos in JSON
        if "unique_constraint" in str(e).loser():
            print("Duplicated data there is already video with the same Primary Key")
        else:
            print(f"Error: {e}")

if __name__ == "__main__":
    load_data_to_db()