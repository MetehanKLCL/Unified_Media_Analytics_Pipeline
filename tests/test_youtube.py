import os
from dotenv import load_dotenv
from googleapiclient.discovery import build

#Taking key from .env folder
load_dotenv()
api_key = os.getenv("YOUTUBE_API_KEY")

#Checking wheter there is key or not
if not api_key:
    print("Error: no such file")
    exit()
print(f"Key is found: {api_key[:3]}")

#Connecting to Youtube
try:
    print("Connecting to Google...")
    youtube = build(
        "youtube",
        "v3",
        developerKey=api_key)
    
    #Connecting to the channel
    target_id = "UC_x5XG1OV2P6uZZ5FSM9Ttw" #Google Developers

    request = youtube.channels().list(
      part="statistics,snippet",
      id=target_id 
    )
    response = request.execute()

    #Printing the result
    if "items" in response and len(response["items"]) > 0:
        channel_name = response["items"][0]["snippet"]["title"]
        view_count = response["items"][0]["statistics"]["viewCount"]

        print("Connection Established")
        print(f"Channel name: {channel_name}")
        print(f"Total View Count: {view_count}")

    else:
        print("Error: No such id or no items")
        print(f"Google: {response}")

except Exception as e:
    print(f"Error: {e}")