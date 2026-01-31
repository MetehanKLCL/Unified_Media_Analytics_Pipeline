import os
import json
import datetime
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly"
]

CLIENT_SECRET_FILE = 'config/client_secret.json'
TOKEN_FILE = 'config/youtube_token.json'

def authenticate_youtube():
    creds = None

    token_path = 'config/youtube_token.json'

    try:
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("Token is expired")
                creds.refresh(Request())
            else:
                print("Please Login")
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
                creds = flow.run_local_server(port=0)

            with open(token_path, 'w') as token:
                token.write(creds.to_json())
        return creds
    
    except Exception as e:
        print(f"Authentication Error: {e}")
        return None
    
def get_video_stats(youtube, video_ids):
    stats_map = {}
    
    try:
        ids_string = ",".join(video_ids)
        request = youtube.videos().list(
            part="statistics,contentDetails",
            id=ids_string
        )
        response = request.execute()

        #ID Mapping
        for item in response.get("items", []):
            vid_id = item["id"]
            stats_map[vid_id] = {
                "view_count": item["statistics"].get("viewCount", 0),
                "like_count": item["statistics"].get("likeCount", 0),
                "comment_count": item["statistics"].get("commentCount", 0),
                "duration": item["contentDetails"].get("duration") # Bonus: Video süresi
            }

    except Exception as e:
        print(f"Statistics error: {e}")
    
    return stats_map

    
def get_recent_videos(youtube, max_results=50):
    print("Getting channel information")
    try:
        channel_response = youtube.channels().list(
            id='UC_x5XG1OV2P6uZZ5FSM9Ttw',
            part="contentDetails"
        ).execute()

        if not channel_response.get("items"):
            print("Error: No such channel")
            return []
        
        uploads_playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        print(f"Videos coming (Playlist ID: {uploads_playlist_id}...) please wait")
        videos = []

        request = youtube.playlistItems().list(
            playlistId=uploads_playlist_id,
            part='snippet,contentDetails',
            maxResults=max_results
        )

        while request:
            response = request.execute()

            video_ids_in_page = []
            for item in response.get("items", []):
                video_ids_in_page.append(item['contentDetails']['videoId'])

            stats_map = {}
            if video_ids_in_page:
                stats_map = get_video_stats(youtube, video_ids_in_page)

            for item in response.get("items", []):
                vid_id = item['contentDetails']['videoId']
                
                stats = stats_map.get(vid_id, {})

                video_data = {
                    'video_id': vid_id,
                    'title': item['snippet']['title'],
                    'published_at': item['snippet']['publishedAt'],
                    'channel_title': item['snippet']['channelTitle'],
                    'view_count': stats.get('view_count', 0),
                    'like_count': stats.get('like_count', 0),
                    'comment_count': stats.get('comment_count', 0),
                    'duration': stats.get('duration', "")
                }
                videos.append(video_data)

            request = youtube.playlistItems().list_next(request, response)

        return videos
    
    except HttpError as e:
        error_reason = json.loads(e.content)["error"]["errors"][0]["reason"]

        if e.resp.status == 403:
            if "quotaExceeded" in error_reason:
                print("Daily Youtube API quota has been reached")
            else:
                print(f"Authorization Error (403): {error_reason}")
        elif e.resp.status == 404:
            print("No such Source (404)")
        else:
            print(f"Google API Error: {e}")
        return []
    
    except Exception as e:
        print(f"Error: {e}")
        return []
    
if __name__ == "__main__":
    creds = authenticate_youtube()

    if creds:
        try:
            youtube = build("youtube", "v3", credentials=creds)
            video_list = get_recent_videos(youtube)

            if video_list:
                date_str = datetime.datetime.now().strftime("%Y-%m-%d")
                filename = f"data/raw/youtube_videos_{date_str}.json"

                os.makedirs("data/raw", exist_ok=True)

                with open(filename, "w", encoding='utf-8') as f:
                    json.dump(video_list, f, ensure_ascii=False, indent=4)

                print(f"Succesfully {len(video_list)} videos got from the source")
                print(f"File: {filename}")
            else:
                print("No videos got from the source")
        except Exception as e:
            print(f"Error {e}")