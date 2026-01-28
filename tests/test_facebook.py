import os
import requests
from dotenv import load_dotenv

#Taking token from .env
load_dotenv()
token = os.getenv("FACEBOOK_ACCESS_TOKEN")

#Checking wheter there is key or not
if not token:
    print("Error: No such token")
    exit()
print("Token is found")

#Connect to Meta
try:
    print("Connecting to Meta")

    #Facebook's 'me' service
    url = "https://graph.facebook.com/v19.0/me"

    #Parameters
    params = {
        "fields": "id,name",
        "access_token": token
    }

    #Send request
    response = requests.get(url,params=params)
    data = response.json()

    if response.status_code == 200:
        print("Connection Established")
        print(f"Facebook Name: {data.get("name")}")
        print(f"Facebook ID: {data.get("id")}")
    else:
        print("Error: No such id or no user")
        print("Facebook: {data}")

except Exception as e:
    print(f"Error: {e}")