import os
import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def get_video_ids_by_channel(channel_ids):
    video_ids = []
    for channel_id in channel_ids:
        request = youtube.search().list(
            part="id",
            channelId=channel_id,
            maxResults=1000,
            type="video"
        )
        response = request.execute()
        video_ids += [item['id']['videoId'] for item in response['items']]
    return video_ids

def get_video_metadata(video_ids):
    request = youtube.videos().list(
        part="snippet, status",
        id=','.join(video_ids)
    )
    response = request.execute()
    return response['items']


if __name__ == "__main__":
    api_key = os.environ.get("YOUTUBE_API_KEY") 
    youtube = build('youtube', 'v3', developerKey=api_key)
    channel_ids = ["UCqNkn5PNeSqbQTAR5RP-L-Q"]

    try:
        video_ids = get_video_ids_by_channel(channel_ids)
        video_metadata = get_video_metadata(video_ids)
        with open("video_metadata.json", "w") as outfile:
            json.dump(video_metadata, outfile)
    except HttpError as e:
        print(f"Error: {e}")
