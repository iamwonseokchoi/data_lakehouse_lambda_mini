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

def get_video_names_and_etags(video_ids):
    request = youtube.videos().list(
        part="snippet",
        id=','.join(video_ids)
    )
    response = request.execute()
    video_names_and_etags = [(item['etag'], item['snippet']['title']) for item in response['items']]
    return video_names_and_etags


if __name__ == "__main__":
    api_key = os.environ.get("YOUTUBE_API_KEY") 
    youtube = build('youtube', 'v3', developerKey=api_key)
    channel_ids = ["UCqNkn5PNeSqbQTAR5RP-L-Q"]

    try:
        video_ids = get_video_ids_by_channel(channel_ids)
        video_names_and_etags = get_video_names_and_etags(video_ids)
        with open("video_names_and_etags.json", "w") as outfile:
            json.dump(video_names_and_etags, outfile)
    except HttpError as e:
        print(f"Error:: {e}")