from confluent_kafka import Producer
from googleapiclient.discovery import build
import os

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_to_kafka(topic, data):
    producer.produce(topic, key=None, value=data, callback=delivery_report)
    producer.flush()

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

def get_video_metrics(video_ids):
    request = youtube.videos().list(
        part="statistics",
        id=','.join(video_ids)
    )
    response = request.execute()
    return response['items']

if __name__ == "__main__":
    api_key = os.environ.get("YOUTUBE_API_KEY") 
    youtube = build('youtube', 'v3', developerKey=api_key)
    channel_ids = ["UCqNkn5PNeSqbQTAR5RP-L-Q"]

    video_ids = get_video_ids_by_channel(channel_ids)
    video_metrics = get_video_metrics(video_ids)

    # Produce the metrics to Kafka topic
    for metric in video_metrics:
        produce_to_kafka('youtube-metrics', str(metric))

    print('Metrics pushed to Kafka')
