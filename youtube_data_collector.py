import os
import pickle
import datetime
import sqlite3
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
from tqdm import tqdm
import logging
import time
logging.basicConfig(filename='youtube_data_collector.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

class YouTubeDataCollector:
    def __init__(self, client_secrets_file, db_file):
        self.client_secrets_file = client_secrets_file
        self.db_file = db_file
        self.conn = sqlite3.connect(db_file)
        self.create_tables()

    def create_tables(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS videos 
                     (channel_title TEXT, video_title TEXT, views INTEGER, likes INTEGER, comment_count INTEGER)''')
        self.conn.commit()

    def authenticate(self):
        scopes = ['https://www.googleapis.com/auth/youtube.readonly']
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                credentials = pickle.load(token)
        else:
            flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(self.client_secrets_file, scopes)
            credentials = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                pickle.dump(credentials, token)
        self.youtube = googleapiclient.discovery.build('youtube', 'v3', credentials=credentials)

    def get_total_subscription_count(self):
        count = 0
        next_page_token = None
        while True:
            subscriptions = self.youtube.subscriptions().list(
                part='id', mine=True, maxResults=50, pageToken=next_page_token
            ).execute()
            count += len(subscriptions['items'])
            next_page_token = subscriptions.get('nextPageToken')
            if not next_page_token:
                break
        return count

    def fetch_and_store_data(self):
        processed_channels = self.load_processed_channels()
        total_subscriptions = self.get_total_subscription_count()
        
        try:
            next_page_token = None
            while True:
                subscriptions = self.youtube.subscriptions().list(
                    part='snippet', mine=True, maxResults=50, pageToken=next_page_token
                ).execute()

                for item in subscriptions['items']:
                    channel_id = item['snippet']['resourceId']['channelId']
                    if channel_id not in processed_channels:
                        self.process_subscription(item)
                        processed_channels.add(channel_id)

                next_page_token = subscriptions.get('nextPageToken')
                if not next_page_token:
                    break

            if len(processed_channels) >= total_subscriptions:
                # 모든 구독 채널을 처리했으므로 리스트를 초기화합니다.
                processed_channels.clear()

            self.save_processed_channels(processed_channels)
            logging.info("Data fetching and storage completed successfully.")
            
        except googleapiclient.errors.HttpError as e:
            logging.error(f"An HTTP error {e.resp.status} occurred:\n{e.content}")
            print(f"An HTTP error {e.resp.status} occurred:\n{e.content}")


    def load_processed_channels(self):
        if os.path.exists('processed_channels.txt'):
            with open('processed_channels.txt', 'r') as file:
                return set(file.read().splitlines())
        return set()

    def save_processed_channels(self, processed_channels):
        with open('processed_channels.txt', 'w') as file:
            for channel_id in processed_channels:
                file.write(channel_id + '\n')

    def process_subscription(self, item):
        channel_id = item['snippet']['resourceId']['channelId']
        channel_title = item['snippet']['title']
        one_year_ago = (datetime.datetime.now() - datetime.timedelta(days=365)).isoformat() + 'Z'
        next_page_token = None
        while True:
            videos_request = self.youtube.search().list(
                part='snippet',
                channelId=channel_id,
                publishedAfter=one_year_ago,
                maxResults=50,
                pageToken=next_page_token,
                type='video'
            ).execute()

            for video in tqdm(videos_request['items'], desc=f'Fetching videos for {channel_title}'):
                self.process_video(channel_title, video)
                time.sleep(0.1)
            next_page_token = videos_request.get('nextPageToken')
            if not next_page_token:
                break
            time.sleep(10)

    def process_video(self, channel_title, video):
        video_id = video['id']['videoId']
        video_title = video['snippet']['title']
        video_details = self.youtube.videos().list(part='statistics', id=video_id).execute()
        if 'items' in video_details and video_details['items']:
            statistics = video_details['items'][0]['statistics']
            self.save_to_database(channel_title, video_title, statistics)
        logging.info(f"Video '{video_title}' data saved successfully.")

    def save_to_database(self, channel_title, video_title, statistics):
        views = statistics.get('viewCount', 0)
        likes = statistics.get('likeCount', 0)
        comment_count = statistics.get('commentCount', 0)
        c = self.conn.cursor()
        c.execute("INSERT INTO videos VALUES (?, ?, ?, ?, ?)", 
                  (channel_title, video_title, views, likes, comment_count))
        self.conn.commit()

    def close(self):
        self.conn.close()

if __name__ == '__main__':
    collector = YouTubeDataCollector('CLIENT_SECRET_FILE.json', 'youtube_data.db')
    collector.authenticate()
    collector.fetch_and_store_data()
    collector.close()
