import json
import gspread
import requests
import datetime
import re
from data_loader import DataLoader
from schemas import ChannelDetails, DiscoveredDomains
from datetime import timedelta, datetime
from urllib.parse import urlparse
from urllib.parse import parse_qs
import time
import random



class Parser:
    base_search_url = 'https://www.googleapis.com/youtube/v3/'

    def raise_total_exception(self, exception):
        print(f'videoCategories?part=snippet&hl=ru&regionCode=ru&key={exception}')
        exit(1)

    def __init__(self):
        # Opening JSON file
        self.keys = []
        self.initial_scan_table = {}
        self.table_discovered_domains = {}

    def get_key(self):
        try:
            with open('config.json') as f:
                data = json.load(f)
                key1 = str(data["google_api_keys"]["key1"])
                key2 = str(data["google_api_keys"]["key2"])
                google_api_key = random.randint(1, 2)
                if google_api_key == 1:
                    print(key1)
                    return key1
                elif google_api_key == 2:
                    print(key2)
                    return key2
                else:
                    print('error get api key')
        except KeyError:
            self.raise_total_exception('ключ не найден')
        return key


    def get_video_categories(self, key):
        url = self.base_search_url + f'videoCategories?part=snippet&hl=ru&regionCode=ru&key={key}'
        response = requests.get(url)
        if response.status_code != 200:
            self.raise_total_exception(
                f"Запрос категорий видео не вернул категории\n{response.text}\nИспользованный ключ: {key}")
        return response.json()

    def get_channel_info(self, key, channel_id):
        url = self.base_search_url + f'channels?part=snippet,statistics&id={channel_id}&key={key}'
        response = requests.get(url)
        if response.status_code != 200:
            self.raise_total_exception(
                f"Ошибка получения инфы о канале - get_channel_info. Status code: {response.status_code}. Response: {response.text}")

        channel_data = response.json()
        items = channel_data.get('items', [])
        if not items:
            self.raise_total_exception(f"Канал с айди '{channel_id}' не найден или недоступен.")

        channel_info = items[0]
        subscriber_count = channel_info['statistics']['subscriberCount']
        custom_url = channel_info['snippet']['customUrl'] if 'customUrl' in channel_info['snippet'] else ['Никого нет дома']

        return subscriber_count, custom_url

    def get_latest_videos(self, key, channel_id):
        url = f'{self.base_search_url}search?part=snippet&channelId={channel_id}&maxResults=50&order=date&type=video&key={key}'
        response = requests.get(url)

        if response.status_code != 200:
            self.raise_total_exception(
                f"Ошибка получения видосов. Status code: {response.status_code}. Response: {response.text}")

        videos_data = response.json()
        items = videos_data.get('items', [])

        video_ids = [item['id']['videoId'] for item in items]  # Получить идентификаторы всех видеороликов

        return video_ids[-50:]  # Вернуть только последние 50 видеороликов (число дорлжно меняться в энвайромент)

    def get_video_details(self, key, video_ids):
        video_ids_str = ','.join(video_ids)
        url = f'{self.base_search_url}videos?part=snippet,contentDetails,statistics&id={video_ids_str}&key={key}'
        response = requests.get(url)

        if response.status_code != 200:
            self.raise_total_exception(
                f"Ошибка получения описания видосов. Status code: {response.status_code}. Response: {response.text}")

        videos_data = response.json()
        items = videos_data.get('items', [])
        video_details = []
        for item in items:
            video_id = item['id']
            try:
                published_at = item['snippet']['publishedAt']
            except:
                published_at = item.get('publishedAt', ['Нет'])
            channel_id = item['snippet']['channelId']
            title = item['snippet']['title']
            description = item['snippet']['description']
            thumbnails = item['snippet'].get('thumbnails',  'url')
            maxres = item['snippet'].get('maxres')
            channel_title = item['snippet']['channelTitle']
            try:
                tags = item['snippet'].get('tags')
            except:
                tags = item['snippet'].get('tags', ['none'])
            category_id = item['snippet']['categoryId']
            default_language = item['snippet'].get('defaultLanguage', 'Unknown')  # иногда пустуют
            duration = item['contentDetails']['duration']
            try:
                view_count = item['statistics']['viewCount']
            except:
                view_count = item['statistics'].get('viewCount', 0)
            try:
                like_count = item['statistics']['likeCount']
            except:
                like_count = item['statistics'].get('likeCount', 0)
            try:
                comment_count = item['statistics']['commentCount']
            except:
                comment_count = item['statistics'].get('commentCount', 0)

            video_details.append({
                'video_id': video_id,
                'published_at': published_at,
                'channel_id': channel_id,
                'title': title,
                'description': description,
                'thumbnails': thumbnails,
                'channel_title': channel_title,
                'tags': tags,
                'category_id': category_id,
                'default_language': default_language,
                'duration': duration,
                'view_count': view_count,
                'like_count': like_count,
                'comment_count': comment_count
            })

        return video_details

    def process_response(self, video_details):
        now = datetime.now()
        seven_days_ago = now - timedelta(days=7)
        one_year_ago = now - timedelta(days=365)

        relevant_videos = []
        total_views = 0
        total_likes = 0

        for video in video_details:
            published_at = datetime.strptime(video['published_at'], '%Y-%m-%dT%H:%M:%SZ')
            view_count = int(video['view_count'])
            like_count = int(video['like_count'])

            if seven_days_ago > published_at > one_year_ago:
                relevant_videos.append(video)
                total_views += view_count
                total_likes += like_count

        count_relevant_videos = len(relevant_videos)
        avg_views = total_views / count_relevant_videos if count_relevant_videos > 0 else 0
        engagement_rate = ((total_views + total_likes) / avg_views) / 100 if avg_views > 0 else 0

        return count_relevant_videos, avg_views, engagement_rate




    def extract_final_url(self, short_url):
        try:
            response = requests.head(short_url, allow_redirects=True)
            return response.url
        except requests.RequestException:
            print(f"Failed to extract final URL for: {short_url}")
            return None

    def get_youtube_channel_related_content(self, channel_id):
        try:
            base_url = 'https://yt.lemnoslife.com'  # Пример базового URL
            api_endpoint = f'/channels?part=about&id={channel_id}'
            url = base_url + api_endpoint
            response = requests.get(url)
            lezu = response.json()
            v_ochko = lezu['items'][0]
            shooby = v_ochko['about']
            tam_uje = shooby['links']
            aidzi = []
            aidzi_string = str()
            if tam_uje is None:
                aidzi_string = str(['Нет контактов'])
            else:
                for sidit in tam_uje:
                    ded = sidit['title'] + ': ' + sidit['url'] + ' '
                    aidzi.extend(ded)
                    aidzi_string = ''.join(aidzi)

            return aidzi_string
        except Exception as ex:
            print(f"Error during related content extraction: {ex}")

    def get_all_data(self, process_response, video_details, channel_id, contacts, subscriber_count,
                     custom_url):
        count_relevant_videos, avg_views, engagement_rate = process_response
        total_tags = []
        tags_string = str()
        for video in video_details:
            try:
                published_at = datetime.strptime(video['published_at'], '%Y-%m-%dT%H:%M:%SZ')
            except:
                published_at = str('')
            try:
                video_title = video.get('title', [])
            except:
                video_title = str('')
            channel_title = video.get('channel_title')
            try:
                tags = video.get('tags', [])
                total_tags.extend(tags)
                tags_string = ', '.join(total_tags)
            except:
                tags_string = str('')

        scan_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')
        except:
            published_at = ('Нет за подходящее время')
        avg_views = avg_views
        engagement_rate = engagement_rate
        channel_data = ChannelDetails(
            id=channel_id,
            title=channel_title,
            video_title=video_title,
            custom_url=custom_url,
            tags=tags_string,
            scan_date=scan_date,
            subs=subscriber_count,
            published_at=published_at,
            avg_views=avg_views,
            er=engagement_rate,
            contacts=contacts
        )
        return channel_data

    def main(self):
        key = self.get_key()
        print('key')
        categories = self.get_video_categories(key)
        print('categories')
        channel_id = DataLoader("12DONCMKhLcjcJ1aNJDBcyVUlp28mrMtOl6Rooj3OAok", 'config').read_periodic_scan_ids()
        print('read_peiodic_scam_ids')
        subscriber_count, custom_url = self.get_channel_info(key, channel_id)
        print('channel_info')
        contacts = self.get_youtube_channel_related_content(channel_id)
        print('channel_related_content')
        video_ids = self.get_latest_videos(key, channel_id)
        print('get_latest_videos')
        video_details = self.get_video_details(key, video_ids)
        print('video_details')
        process_response = self.process_response(video_details)
        print('process_response')

        channel_data = self.get_all_data(process_response, video_details, channel_id, contacts, subscriber_count, custom_url)
        print('get_all_data')
        DataLoader("12DONCMKhLcjcJ1aNJDBcyVUlp28mrMtOl6Rooj3OAok", 'config').write_periodic_scan_data(channel_data)
        print('write_periodic_scan_data')


if __name__ == "__main__":
    parser = Parser()
    while True:
        parser.main()
