import requests
import json
from datetime import date
from airflow.models import Variable
from airflow.decorators import task
#import os
#from dotenv import load_dotenv



@task
def get_playlist_id():
    api_key = Variable.get("API_KEY")
    channel_handle = Variable.get("CHANNEL_HANDLE")

    if not api_key or not channel_handle:
        raise ValueError("Both API_KEY and CHANNEL_HANDLE Airflow Variables must be set.")

    try: 
        url = f"""https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"""

        response = requests.get(url)

        response.raise_for_status()
        data = response.json()

        playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        return playlist_id
    
    except requests.exceptions.RequestException as e:
        raise e
    

@task
def get_videos_id(playlist_id):
    api_key = Variable.get("API_KEY")
    maxResults = 50

    video_ids = []
    pageToken = None
    base_url = f"""https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlist_id}&key={api_key}"""

    try:
        
        while True:

            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"
            
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items', []):
                video_ids.append(item['contentDetails']['videoId'])
            
            pageToken = data.get('nextPageToken')
            if not pageToken:
                break
        
        return video_ids
    
    except requests.exceptions.RequestException as e:
        raise e
    

@task
def extract_video_details(video_ids):
   api_key = Variable.get("API_KEY")
   maxResults = 50
   
   extracted_data = []

   def batch_list(video_ids_list, batch_size):
       for video_id in range(0, len(video_ids_list), batch_size):
           yield video_ids_list[video_id:video_id + batch_size]


   try:
       
       for batch in batch_list(video_ids, maxResults):
           video_ids_str = ",".join(batch)

           url = f"""https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=statistics&part=snippet&id={video_ids_str}&key={api_key}"""
           response = requests.get(url)
           response.raise_for_status()

           data = response.json()

           for item in data.get('items', []):
               video_id = item['id']
               snippet = item['snippet']
               contentDetails = item['contentDetails']
               statistics = item['statistics']

               video_data = {
                   "video_id": video_id,
                   "title": snippet["title"],
                   "publishedAt": snippet["publishedAt"],
                   "duration": contentDetails["duration"],
                   "viewCount": statistics.get("viewCount", None),
                   "likeCount": statistics.get("likeCount", None),
                   "commentCount": statistics.get("commentCount", None)
               }

               extracted_data.append(video_data)


       return extracted_data
   
   except requests.exceptions.RequestException as e:
       raise e

@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
    

""" if __name__ == "__main__":
    playlist_id= get_playlist_id()
    video_ids = get_videos_id(playlist_id)
    video_data = extract_video_details(video_ids)
    save_to_json(video_data) """