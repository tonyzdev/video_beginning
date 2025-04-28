from bilibili_utils import av2bv
import pandas as pd
import requests
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from tqdm import tqdm
import time


class BilibiliDownloader:
    def __init__(self, base_path, max_workers=8):
        """
        Initialize the downloader with base path and number of worker threads
        """
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.bilibili.com'
        }
        self.base_path = base_path
        self.max_workers = max_workers
        self.download_records = []
    
    def sanitize_filename(self, filename):
        """Remove illegal characters from filename"""
        return re.sub(r'[\\/:*?"<>|]', '_', filename)
    
    def get_subtitle_urls(self, bvid, cid):
        """Get subtitle information for a video"""
        url = f'https://api.bilibili.com/x/player/v2?bvid={bvid}&cid={cid}'
        response = requests.get(url, headers=self.headers)
        subtitle_urls = []
        
        if response.status_code == 200:
            data = response.json()
            if data['code'] == 0:
                subtitle_info = data['data']['subtitle'].get('subtitles', [])
                for subtitle in subtitle_info:
                    subtitle_url = 'https:' + subtitle['subtitle_url']
                    language = subtitle['lan_doc']
                    subtitle_urls.append((subtitle_url, language))
        
        return subtitle_urls

    def download_subtitle(self, subtitle_url, output_path, language):
        """Download and convert subtitle to txt format"""
        try:
            response = requests.get(subtitle_url, headers=self.headers)
            if response.status_code == 200:
                subtitle_data = response.json()
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    for line in subtitle_data['body']:
                        content = line['content']
                        f.write(f"{content}\n")
                
                return True
            return False
        except Exception as e:
            print(f'Subtitle download failed: {language}, Error: {str(e)}')
            return False

    def get_video_info(self, bvid):
        """Get video information"""
        url = f'https://api.bilibili.com/x/web-interface/view?bvid={bvid}'
        response = requests.get(url, headers=self.headers)
        return response.json()
    
    def get_play_url(self, bvid, cid):
        """Get video playback URL"""
        url = f'https://api.bilibili.com/x/player/playurl?bvid={bvid}&cid={cid}&qn=80&fnval=16'
        response = requests.get(url, headers=self.headers)
        return response.json()
    
    def download_file(self, url, filename):
        """Download file and return status"""
        try:
            response = requests.get(url, headers=self.headers, stream=True)
            total_size = int(response.headers.get('content-length', 0))
            
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True
        except Exception as e:
            print(f"Download failed: {filename}, Error: {str(e)}")
            return False
    
    def merge_audio_video(self, video_file, audio_file, output_file):
        """Merge audio and video files"""
        os.system(f'ffmpeg -i "{video_file}" -i "{audio_file}" -c copy "{output_file}"')
        os.remove(video_file)
        os.remove(audio_file)

    def download_single_video(self, bvid):
        """Download all content (video, audio, subtitles) for a single video"""
        record = {
            'bvid': bvid,
            'status': 'skipped',
            'video_downloaded': False,
            'audio_downloaded': False,
            'subtitle_count': 0,
            'error_message': '',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        try:
            video_info = self.get_video_info(bvid)
            if video_info['code'] != 0:
                record['error_message'] = 'Failed to get video info'
                record['status'] = 'failed'
                return record
            
            data = video_info['data']
            cid = data['cid']
            
            output_dir = os.path.join(self.base_path, bvid)
            os.makedirs(output_dir, exist_ok=True)
            
            video_file = os.path.join(output_dir, f'{bvid}_video.mp4')
            audio_file = os.path.join(output_dir, f'{bvid}_audio.mp4')
            
            if os.path.exists(video_file) and os.path.exists(audio_file):
                return record
            
            play_info = self.get_play_url(bvid, cid)
            if play_info['code'] != 0:
                record['error_message'] = 'Failed to get play URL'
                record['status'] = 'failed'
                return record
            
            dash = play_info['data']['dash']
            video_url = dash['video'][0]['baseUrl']
            audio_url = dash['audio'][0]['baseUrl']
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                video_future = executor.submit(self.download_file, video_url, video_file)
                audio_future = executor.submit(self.download_file, audio_url, audio_file)
                
                record['video_downloaded'] = video_future.result()
                record['audio_downloaded'] = audio_future.result()
            
            subtitle_urls = self.get_subtitle_urls(bvid, cid)
            if subtitle_urls:
                for subtitle_url, language in subtitle_urls:
                    subtitle_file = os.path.join(output_dir, f'{bvid}_{language}.txt')
                    if self.download_subtitle(subtitle_url, subtitle_file, language):
                        record['subtitle_count'] += 1
            
            if record['video_downloaded'] and record['audio_downloaded']:
                record['status'] = 'success'
            else:
                record['status'] = 'partial'
                
        except Exception as e:
            record['error_message'] = str(e)
            record['status'] = 'failed'
        
        return record

    def batch_download(self, df):
        """Parallel batch download videos"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_bvid = {
                executor.submit(self.download_single_video, row['bvid']): row['bvid'] 
                for _, row in df.iterrows()
            }
            
            with tqdm(total=len(df), desc="Download Progress") as pbar:
                for future in as_completed(future_to_bvid):
                    bvid = future_to_bvid[future]
                    try:
                        record = future.result()
                        self.download_records.append(record)
                        self.save_records()
                    except Exception as e:
                        print(f"Error downloading {bvid}: {str(e)}")
                    finally:
                        pbar.update(1)
    
    def save_records(self):
        """Save download records to CSV"""
        records_df = pd.DataFrame(self.download_records)
        records_df.to_csv(os.path.join(self.base_path, 'download_records.csv'), index=False)
    
    def get_statistics(self):
        """Get download statistics"""
        records_df = pd.DataFrame(self.download_records)
        stats = {
            'total_videos': len(records_df),
            'successful_downloads': len(records_df[records_df['status'] == 'success']),
            'partial_downloads': len(records_df[records_df['status'] == 'partial']),
            'failed_downloads': len(records_df[records_df['status'] == 'failed']),
            'total_subtitles': records_df['subtitle_count'].sum()
        }
        return stats

if __name__ == '__main__':
    # Read data
    data_path = 'cleaned_data.csv'
    video_store_path = '/Volumes/externalssd/bilibili/'

    data = pd.read_csv(data_path)
    data['bvid'] = data['avid'].apply(av2bv)
    data.drop(columns=['avid', 'Unnamed: 0'], inplace=True)
    
    # Initialize downloader with 32 parallel download threads
    downloader = BilibiliDownloader(base_path=video_store_path, max_workers=32)
    
    # Start batch download
    print("Starting video downloads...")
    downloader.batch_download(data)
    
    # Get and print statistics
    stats = downloader.get_statistics()
    print("\nDownload Statistics:")
    print(f"Total videos: {stats['total_videos']}")
    print(f"Successful downloads: {stats['successful_downloads']}")
    print(f"Partial downloads: {stats['partial_downloads']}")
    print(f"Failed downloads: {stats['failed_downloads']}")
    print(f"Total subtitles: {stats['total_subtitles']}") 