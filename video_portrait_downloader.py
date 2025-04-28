import pandas as pd
import numpy as np
import os
import random
from tqdm import tqdm
import cv2
import time
import datetime
from bilibili_api_client import BilibiliDownloader

# 记录开始时间
start_time = time.time()
start_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"下载任务开始时间: {start_time_str}")

# 读取 parquet 文件
df = pd.read_parquet('step_1_df.parquet')

# 提取唯一的 bvid
unique_bvids = df['bvid'].unique().tolist()

# 随机打乱 bvids
random.shuffle(unique_bvids)

# 存储已下载的视频信息
downloaded_videos = []
# 检查是否有之前的保存进度
progress_file = 'portrait_videos_progress.csv'
if os.path.exists(progress_file):
    try:
        progress_df = pd.read_csv(progress_file)
        downloaded_videos = progress_df['bvid'].tolist()
        print(f"已从进度文件加载 {len(downloaded_videos)} 个下载记录")
    except Exception as e:
        print(f"读取进度文件失败: {e}")

target_count = 2146  # 目标下载数量
batch_size = 100  # 每批下载数量
base_path = '/Volumes/externalssd/video_data'  # 视频保存路径

# 创建下载器
downloader = BilibiliDownloader(base_path=base_path)

# 创建保存目录
os.makedirs(base_path, exist_ok=True)

# 检查已存在的视频文件
def get_existing_videos(directory):
    existing_videos = set()
    if os.path.exists(directory):
        for bvid in os.listdir(directory):
            video_dir = os.path.join(directory, bvid)
            if os.path.isdir(video_dir):
                # 检查是否有完整视频文件
                mp4_files = [f for f in os.listdir(video_dir) if f.endswith('.mp4') and not (f.endswith('_video.mp4') or f.endswith('_audio.mp4'))]
                if mp4_files:
                    existing_videos.add(bvid)
    return existing_videos

# 获取已存在的视频列表
print("正在检查已存在的视频文件...")
existing_videos = get_existing_videos(base_path)
print(f"找到 {len(existing_videos)} 个已存在的视频")


# 检查视频是否为竖屏（宽高比 <= 1）
def is_portrait_video(video_path):
    try:
        if not os.path.exists(video_path):
            print(f"视频文件不存在: {video_path}")
            return False
            
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            print(f"无法打开视频文件: {video_path}")
            return False
        
        # 尝试读取第一帧
        ret, _ = cap.read()
        if not ret:
            print(f"无法读取视频帧: {video_path}")
            cap.release()
            return False
            
        width = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
        height = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
        cap.release()
        
        if width <= 0 or height <= 0:
            print(f"视频维度无效: {video_path}, 宽={width}, 高={height}")
            return False
            
        aspect_ratio = width / height
        print(f"视频: {video_path}, 宽高比: {aspect_ratio:.2f} ({width}x{height})")
        return aspect_ratio <= 1  # 竖屏视频
    except Exception as e:
        print(f"检查视频尺寸时出错: {video_path}, 错误: {e}")
        return False

# 检查已存在的视频是否为竖屏
print("检查已存在视频的宽高比...")
for bvid in tqdm(existing_videos):
    if len(downloaded_videos) >= target_count:
        break
        
    if bvid in downloaded_videos:
        continue
        
    video_dir = os.path.join(base_path, bvid)
    mp4_files = [f for f in os.listdir(video_dir) if f.endswith('.mp4') and not (f.endswith('_video.mp4') or f.endswith('_audio.mp4'))]
    
    if mp4_files:
        video_path = os.path.join(video_dir, mp4_files[0])
        if is_portrait_video(video_path):
            downloaded_videos.append(bvid)
            print(f"已有竖屏视频: {bvid}, 当前总数: {len(downloaded_videos)}/{target_count}")

# 如果已经达到目标数量，提前结束
if len(downloaded_videos) >= target_count:
    print(f"已有足够的竖屏视频 ({len(downloaded_videos)}/{target_count})，无需下载")
    # 保存结果
    result_df = pd.DataFrame({'bvid': downloaded_videos})
    result_df.to_csv('portrait_videos.csv', index=False)
    exit(0)

# 保存进度的函数
def save_progress():
    progress_df = pd.DataFrame({'bvid': downloaded_videos})
    progress_df.to_csv(progress_file, index=False)
    print(f"进度已保存，当前已下载 {len(downloaded_videos)} 个视频")

# 定义上次保存时间
last_save_time = time.time()
save_interval = 300  # 每5分钟保存一次进度


# 开始下载过程
current_batch_number = 1

while len(downloaded_videos) < target_count and unique_bvids:
    # 获取当前批次的BVIDs
    current_batch = unique_bvids[:batch_size]
    unique_bvids = unique_bvids[batch_size:]
    
    print(f"下载批次 #{current_batch_number}，当前进度: {len(downloaded_videos)}/{target_count}，本批次大小: {len(current_batch)}")
    current_batch_number += 1
    
    # 创建批次数据框
    batch_df = pd.DataFrame({'bvid': current_batch})
    
    # 过滤掉已经存在的视频
    new_batch = []
    for bvid in current_batch:
        if bvid in existing_videos:
            print(f"视频已存在，跳过下载: {bvid}")
        elif bvid in downloaded_videos:
            print(f"视频已处理，跳过下载: {bvid}")
        else:
            new_batch.append(bvid)
    
    if not new_batch:
        print(f"批次 #{current_batch_number} 中所有视频都已存在或处理，跳过下载")
        continue
    
    print(f"批次 #{current_batch_number} 过滤后需要下载的视频数: {len(new_batch)}/{len(current_batch)}")
    batch_df = pd.DataFrame({'bvid': new_batch})
    
    try:
        # 下载当前批次
        downloader.batch_download(batch_df)
    except Exception as e:
        print(f"批次下载过程中出错: {e}")
        # 继续处理已下载的内容
    
    # 检查是否创建了视频目录
    if not os.path.exists(base_path):
        print(f"视频保存目录不存在: {base_path}")
        os.makedirs(base_path, exist_ok=True)
        
    # 检查已下载的视频，筛选竖屏视频
    print(f"开始筛选本批次视频...")
    successful_in_batch = 0
    
    for bvid in tqdm(new_batch, desc="筛选竖屏视频"):
        video_dir = os.path.join(base_path, bvid)
        # 检查下载的视频是否存在
        if not os.path.exists(video_dir):
            continue
            
        # 寻找合并后的MP4文件
        mp4_files = [f for f in os.listdir(video_dir) if f.endswith('.mp4') and not (f.endswith('_video.mp4') or f.endswith('_audio.mp4'))]
        
        video_path = None
        
        # 如果没有合并文件，直接使用视频文件检查是否为竖屏，不再合并
        if not mp4_files and os.path.exists(os.path.join(video_dir, f'{bvid}_video.mp4')):
            video_path = os.path.join(video_dir, f'{bvid}_video.mp4')
        elif mp4_files:
            video_path = os.path.join(video_dir, mp4_files[0])
            
        if video_path is None or not os.path.exists(video_path):
            print(f"视频文件不存在: {bvid}")
            continue
        
        if is_portrait_video(video_path):
            downloaded_videos.append(bvid)
            print(f"保留竖屏视频: {bvid}, 当前总数: {len(downloaded_videos)}/{target_count}")
            successful_in_batch += 1
            
            # 定期保存进度
            current_time = time.time()
            if current_time - last_save_time > save_interval:
                save_progress()
                last_save_time = current_time
        else:
            # 删除非竖屏视频前检查文件是否存在
            print(f"检测到非竖屏视频: {bvid}")
            try:
                if os.path.exists(video_path):
                    os.remove(video_path)
                    print(f"已删除非竖屏视频: {bvid}")
                    
                    # 删除视频所在文件夹
                    video_dir = os.path.dirname(video_path)
                    if os.path.exists(video_dir):
                        import shutil
                        shutil.rmtree(video_dir)
                        print(f"已删除视频文件夹: {video_dir}")
            except Exception as e:
                print(f"删除文件失败: {video_path}, 错误: {e}")
        
        # 如果已经达到目标数量，退出循环
        if len(downloaded_videos) >= target_count:
            break

    print(f"批次 #{current_batch_number - 1} 完成，成功下载 {successful_in_batch} 个竖屏视频")
    save_progress()  # 每批次结束后保存进度

print(f"下载完成，共下载 {len(downloaded_videos)} 个竖屏视频")

# 记录结束时间和总耗时
end_time = time.time()
end_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
duration = end_time - start_time
hours, remainder = divmod(duration, 3600)
minutes, seconds = divmod(remainder, 60)
print(f"下载任务结束时间: {end_time_str}")
print(f"总耗时: {int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒")
print(f"平均每个视频耗时: {duration/max(1, len(downloaded_videos)):.2f}秒") 