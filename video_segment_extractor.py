import os
import subprocess
import glob
import pandas as pd
from tqdm import tqdm
import shutil
import concurrent.futures
import re
import multiprocessing
import time

# 配置选项
CONFIG = {
    'max_workers': min(8, multiprocessing.cpu_count()),  # 自动使用CPU核心数
    'ffmpeg_threads': 2,  # 每个FFmpeg进程使用的线程数
    'durations': [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24],  # 切片持续时间列表
    'timeout': 180,  # FFmpeg命令超时时间(秒)
    'ffmpeg_preset': 'ultrafast',  # FFmpeg编码速度预设 (ultrafast, superfast, veryfast, faster, fast, medium)
    'crf_value': 28,  # 视频质量值 (值越大，质量越低，速度越快)
    'batch_size': 100  # 每批处理的文件数量
}

def slice_video(video_path, output_dir=None, durations=None):
    """
    对视频进行切片，切片从0秒开始，到指定的持续时间结束
    
    Args:
        video_path (str): 视频文件路径
        output_dir (str): 输出目录，默认为视频所在目录
        durations (list): 切片持续时间列表，单位为秒
    
    Returns:
        list: 生成的切片文件路径列表
    """
    start_time = time.time()
    if not os.path.exists(video_path):
        print(f"错误: 文件不存在: {video_path}")
        return []
    
    # 设置默认参数
    if durations is None:
        durations = CONFIG['durations']
    
    if output_dir is None:
        output_dir = os.path.dirname(video_path)
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 获取文件名（不含扩展名）和扩展名
    filename = os.path.basename(video_path)
    name, ext = os.path.splitext(filename)
    
    # 判断是视频还是音频文件
    is_audio = "_audio" in filename
    file_type = "音频" if is_audio else "视频"
    
    # 存储生成的切片文件路径
    output_files = []
    success_count = 0
    
    # 检查哪些切片已经存在
    existing_slices = []
    missing_durations = []
    for duration in durations:
        output_filename = f"{name}_0-{duration}s{ext}"
        output_path = os.path.join(output_dir, output_filename)
        if os.path.exists(output_path):
            existing_slices.append(duration)
            output_files.append(output_path)
            success_count += 1
        else:
            missing_durations.append(duration)
    
    if existing_slices:
        print(f"跳过已存在的 {file_type} 切片: {name} 的 {existing_slices}")
    
    # 如果所有切片都已存在，则直接返回
    if not missing_durations:
        print(f"{file_type} {name} 的所有切片已存在")
        return output_files
    
    # 先检查输入文件是否可读 - 只在必要时检查
    try:
        # 使用更快的FFprobe命令检查文件有效性
        probe_cmd = [
            'ffprobe', 
            '-v', 'error', 
            '-select_streams', 'v:0' if not is_audio else 'a:0',
            '-show_entries', 'stream=duration', 
            '-of', 'csv=p=0',
            video_path
        ]
        result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            print(f"错误: 无法读取文件: {video_path}, 跳过处理")
            return output_files  # 返回已存在的切片
    except Exception as e:
        print(f"错误: 文件检查失败: {video_path}, 错误: {e}")
        return output_files  # 返回已存在的切片
    
    print(f"开始处理 {file_type}: {name} (还需生成 {len(missing_durations)} 个切片)")
    
    # 使用 FFmpeg 进行切片 - 批量模式
    try:
        # 对于音频文件或者低于10秒的短切片，使用一次性切割
        if is_audio or (max(missing_durations) <= 10):
            # 一次性生成所有切片
            for duration in missing_durations:
                output_filename = f"{name}_0-{duration}s{ext}"
                output_path = os.path.join(output_dir, output_filename)
                
                # 根据文件类型使用不同的命令
                if is_audio:
                    # 音频文件 - 使用复制模式，速度更快
                    ffmpeg_cmd = [
                        'ffmpeg', '-y',
                        '-i', video_path,
                        '-ss', '0',
                        '-t', str(duration),
                        '-c:a', 'copy',
                        output_path
                    ]
                else:
                    # 视频文件 - 使用更快的编码设置
                    ffmpeg_cmd = [
                        'ffmpeg', '-y',
                        '-i', video_path,
                        '-ss', '0',
                        '-t', str(duration),
                        '-c:v', 'libx264',
                        '-crf', str(CONFIG['crf_value']),
                        '-preset', CONFIG['ffmpeg_preset'],
                        '-c:a', 'aac',
                        '-threads', str(CONFIG['ffmpeg_threads']),
                        output_path
                    ]
                
                try:
                    # 执行 FFmpeg 命令
                    result = subprocess.run(ffmpeg_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=CONFIG['timeout'])
                    output_files.append(output_path)
                    success_count += 1
                except Exception as e:
                    print(f"错误: {file_type}切片失败: {filename} 的 0-{duration}秒: {e}")
        else:
            # 对于视频文件和较长切片，使用更复杂的流程
            # 先一次性截取最长的部分，然后分割成多个短片段
            max_duration = max(missing_durations)
            temp_file = os.path.join(output_dir, f"{name}_temp_0-{max_duration}s{ext}")
            
            # 1. 提取最长部分
            extract_cmd = [
                'ffmpeg', '-y',
                '-i', video_path,
                '-ss', '0',
                '-t', str(max_duration),
                '-c:v', 'libx264',
                '-crf', str(CONFIG['crf_value']),
                '-preset', CONFIG['ffmpeg_preset'],
                '-c:a', 'aac',
                '-threads', str(CONFIG['ffmpeg_threads']),
                temp_file
            ]
            
            try:
                # 执行FFmpeg提取命令
                result = subprocess.run(extract_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=CONFIG['timeout'])
                
                # 2. 从临时文件中切割各个小片段
                for duration in missing_durations:
                    output_filename = f"{name}_0-{duration}s{ext}"
                    output_path = os.path.join(output_dir, output_filename)
                    
                    # 从临时文件中复制指定时长
                    segment_cmd = [
                        'ffmpeg', '-y',
                        '-i', temp_file,
                        '-ss', '0',
                        '-t', str(duration),
                        '-c', 'copy',  # 直接复制，非常快
                        output_path
                    ]
                    
                    try:
                        result = subprocess.run(segment_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=CONFIG['timeout'])
                        output_files.append(output_path)
                        success_count += 1
                    except Exception as e:
                        print(f"错误: 从临时文件切片失败: {filename} 的 0-{duration}秒: {e}")
                
                # 删除临时文件
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            
            except Exception as e:
                print(f"错误: 提取临时文件失败: {filename}, 错误: {e}")
                # 如果临时文件提取失败，回退到一个一个单独处理
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                
                # 回退方案：单独处理每个切片
                for duration in missing_durations:
                    output_filename = f"{name}_0-{duration}s{ext}"
                    output_path = os.path.join(output_dir, output_filename)
                    
                    fallback_cmd = [
                        'ffmpeg', '-y',
                        '-i', video_path,
                        '-ss', '0',
                        '-t', str(duration),
                        '-c:v', 'libx264',
                        '-crf', str(CONFIG['crf_value']),
                        '-preset', CONFIG['ffmpeg_preset'],
                        '-c:a', 'aac',
                        '-threads', str(CONFIG['ffmpeg_threads']),
                        output_path
                    ]
                    
                    try:
                        result = subprocess.run(fallback_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=CONFIG['timeout'])
                        output_files.append(output_path)
                        success_count += 1
                    except Exception as e2:
                        print(f"错误: 回退方案也失败: {filename} 的 0-{duration}秒: {e2}")
    
    except Exception as e:
        print(f"错误: 处理文件时出现异常: {filename}, 错误: {e}")
    
    # 报告成功率
    if durations:
        success_rate = success_count / len(durations) * 100
        elapsed_time = time.time() - start_time
        print(f"{file_type}切片完成: {name}, {success_count}/{len(durations)} 成功 ({success_rate:.1f}%)，耗时: {elapsed_time:.1f}秒")
    
    return output_files

def process_video_worker(file_path):
    """处理单个视频或音频的工作函数，用于并行处理"""
    try:
        return slice_video(file_path), None
    except Exception as e:
        return None, (file_path, str(e))

def process_files_batch(file_paths, max_workers=None):
    """批量处理文件"""
    if max_workers is None:
        max_workers = CONFIG['max_workers']
    
    errors = []
    results = []
    
    with tqdm(total=len(file_paths), desc="批量处理进度") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_path = {executor.submit(process_video_worker, path): path for path in file_paths}
            for future in concurrent.futures.as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    result, error = future.result()
                    if error:
                        errors.append(error)
                    else:
                        results.extend(result or [])
                except Exception as exc:
                    print(f'{path} 生成的错误: {exc}')
                    errors.append((path, str(exc)))
                finally:
                    pbar.update(1)
    
    # 报告错误
    if errors:
        print(f"处理过程中遇到 {len(errors)} 个错误:")
        for path, error in errors[:5]:  # 仅显示前5个错误
            print(f"- {path}: {error}")
        if len(errors) > 5:
            print(f"... 以及其他 {len(errors) - 5} 个错误")
    
    return results

def process_all_videos(base_dir='/Volumes/externalssd/video_data', video_pattern='*_video.mp4', audio_pattern='*_audio.mp4', max_workers=None):
    """处理指定目录中的所有视频和音频文件"""
    start_time = time.time()
    
    if max_workers is None:
        max_workers = CONFIG['max_workers']
    
    # 查找所有视频文件
    video_files = []
    audio_files = []
    
    for root, _, _ in os.walk(base_dir):
        # 查找视频文件
        for video_file in glob.glob(os.path.join(root, video_pattern)):
            video_files.append(video_file)
        
        # 查找音频文件
        for audio_file in glob.glob(os.path.join(root, audio_pattern)):
            audio_files.append(audio_file)
    
    print(f"找到 {len(video_files)} 个视频文件和 {len(audio_files)} 个音频文件")
    
    # 分批处理视频文件
    batch_size = CONFIG['batch_size']
    total_video_slices = []
    total_audio_slices = []
    
    for i in range(0, len(video_files), batch_size):
        print(f"处理视频批次 {i//batch_size + 1}/{(len(video_files) + batch_size - 1)//batch_size}")
        batch = video_files[i:i+batch_size]
        slices = process_files_batch(batch, max_workers)
        total_video_slices.extend(slices)
    
    for i in range(0, len(audio_files), batch_size):
        print(f"处理音频批次 {i//batch_size + 1}/{(len(audio_files) + batch_size - 1)//batch_size}")
        batch = audio_files[i:i+batch_size]
        slices = process_files_batch(batch, max_workers)
        total_audio_slices.extend(slices)
    
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    print(f"处理完成! 总耗时: {int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒")
    print(f"生成了 {len(total_video_slices)} 个视频切片和 {len(total_audio_slices)} 个音频切片")
    
    return total_video_slices, total_audio_slices

def process_from_csv(csv_file='portrait_videos.csv', video_dir='/Volumes/externalssd/video_data', max_workers=None):
    """从CSV文件中读取BV号，处理对应目录中的视频和音频文件"""
    start_time = time.time()
    
    if max_workers is None:
        max_workers = CONFIG['max_workers']
    
    # 读取CSV文件
    try:
        df = pd.read_csv(csv_file)
        bvids = df['bvid'].tolist()
        print(f"从CSV文件中读取了 {len(bvids)} 个BV号")
    except Exception as e:
        print(f"读取CSV文件失败: {e}")
        return [], []
    
    # 查找所有视频和音频文件
    video_files = []
    audio_files = []
    
    with tqdm(total=len(bvids), desc="查找文件") as pbar:
        for bvid in bvids:
            bvid_dir = os.path.join(video_dir, bvid)
            if not os.path.exists(bvid_dir):
                pbar.update(1)
                continue
            
            # 查找视频文件
            video_file = find_video_in_dir(bvid_dir)
            if video_file:
                video_files.append(video_file)
            
            # 查找音频文件
            audio_file = find_audio_in_dir(bvid_dir)
            if audio_file:
                audio_files.append(audio_file)
            
            pbar.update(1)
    
    print(f"找到 {len(video_files)} 个视频文件和 {len(audio_files)} 个音频文件")
    
    # 分批处理文件
    batch_size = CONFIG['batch_size']
    total_video_slices = []
    total_audio_slices = []
    
    for i in range(0, len(video_files), batch_size):
        print(f"处理视频批次 {i//batch_size + 1}/{(len(video_files) + batch_size - 1)//batch_size}")
        batch = video_files[i:i+batch_size]
        slices = process_files_batch(batch, max_workers)
        total_video_slices.extend(slices)
    
    for i in range(0, len(audio_files), batch_size):
        print(f"处理音频批次 {i//batch_size + 1}/{(len(audio_files) + batch_size - 1)//batch_size}")
        batch = audio_files[i:i+batch_size]
        slices = process_files_batch(batch, max_workers)
        total_audio_slices.extend(slices)
    
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    print(f"处理完成! 总耗时: {int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒")
    print(f"生成了 {len(total_video_slices)} 个视频切片和 {len(total_audio_slices)} 个音频切片")
    
    return total_video_slices, total_audio_slices

def process_from_parquet(parquet_file='step_1_df.parquet', video_dir='/Volumes/externalssd/video_data', max_workers=None):
    """从Parquet文件中读取BV号，处理对应目录中的视频和音频文件"""
    start_time = time.time()
    
    if max_workers is None:
        max_workers = CONFIG['max_workers']
    
    # 读取Parquet文件
    try:
        df = pd.read_parquet(parquet_file)
        bvids = df['bvid'].unique().tolist()
        print(f"从Parquet文件中读取了 {len(bvids)} 个唯一BV号")
    except Exception as e:
        print(f"读取Parquet文件失败: {e}")
        return [], []
    
    # 查找所有视频和音频文件
    video_files = []
    audio_files = []
    found_bvids = set()
    
    with tqdm(total=len(bvids), desc="查找文件") as pbar:
        for bvid in bvids:
            bvid_dir = os.path.join(video_dir, bvid)
            if not os.path.exists(bvid_dir):
                pbar.update(1)
                continue
            
            # 查找视频文件
            video_file = find_video_in_dir(bvid_dir)
            if video_file:
                video_files.append(video_file)
                found_bvids.add(bvid)
            
            # 查找音频文件
            audio_file = find_audio_in_dir(bvid_dir)
            if audio_file:
                audio_files.append(audio_file)
            
            pbar.update(1)
    
    print(f"在视频目录中找到 {len(found_bvids)}/{len(bvids)} 个BV号的视频")
    print(f"找到 {len(video_files)} 个视频文件和 {len(audio_files)} 个音频文件")
    
    # 分批处理文件
    batch_size = CONFIG['batch_size']
    total_video_slices = []
    total_audio_slices = []
    
    for i in range(0, len(video_files), batch_size):
        print(f"处理视频批次 {i//batch_size + 1}/{(len(video_files) + batch_size - 1)//batch_size}")
        batch = video_files[i:i+batch_size]
        slices = process_files_batch(batch, max_workers)
        total_video_slices.extend(slices)
    
    for i in range(0, len(audio_files), batch_size):
        print(f"处理音频批次 {i//batch_size + 1}/{(len(audio_files) + batch_size - 1)//batch_size}")
        batch = audio_files[i:i+batch_size]
        slices = process_files_batch(batch, max_workers)
        total_audio_slices.extend(slices)
    
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    print(f"处理完成! 总耗时: {int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒")
    print(f"生成了 {len(total_video_slices)} 个视频切片和 {len(total_audio_slices)} 个音频切片")
    
    return total_video_slices, total_audio_slices

def clean_all_slices(base_dir='/Volumes/externalssd/video_data', dry_run=False):
    """删除所有生成的切片文件，不删除原始视频和音频文件"""
    start_time = time.time()
    
    # 查找所有切片文件
    slice_patterns = []
    for duration in CONFIG['durations']:
        # 视频切片
        slice_patterns.append(f"*_0-{duration}s.mp4")
        # 音频切片
        slice_patterns.append(f"*_audio_0-{duration}s.mp4")
    
    total_files = 0
    total_size = 0
    files_to_delete = []
    
    with tqdm(desc="查找切片文件") as pbar:
        for root, _, _ in os.walk(base_dir):
            for pattern in slice_patterns:
                for file_path in glob.glob(os.path.join(root, pattern)):
                    try:
                        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                        total_size += file_size
                        total_files += 1
                        files_to_delete.append((file_path, file_size))
                        pbar.update(1)
                    except Exception as e:
                        print(f"获取文件信息失败: {file_path}, 错误: {e}")
    
    print(f"找到 {total_files} 个切片文件，总大小: {total_size:.2f} MB")
    
    if dry_run:
        print("仅测试运行，未删除任何文件")
        return
    
    # 删除文件
    deleted_files = 0
    deleted_size = 0
    
    with tqdm(total=len(files_to_delete), desc="删除切片文件") as pbar:
        for file_path, file_size in files_to_delete:
            try:
                os.remove(file_path)
                deleted_files += 1
                deleted_size += file_size
            except Exception as e:
                print(f"删除文件失败: {file_path}, 错误: {e}")
            finally:
                pbar.update(1)
    
    elapsed_time = time.time() - start_time
    print(f"已删除 {deleted_files}/{total_files} 个切片文件，释放 {deleted_size:.2f} MB 空间")
    print(f"清理耗时: {elapsed_time:.2f} 秒")

def clean_previous_slices(base_dir='/Volumes/externalssd/video_data'):
    """删除先前生成的切片文件，包括临时文件"""
    # 删除临时文件
    clean_patterns = ['*_temp_*.mp4']
    
    for root, _, _ in os.walk(base_dir):
        for pattern in clean_patterns:
            for file_path in glob.glob(os.path.join(root, pattern)):
                try:
                    os.remove(file_path)
                    print(f"已删除临时文件: {file_path}")
                except Exception as e:
                    print(f"删除文件失败: {file_path}, 错误: {e}")

def find_video_in_dir(bvid_dir):
    """查找目录中的视频文件，优先查找合并后的MP4文件，其次是视频轨道文件"""
    # 查找合并后的MP4文件
    mp4_files = [f for f in os.listdir(bvid_dir) if f.endswith('.mp4') and not (f.endswith('_video.mp4') or f.endswith('_audio.mp4'))]
    if mp4_files:
        return os.path.join(bvid_dir, mp4_files[0])
    
    # 查找视频轨道文件
    video_file = os.path.join(bvid_dir, f"{os.path.basename(bvid_dir)}_video.mp4")
    if os.path.exists(video_file):
        return video_file
    
    return None

def find_audio_in_dir(bvid_dir):
    """查找目录中的音频文件"""
    audio_file = os.path.join(bvid_dir, f"{os.path.basename(bvid_dir)}_audio.mp4")
    if os.path.exists(audio_file):
        return audio_file
    
    return None

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='处理视频和音频文件，生成指定时长的切片')
    parser.add_argument('--mode', type=str, choices=['all', 'csv', 'parquet', 'clean'], default='parquet',
                        help='处理模式: all=处理所有视频, csv=从CSV文件处理, parquet=从Parquet文件处理, clean=清理切片文件')
    parser.add_argument('--input', type=str, default='portrait_videos.csv',
                        help='输入文件路径 (CSV或Parquet)')
    parser.add_argument('--video-dir', type=str, default='/Volumes/externalssd/video_data',
                        help='视频目录路径')
    parser.add_argument('--workers', type=int, default=None,
                        help='并行工作线程数')
    parser.add_argument('--dry-run', action='store_true',
                        help='dry run模式，不实际删除文件')
    
    args = parser.parse_args()
    
    if args.mode == 'all':
        process_all_videos(base_dir=args.video_dir, max_workers=args.workers)
    elif args.mode == 'csv':
        process_from_csv(csv_file=args.input, video_dir=args.video_dir, max_workers=args.workers)
    elif args.mode == 'parquet':
        process_from_parquet(parquet_file=args.input, video_dir=args.video_dir, max_workers=args.workers)
    elif args.mode == 'clean':
        clean_all_slices(base_dir=args.video_dir, dry_run=args.dry_run) 