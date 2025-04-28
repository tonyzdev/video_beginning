#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import time
import argparse
import subprocess
import multiprocessing
from tqdm import tqdm
import concurrent.futures
import shutil
import re
import pandas as pd

# 配置选项
CONFIG = {
    'max_workers': min(8, multiprocessing.cpu_count()),  # 自动使用CPU核心数
    'archive_format': 'zip',  # 压缩格式: zip, tar, gztar, bztar, xztar
    'compression_level': 9,   # 压缩级别 (1-9, 仅zip格式使用)
    'batch_size': 50,         # 每批处理的文件/目录数量
    'timeout': 1800,          # 压缩命令超时时间(秒)
    'delete_original': False, # 压缩后是否删除原始文件/目录
    'skip_compressed': True,  # 跳过已压缩的文件/目录
}

def compress_directory(dir_path, output_path=None, config=None):
    """
    压缩一个目录
    
    Args:
        dir_path (str): 要压缩的目录路径
        output_path (str): 输出文件路径，默认为dir_path.zip
        config (dict): 压缩配置参数
        
    Returns:
        dict: 包含压缩结果信息的字典
    """
    start_time = time.time()
    
    if config is None:
        config = CONFIG
        
    # 确保输入目录存在
    if not os.path.exists(dir_path):
        return {'success': False, 'error': '输入目录不存在', 'input_path': dir_path}
    
    # 获取目录大小
    input_size = get_dir_size(dir_path) / (1024 * 1024)  # MB
    
    # 获取目录名称
    dir_name = os.path.dirname(dir_path)
    base_name = os.path.basename(dir_path)
    
    # 设置输出路径
    if output_path is None:
        output_path = f"{dir_path}.{config['archive_format']}"
    
    # 检查输出文件是否已存在
    if os.path.exists(output_path) and config['skip_compressed']:
        output_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
        return {
            'success': True, 
            'input_path': dir_path,
            'output_path': output_path,
            'input_size': input_size,
            'output_size': output_size,
            'compression_ratio': output_size / max(1, input_size),
            'time': 0,
            'skipped': True
        }
    
    try:
        print(f"正在压缩目录: {dir_path}")
        
        # 设置压缩命令
        if config['archive_format'] == 'zip':
            # 使用zip命令行工具，可以设置压缩级别
            cmd = [
                'zip', 
                f'-{config["compression_level"]}',  # 压缩级别
                '-r',  # 递归
                output_path, 
                base_name  # 只压缩目标目录
            ]
            # 切换到父目录执行命令
            cwd = dir_name if dir_name else '.'
        else:
            # 使用Python内置的shutil.make_archive
            root_dir = os.path.dirname(dir_path)
            base_name_without_ext = os.path.splitext(output_path)[0]
            
            # 使用shutil.make_archive
            shutil.make_archive(
                base_name_without_ext,  # 输出文件名（不含扩展名）
                config['archive_format'],  # 压缩格式
                root_dir,  # 起始目录
                os.path.basename(dir_path)  # 要压缩的目录名
            )
            
            # 为了保持一致的返回格式，伪造一个成功的结果
            elapsed_time = time.time() - start_time
            output_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
            
            return {
                'success': True,
                'input_path': dir_path,
                'output_path': output_path,
                'input_size': input_size,
                'output_size': output_size,
                'saved': input_size - output_size,
                'compression_ratio': output_size / max(1, input_size),
                'time': elapsed_time
            }
        
        # 执行zip命令
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=config['timeout'], cwd=cwd)
        
        if result.returncode != 0:
            # 压缩失败
            return {
                'success': False, 
                'error': f"压缩失败: {result.stderr}", 
                'input_path': dir_path,
                'input_size': input_size
            }
        
        # 检查输出文件
        if not os.path.exists(output_path):
            return {
                'success': False, 
                'error': '输出文件未生成', 
                'input_path': dir_path,
                'input_size': input_size
            }
        
        # 计算压缩比和节省的空间
        output_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
        compression_ratio = output_size / max(1, input_size)
        
        # 如果设置为删除原始目录
        if config['delete_original']:
            try:
                shutil.rmtree(dir_path)
            except Exception as e:
                print(f"无法删除原始目录: {dir_path}, 错误: {e}")
        
        # 计算处理时间
        elapsed_time = time.time() - start_time
        
        return {
            'success': True,
            'input_path': dir_path,
            'output_path': output_path,
            'input_size': input_size,
            'output_size': output_size,
            'saved': input_size - output_size,
            'compression_ratio': compression_ratio,
            'time': elapsed_time
        }
        
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'error': '处理超时',
            'input_path': dir_path,
            'input_size': input_size
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'压缩过程中发生错误: {str(e)}',
            'input_path': dir_path,
            'input_size': input_size
        }

def get_dir_size(path):
    """获取目录大小（字节）"""
    total_size = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size

def find_dirs(base_dir, patterns=None, recursive=False):
    """
    查找符合条件的目录
    
    Args:
        base_dir (str): 基础目录路径
        patterns (list): 目录名匹配模式列表，默认为None匹配所有
        recursive (bool): 是否递归查找子目录
        
    Returns:
        list: 包含所有匹配目录路径的列表
    """
    if patterns is None:
        patterns = ['*']
    
    dirs = []
    try:
        # 获取所有子目录
        all_dirs = []
        if recursive:
            for root, subdirs, _ in os.walk(base_dir):
                all_dirs.extend([os.path.join(root, d) for d in subdirs])
        else:
            all_dirs = [os.path.join(base_dir, d) for d in os.listdir(base_dir) 
                       if os.path.isdir(os.path.join(base_dir, d))]
        
        # 过滤匹配的目录
        for pattern in patterns:
            import fnmatch
            matches = [d for d in all_dirs if fnmatch.fnmatch(os.path.basename(d), pattern)]
            dirs.extend(matches)
        
        # 去重
        dirs = list(set(dirs))
    except Exception as e:
        print(f"查找目录时出错: {e}")
    
    return dirs

def compress_target(target_path, config=None):
    """
    压缩目标路径（可以是单个目录或整个目录树）
    
    Args:
        target_path (str): 目标路径
        config (dict): 压缩配置
        
    Returns:
        dict: 压缩结果
    """
    if config is None:
        config = CONFIG
    
    # 如果目标是目录，直接压缩
    if os.path.isdir(target_path):
        return compress_directory(target_path, config=config)
    else:
        return {
            'success': False,
            'error': '目标不是目录',
            'input_path': target_path
        }

def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description='批量压缩目录工具')
    parser.add_argument('target', type=str, nargs='?', default='.', 
                        help='要压缩的目标目录或包含多个目录的父目录')
    parser.add_argument('--format', type=str, choices=['zip', 'tar', 'gztar', 'bztar', 'xztar'], 
                        default=CONFIG['archive_format'], help='压缩格式')
    parser.add_argument('--level', type=int, choices=range(1, 10), 
                        default=CONFIG['compression_level'], help='压缩级别 (仅对zip格式有效)')
    parser.add_argument('--workers', type=int, default=CONFIG['max_workers'], 
                        help='并行压缩的工作线程数')
    parser.add_argument('--batch', type=int, default=CONFIG['batch_size'], 
                        help='每批处理的目录数量')
    parser.add_argument('--delete', action='store_true', 
                        help='压缩后删除原始目录')
    parser.add_argument('--no-skip', action='store_true', 
                        help='不跳过已压缩的目录')
    parser.add_argument('--patterns', type=str, nargs='+', default=['BV*'], 
                        help='目录名匹配模式 (使用通配符)')
    parser.add_argument('--recursive', action='store_true', 
                        help='递归查找子目录')
    parser.add_argument('--timeout', type=int, default=CONFIG['timeout'], 
                        help='压缩命令超时时间(秒)')
    parser.add_argument('--output-dir', type=str, default=None, 
                        help='输出目录 (默认与输入目录相同)')
    parser.add_argument('--output-file', type=str, default=None, 
                        help='结果保存的CSV文件路径')
    
    args = parser.parse_args()
    
    # 更新配置
    config = CONFIG.copy()
    config['archive_format'] = args.format
    config['compression_level'] = args.level
    config['max_workers'] = args.workers
    config['batch_size'] = args.batch
    config['delete_original'] = args.delete
    config['skip_compressed'] = not args.no_skip
    config['timeout'] = args.timeout
    
    # 查找符合条件的目录
    target_dirs = []
    if os.path.isdir(args.target):
        # 如果目标是目录，查找内部符合条件的子目录
        target_dirs = find_dirs(args.target, args.patterns, args.recursive)
        print(f"找到 {len(target_dirs)} 个目标目录")
    else:
        # 如果目标不是目录，报错
        print(f"错误: 目标不是目录: {args.target}")
        return
    
    # 没有找到目标目录
    if not target_dirs:
        print("没有找到符合条件的目录，请检查目标路径和匹配模式")
        return
    
    # 处理输出目录
    if args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)
    
    # 分批处理
    start_time = time.time()
    results = []
    num_batches = (len(target_dirs) + config['batch_size'] - 1) // config['batch_size']
    
    for i in range(0, len(target_dirs), config['batch_size']):
        batch = target_dirs[i:i+config['batch_size']]
        print(f"处理批次 {i//config['batch_size'] + 1}/{num_batches}, 共 {len(batch)} 个目录")
        
        batch_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=config['max_workers']) as executor:
            # 为每个目录提交压缩任务
            future_to_dir = {}
            for dir_path in batch:
                # 设置输出路径
                if args.output_dir:
                    # 使用输出目录 + 原目录名作为输出路径
                    base_name = os.path.basename(dir_path)
                    output_path = os.path.join(args.output_dir, f"{base_name}.{config['archive_format']}")
                else:
                    # 默认输出路径
                    output_path = f"{dir_path}.{config['archive_format']}"
                
                future = executor.submit(compress_directory, dir_path, output_path, config)
                future_to_dir[future] = dir_path
            
            # 处理结果
            with tqdm(total=len(batch), desc=f"批次 {i//config['batch_size'] + 1} 进度") as pbar:
                for future in concurrent.futures.as_completed(future_to_dir):
                    dir_path = future_to_dir[future]
                    try:
                        result = future.result()
                        batch_results.append(result)
                        
                        # 显示压缩结果
                        if result['success']:
                            if result.get('skipped', False):
                                status = '已跳过'
                            else:
                                ratio = result.get('compression_ratio', 0) * 100
                                saved = result.get('saved', 0)
                                status = f'已压缩 (节省 {saved:.1f} MB, 压缩率 {ratio:.1f}%)'
                        else:
                            status = f'失败: {result.get("error", "未知错误")}'
                        
                        print(f"{dir_path}: {status}")
                    except Exception as e:
                        print(f"处理 {dir_path} 时出错: {e}")
                        batch_results.append({
                            'success': False,
                            'error': str(e),
                            'input_path': dir_path
                        })
                    finally:
                        pbar.update(1)
        
        # 将批次结果添加到总结果
        results.extend(batch_results)
        
        # 计算并显示批次统计信息
        success_count = sum(1 for r in batch_results if r['success'])
        fail_count = len(batch_results) - success_count
        total_input = sum(r.get('input_size', 0) for r in batch_results)
        total_output = sum(r.get('output_size', 0) for r in batch_results if r['success'])
        total_saved = sum(r.get('saved', 0) for r in batch_results if r['success'] and 'saved' in r)
        avg_ratio = total_output / max(1, total_input) * 100
        
        print(f"批次 {i//config['batch_size'] + 1} 完成: {success_count} 成功, {fail_count} 失败")
        print(f"总输入: {total_input:.1f} MB, 总输出: {total_output:.1f} MB")
        print(f"节省空间: {total_saved:.1f} MB, 平均压缩率: {avg_ratio:.1f}%")
    
    # 总结统计
    elapsed_time = time.time() - start_time
    success_count = sum(1 for r in results if r['success'])
    fail_count = len(results) - success_count
    
    total_input = sum(r.get('input_size', 0) for r in results)
    total_output = sum(r.get('output_size', 0) for r in results if r['success'])
    total_saved = sum(r.get('saved', 0) for r in results if r['success'] and 'saved' in r)
    
    if total_input > 0:
        avg_ratio = total_output / total_input * 100
    else:
        avg_ratio = 0
    
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    print("\n----------- 总结 -----------")
    print(f"处理了 {len(results)} 个目录, {success_count} 成功, {fail_count} 失败")
    print(f"总输入: {total_input:.1f} MB, 总输出: {total_output:.1f} MB")
    print(f"节省空间: {total_saved:.1f} MB, 平均压缩率: {avg_ratio:.1f}%")
    print(f"总耗时: {int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒")
    
    # 保存结果
    if args.output_file:
        try:
            # 转换结果为DataFrame
            df = pd.DataFrame(results)
            df.to_csv(args.output_file, index=False)
            print(f"结果已保存到: {args.output_file}")
        except Exception as e:
            print(f"保存结果时出错: {e}")

if __name__ == "__main__":
    main() 