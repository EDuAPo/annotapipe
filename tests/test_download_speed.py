#!/usr/bin/env python3
"""测试 DataWeave 下载速度"""
import time
import sys
sys.path.insert(0, '.')

from src.pipeline.config import load_env_file
load_env_file()  # 先加载环境变量

from src.pipeline.downloader import Downloader
from pathlib import Path

def test_speed():
    downloader = Downloader()
    
    # 测试文件
    test_file = "1209_134548_134748.zip"
    target = Path("/tmp/speed_test.zip")
    
    print(f"测试下载: {test_file}")
    print("-" * 40)
    
    start = time.time()
    last_update = [start]
    last_bytes = [0]
    
    def progress(downloaded, total):
        now = time.time()
        if now - last_update[0] >= 1:  # 每秒更新
            speed = (downloaded - last_bytes[0]) / (now - last_update[0])
            pct = downloaded / total * 100 if total else 0
            print(f"\r进度: {pct:.1f}% | 速度: {speed/1024/1024:.2f} MB/s | 已下载: {downloaded/1024/1024:.1f} MB", end="", flush=True)
            last_update[0] = now
            last_bytes[0] = downloaded
    
    success = downloader.download_file(test_file, target, progress_callback=progress)
    
    elapsed = time.time() - start
    print()
    print("-" * 40)
    
    if success and target.exists():
        size = target.stat().st_size
        avg_speed = size / elapsed
        print(f"✓ 下载成功")
        print(f"  文件大小: {size/1024/1024:.2f} MB")
        print(f"  总耗时: {elapsed:.1f} 秒")
        print(f"  平均速度: {avg_speed/1024/1024:.2f} MB/s ({avg_speed*8/1024/1024:.1f} Mbps)")
        target.unlink()
    else:
        print(f"✗ 下载失败")

if __name__ == "__main__":
    test_speed()
