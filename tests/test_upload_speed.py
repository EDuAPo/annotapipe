#!/usr/bin/env python3
"""测试上传速度"""
import time
import sys
import os
sys.path.insert(0, '.')

from src.pipeline.config import load_env_file
load_env_file()

from src.pipeline.ssh_client import SSHClient
from pathlib import Path

def test_speed():
    # 创建测试文件 (100MB)
    test_file = Path("/tmp/upload_test.bin")
    test_size = 100 * 1024 * 1024  # 100MB
    
    print(f"创建测试文件: {test_size/1024/1024:.0f} MB")
    with open(test_file, 'wb') as f:
        f.write(os.urandom(test_size))
    
    print("-" * 40)
    
    with SSHClient() as ssh:
        remote_path = f"{ssh.server.zip_dir}/_speed_test.bin"
        
        start = time.time()
        last_update = [start]
        last_bytes = [0]
        
        def progress(uploaded, total):
            now = time.time()
            if now - last_update[0] >= 1:
                speed = (uploaded - last_bytes[0]) / (now - last_update[0])
                pct = uploaded / total * 100 if total else 0
                print(f"\r进度: {pct:.1f}% | 速度: {speed/1024/1024:.2f} MB/s | 已上传: {uploaded/1024/1024:.1f} MB", end="", flush=True)
                last_update[0] = now
                last_bytes[0] = uploaded
        
        success = ssh.upload_file(str(test_file), remote_path, progress_callback=progress, verify_md5=False)
        
        elapsed = time.time() - start
        print()
        print("-" * 40)
        
        if success:
            avg_speed = test_size / elapsed
            print(f"✓ 上传成功")
            print(f"  文件大小: {test_size/1024/1024:.2f} MB")
            print(f"  总耗时: {elapsed:.1f} 秒")
            print(f"  平均速度: {avg_speed/1024/1024:.2f} MB/s ({avg_speed*8/1024/1024:.1f} Mbps)")
            
            # 清理远程测试文件
            ssh.exec_command(f"rm -f '{remote_path}'")
        else:
            print(f"✗ 上传失败")
    
    # 清理本地测试文件
    test_file.unlink()

if __name__ == "__main__":
    test_speed()
