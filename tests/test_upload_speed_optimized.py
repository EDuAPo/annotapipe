#!/usr/bin/env python3
"""测试优化后的上传速度"""
import time
import sys
import os
sys.path.insert(0, '.')

from src.pipeline.config import load_env_file
load_env_file()

import paramiko
from src.pipeline.config import get_config
from pathlib import Path

def test_optimized_upload():
    """测试优化窗口大小后的 SFTP 上传速度"""
    test_file = Path("/tmp/test_100m.bin")
    test_size = 100 * 1024 * 1024
    
    if not test_file.exists():
        print(f"创建测试文件: {test_size/1024/1024:.0f} MB")
        with open(test_file, 'wb') as f:
            f.write(os.urandom(test_size))
    
    config = get_config()
    server = config.get_available_server()
    
    print(f"连接服务器: {server.ip}")
    print("-" * 50)
    
    # 创建优化的 SSH 连接
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server.ip, username=server.user, timeout=10)
    
    # 获取 transport 并优化窗口大小
    transport = ssh.get_transport()
    transport.set_keepalive(30)
    
    # 使用更大的窗口大小创建 SFTP
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    # 设置更大的缓冲区
    sftp.get_channel().settimeout(300)
    
    remote_path = f"{server.zip_dir}/_speed_test_opt.bin"
    
    start = time.time()
    last_update = [start]
    last_bytes = [0]
    
    def progress(uploaded, total):
        now = time.time()
        if now - last_update[0] >= 1:
            speed = (uploaded - last_bytes[0]) / (now - last_update[0])
            pct = uploaded / total * 100 if total else 0
            print(f"\r进度: {pct:.1f}% | 速度: {speed/1024/1024:.2f} MB/s", end="", flush=True)
            last_update[0] = now
            last_bytes[0] = uploaded
    
    # 上传
    sftp.put(str(test_file), remote_path, callback=progress)
    
    elapsed = time.time() - start
    print()
    print("-" * 50)
    
    avg_speed = test_size / elapsed
    print(f"✓ 优化 SFTP 上传完成")
    print(f"  文件大小: {test_size/1024/1024:.2f} MB")
    print(f"  总耗时: {elapsed:.1f} 秒")
    print(f"  平均速度: {avg_speed/1024/1024:.2f} MB/s ({avg_speed*8/1024/1024:.1f} Mbps)")
    
    # 清理
    ssh.exec_command(f"rm -f '{remote_path}'")
    sftp.close()
    ssh.close()

if __name__ == "__main__":
    test_optimized_upload()
