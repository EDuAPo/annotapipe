#!/usr/bin/env python3
"""
标注数据处理流水线 - 命令行入口
模块化重构版本 v2.0

使用方法:
    python3 run_pipeline.py --json_dir /path/to/jsons
    python3 run_pipeline.py --json_dir /path/to/jsons --mode parallel --workers 4
    python3 run_pipeline.py --json_dir /path/to/jsons --mode streaming
"""

import argparse
import logging
from pathlib import Path

from src.pipeline import PipelineRunner, PipelineConfig
from src.pipeline.config import load_env_file

# 加载环境变量
load_env_file()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S'
)
logging.getLogger("paramiko").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(
        description="标注数据自动化处理流水线 v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
运行模式:
  optimized  下载并行 + 服务器操作串行 (默认，推荐)
  parallel   全并行模式，每个线程独立处理
  streaming  流式模式，下载一个处理一个

示例:
  python run_pipeline.py --json_dir ./test_jsons
  python run_pipeline.py --json_dir ./test_jsons --mode parallel -w 4
  python run_pipeline.py --json_dir ./test_jsons --zip_dir /tmp/zips
        """
    )
    
    parser.add_argument(
        '--json_dir', '-j',
        type=str,
        required=True,
        help='本地 JSON 文件夹路径'
    )
    
    parser.add_argument(
        '--zip_dir', '-z',
        type=str,
        default=None,
        help='本地 ZIP 文件存储路径 (可选)'
    )
    
    parser.add_argument(
        '--mode', '-m',
        type=str,
        default='optimized',
        choices=['optimized', 'parallel', 'streaming'],
        help='运行模式 (默认: optimized)'
    )
    
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=3,
        help='并发数 (默认: 3)'
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        default=None,
        help='配置文件路径 (可选)'
    )
    
    args = parser.parse_args()
    
    # 验证路径
    json_dir = Path(args.json_dir)
    if not json_dir.exists():
        print(f"❌ JSON 目录不存在: {json_dir}")
        return 1
    
    # 加载配置
    config = PipelineConfig.load(args.config) if args.config else None
    
    # 创建并运行流水线
    runner = PipelineRunner(
        json_dir=str(json_dir),
        local_zip_dir=args.zip_dir,
        config=config
    )
    
    result = runner.run(mode=args.mode, workers=args.workers)
    
    # 返回状态码
    if result.check_failed:
        return 1
    return 0


if __name__ == "__main__":
    exit(main())
