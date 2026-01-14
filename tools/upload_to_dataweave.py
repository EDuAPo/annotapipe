#!/usr/bin/env python3
"""
DataWeave 上传工具
从本地路径扫描 ZIP 文件并上传到 DataWeave 指定目录
"""
import os
import sys
import time
import argparse
import logging
from pathlib import Path
from typing import List, Optional

# 添加项目根目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

from src.pipeline.downloader import TokenManager
from src.pipeline.config import get_config, load_env_file

# Load environment variables at module import
load_env_file()

# 配置 requests session 以提高稳定性
def create_robust_session():
    """创建一个更稳定的 requests session"""
    session = requests.Session()
    
    # 配置重试策略
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"]
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class DataWeaveUploader:
    """DataWeave 上传器"""
    
    def __init__(self, config_path: str = "configs/upload_config.yaml"):
        self.config = self._load_config(config_path)
        self.token_manager = TokenManager(get_config().dataweave)
        self.base_url = get_config().dataweave.base_url
        self.session = create_robust_session()
    
    def _load_config(self, config_path: str) -> dict:
        """加载上传配置"""
        config_file = Path(config_path)
        if not config_file.exists():
            logger.warning(f"配置文件不存在: {config_path}，使用默认配置")
            return {
                'local_dir': '/path/to/local/zips',
                'target_path': 'dataweave://my/TO_RERE/未上传平台',
                'file_pattern': '*.zip'
            }
        
        with open(config_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    
    def scan_zip_files(self, local_dir: str, pattern: str = "*.zip") -> List[Path]:
        """扫描本地目录中的 ZIP 文件（递归搜索所有子目录）"""
        local_path = Path(local_dir)
        if not local_path.exists():
            logger.error(f"本地目录不存在: {local_dir}")
            return []
        
        # 使用 rglob 递归搜索所有子目录
        zip_files = sorted(local_path.rglob(pattern))
        logger.info(f"📁 扫描目录: {local_dir} (递归)")
        logger.info(f"📦 找到 {len(zip_files)} 个 ZIP 文件")
        return zip_files
    
    def check_file_exists(self, filename: str, target_path: str) -> bool:
        """检查文件是否已存在于 DataWeave"""
        dw_path = f"{target_path}/{filename}"
        payload = {"uris": [dw_path]}
        
        token = self.token_manager.get_token()
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/json",
            "Authorization": token,
        }
        
        try:
            # 使用获取下载 URL 的 API 来检查文件是否存在
            api_url = f"{self.base_url}/file/url"
            r = requests.post(api_url, json=payload, headers=headers, timeout=10)
            data = r.json()
            
            # 如果返回成功，说明文件存在
            if data.get("code") == 0:
                return True
            return False
        except Exception as e:
            logger.debug(f"检查文件存在性失败: {e}")
            return False
    
    def upload_file(self, local_file: Path, target_path: str, 
                    skip_existing: bool = True, max_retries: int = 3) -> bool:
        """
        上传单个文件到 DataWeave（支持重试）
        
        Args:
            local_file: 本地文件路径
            target_path: DataWeave 目标路径（不包含文件名）
            skip_existing: 是否跳过已存在的文件
            max_retries: 最大重试次数
        """
        filename = local_file.name
        file_size = local_file.stat().st_size
        
        # 检查文件是否已存在
        if skip_existing and self.check_file_exists(filename, target_path):
            logger.info(f"⏭ 跳过（已存在）: {filename}")
            return True
        
        logger.info(f"📤 上传: {filename} ({file_size / 1024 / 1024:.1f}MB)")
        
        # DataWeave 上传 API
        upload_url = f"{self.base_url}/file/upload"
        dw_path = f"{target_path}/{filename}"
        
        # 重试循环
        for attempt in range(max_retries):
            file_handle = None
            try:
                # 获取最新 token
                token = self.token_manager.get_token()
                headers = {"Authorization": token}
                
                # 打开文件
                file_handle = open(local_file, 'rb')
                files = {'file': (filename, file_handle, 'application/zip')}
                data = {
                    'path': dw_path,
                    'overwrite': 'false' if skip_existing else 'true'
                }
                
                # 上传文件（增加超时时间，根据文件大小动态调整）
                timeout = max(600, file_size / (1024 * 1024))  # 至少10分钟，大文件更长
                
                r = self.session.post(
                    upload_url,
                    headers=headers,
                    files=files,
                    data=data,
                    timeout=timeout
                )
                
                # 检查响应状态
                if r.status_code != 200:
                    logger.error(f"❌ 上传失败: {filename} - HTTP {r.status_code}")
                    if attempt < max_retries - 1:
                        logger.info(f"🔄 重试 {attempt + 1}/{max_retries - 1}...")
                        time.sleep(2 ** attempt)  # 指数退避
                        continue
                    return False
                
                try:
                    result = r.json()
                except Exception as e:
                    logger.error(f"❌ 解析响应失败: {filename} - {e}")
                    if attempt < max_retries - 1:
                        logger.info(f"🔄 重试 {attempt + 1}/{max_retries - 1}...")
                        time.sleep(2 ** attempt)
                        continue
                    return False
                
                if result.get('code') == 0:
                    logger.info(f"✅ 上传成功: {filename}")
                    return True
                else:
                    logger.error(f"❌ 上传失败: {filename} - {result.get('msg', 'Unknown error')}")
                    if attempt < max_retries - 1:
                        logger.info(f"🔄 重试 {attempt + 1}/{max_retries - 1}...")
                        time.sleep(2 ** attempt)
                        continue
                    return False
                    
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, 
                    requests.exceptions.Timeout) as e:
                logger.error(f"❌ 网络错误: {filename} - {type(e).__name__}")
                if attempt < max_retries - 1:
                    logger.info(f"🔄 重试 {attempt + 1}/{max_retries - 1}...")
                    time.sleep(2 ** attempt)
                    continue
                return False
            except Exception as e:
                logger.error(f"❌ 上传异常: {filename} - {e}")
                if attempt < max_retries - 1:
                    logger.info(f"🔄 重试 {attempt + 1}/{max_retries - 1}...")
                    time.sleep(2 ** attempt)
                    continue
                return False
            finally:
                # 确保文件被关闭
                if file_handle:
                    file_handle.close()
        
        return False
    
    def upload_batch(self, local_dir: str, target_path: str, 
                     pattern: str = "*.zip", skip_existing: bool = True) -> dict:
        """
        批量上传文件
        
        Args:
            local_dir: 本地目录
            target_path: DataWeave 目标路径
            pattern: 文件匹配模式
            skip_existing: 是否跳过已存在的文件
        """
        zip_files = self.scan_zip_files(local_dir, pattern)
        
        if not zip_files:
            logger.warning("没有找到需要上传的文件")
            return {'success': 0, 'failed': 0, 'skipped': 0}
        
        print()
        print("=" * 60)
        print(f"  开始上传到: {target_path}")
        print("=" * 60)
        
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        for i, zip_file in enumerate(zip_files, 1):
            print(f"\n[{i}/{len(zip_files)}]")
            
            # 检查是否已存在
            if skip_existing and self.check_file_exists(zip_file.name, target_path):
                logger.info(f"⏭ 跳过（已存在）: {zip_file.name}")
                skipped_count += 1
                continue
            
            success = self.upload_file(zip_file, target_path, skip_existing)
            if success:
                success_count += 1
            else:
                failed_count += 1
        
        # 打印汇总
        print()
        print("=" * 60)
        print("  上传汇总")
        print("=" * 60)
        print(f"  ✅ 成功: {success_count}")
        print(f"  ❌ 失败: {failed_count}")
        print(f"  ⏭ 跳过: {skipped_count}")
        print(f"  📊 总计: {len(zip_files)}")
        print("=" * 60)
        
        return {
            'success': success_count,
            'failed': failed_count,
            'skipped': skipped_count,
            'total': len(zip_files)
        }


def main():
    parser = argparse.ArgumentParser(
        description='上传 ZIP 文件到 DataWeave',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 使用配置文件
  python upload_to_dataweave.py --config configs/upload_config.yaml
  
  # 指定本地目录和目标路径
  python upload_to_dataweave.py --local /path/to/zips --target "dataweave://my/TO_RERE/未上传平台"
  
  # 不跳过已存在的文件（覆盖）
  python upload_to_dataweave.py --local /path/to/zips --target "dataweave://my/path" --no-skip
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        default='configs/upload_config.yaml',
        help='配置文件路径 (默认: configs/upload_config.yaml)'
    )
    parser.add_argument(
        '--local', '-l',
        help='本地 ZIP 文件目录（覆盖配置文件）'
    )
    parser.add_argument(
        '--target', '-t',
        help='DataWeave 目标路径（覆盖配置文件）'
    )
    parser.add_argument(
        '--pattern', '-p',
        default='*.zip',
        help='文件匹配模式 (默认: *.zip)'
    )
    parser.add_argument(
        '--no-skip',
        action='store_true',
        help='不跳过已存在的文件（覆盖上传）'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='启用调试日志'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 初始化上传器
    uploader = DataWeaveUploader(args.config)
    
    # 从命令行参数或配置文件获取设置
    local_dir = args.local or uploader.config.get('local_dir')
    target_path = args.target or uploader.config.get('target_path')
    pattern = args.pattern or uploader.config.get('file_pattern', '*.zip')
    skip_existing = not args.no_skip
    
    if not local_dir or not target_path:
        logger.error("错误: 必须指定本地目录和目标路径")
        logger.error("使用 --local 和 --target 参数，或在配置文件中设置")
        sys.exit(1)
    
    print()
    print("╔" + "═" * 58 + "╗")
    print("║  📤 DataWeave 上传工具".ljust(59) + "║")
    print("╚" + "═" * 58 + "╝")
    print(f"  本地目录: {local_dir}")
    print(f"  目标路径: {target_path}")
    print(f"  文件模式: {pattern}")
    print(f"  跳过已存在: {'是' if skip_existing else '否'}")
    print()
    
    # 执行批量上传
    result = uploader.upload_batch(local_dir, target_path, pattern, skip_existing)
    
    # 返回退出码
    if result['failed'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
