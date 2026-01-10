"""
下载模块
负责从 DataWeave 下载 ZIP 文件
"""
import time
import logging
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import requests

from .config import get_config, DataWeaveConfig

logger = logging.getLogger(__name__)


class TokenManager:
    """Token 管理器，支持自动刷新"""
    
    def __init__(self, config: DataWeaveConfig):
        self.config = config
        self._token: Optional[str] = None
        self._token_time: Optional[float] = None
        self._max_age = 50 * 60  # 50分钟
    
    def get_token(self, force_refresh: bool = False) -> str:
        """获取有效的 Token"""
        if not force_refresh and self._token and self._token_time:
            if time.time() - self._token_time < self._max_age:
                return self._token
        
        if not self.config.username or not self.config.password:
            return f"Bearer {self.config.token}" if self.config.token else ""
        
        for attempt in range(3):
            try:
                login_data = {
                    "email": self.config.username,
                    "password": self.config.password
                }
                headers = {
                    "User-Agent": "Mozilla/5.0",
                    "Content-Type": "application/json",
                }
                
                r = requests.post(self.config.login_url, json=login_data, headers=headers, timeout=15)
                data = r.json()
                
                if data.get("code") == 0:
                    token_data = data.get("data", {}).get("token", {})
                    access_token = token_data.get("access_token")
                    if access_token:
                        self._token = f"Bearer {access_token}"
                        self._token_time = time.time()
                        logger.info("🔑 Token 获取成功")
                        return self._token
            except Exception:
                if attempt < 2:
                    time.sleep(1)
        
        logger.warning("⚠ 使用备用 Token")
        return f"Bearer {self.config.token}" if self.config.token else ""


class Downloader:
    """ZIP 文件下载器"""
    
    def __init__(self, config: DataWeaveConfig = None):
        self.config = config or get_config().dataweave
        self.token_manager = TokenManager(self.config)
    
    def is_valid_zip(self, zip_path: Path) -> bool:
        """检查 ZIP 文件是否有效（验证完整性）"""
        if not zip_path.exists() or zip_path.stat().st_size == 0:
            return False
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # 测试 ZIP 文件完整性（检查 CRC）
                bad_file = zf.testzip()
                return bad_file is None
        except (zipfile.BadZipFile, OSError, IOError):
            return False
    
    def get_download_url(self, filename: str, headers: Dict[str, str]) -> Optional[Tuple[str, str]]:
        """获取文件的下载 URL，返回 (url, found_path)"""
        for template in self.config.path_templates:
            dw_path = template.format(filename=filename)
            payload = {"uris": [dw_path]}
            
            try:
                r = requests.post(self.config.api_url, json=payload, headers=headers, timeout=15)
                r.raise_for_status()
                data = r.json()
                
                if data.get("code") != 0:
                    msg = data.get("msg", "")
                    if "Login required" in msg or data.get("code") == 401:
                        return None  # Token 过期
                    continue
                
                url_data = data.get("data", {})
                if isinstance(url_data, dict) and "urls" in url_data:
                    urls_list = url_data["urls"]
                    if urls_list and isinstance(urls_list[0], dict):
                        url = urls_list[0].get("url")
                        if url:
                            found_path = template.split("/")[-2]
                            return url, found_path
            except Exception:
                continue
        
        return None
    
    def download_file(self, filename: str, target_path: Path, 
                      progress_callback=None) -> bool:
        """下载单个文件"""
        temp_file = target_path.with_suffix('.zip.tmp')
        
        token = self.token_manager.get_token()
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/json",
            "Authorization": token,
        }
        
        for attempt in range(3):
            try:
                result = self.get_download_url(filename, headers)
                
                if result is None:
                    # Token 可能过期，刷新后重试
                    if attempt < 2:
                        token = self.token_manager.get_token(force_refresh=True)
                        headers["Authorization"] = token
                        continue
                    return False
                
                url, found_path = result
                logger.debug(f"找到文件，路径: {found_path}")
                
                # 下载文件
                download_headers = {"User-Agent": "Mozilla/5.0"}
                with requests.get(url, headers=download_headers, stream=True, timeout=600) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get('content-length', 0))
                    
                    with open(temp_file, 'wb') as f:
                        downloaded = 0
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                if progress_callback and total_size > 0:
                                    progress_callback(downloaded, total_size)
                
                # 验证完整性
                if total_size > 0:
                    actual_size = temp_file.stat().st_size
                    if actual_size != total_size:
                        logger.error(f"下载不完整: 预期 {total_size}, 实际 {actual_size}")
                        temp_file.unlink()
                        if attempt < 2:
                            time.sleep(2)
                            continue
                        return False
                
                # 重命名为正式文件
                if target_path.exists():
                    target_path.unlink()
                temp_file.rename(target_path)
                return True
                
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                logger.warning(f"网络错误: {e}")
                if temp_file.exists():
                    temp_file.unlink()
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
                    continue
                return False
            except Exception as e:
                logger.error(f"下载失败: {e}")
                if temp_file.exists():
                    temp_file.unlink()
                return False
        
        return False
    
    def download_batch(self, files: List[Tuple[str, Path]], 
                       skip_existing: bool = True,
                       server_exists: Set[str] = None) -> Dict[str, bool]:
        """批量下载文件（串行）"""
        results = {}
        server_exists = server_exists or set()
        
        for filename, target_path in files:
            stem = filename.replace('.zip', '')
            
            # 跳过服务器已存在的
            if filename in server_exists:
                results[filename] = True
                continue
            
            # 跳过本地已存在的
            if skip_existing and self.is_valid_zip(target_path):
                results[filename] = True
                continue
            
            success = self.download_file(filename, target_path)
            results[filename] = success
        
        return results
