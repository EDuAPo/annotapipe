"""
下载模块
负责从 DataWeave 下载 ZIP 文件
"""
import time
import logging
import zipfile
import threading
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import requests

from .config import get_config, DataWeaveConfig

logger = logging.getLogger(__name__)


class TokenManager:
    """Token 管理器，支持自动刷新（线程安全）"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, config: DataWeaveConfig = None):
        """单例模式，确保多线程共享同一个 Token"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config: DataWeaveConfig = None):
        if self._initialized:
            return
        self.config = config or get_config().dataweave
        self._token: Optional[str] = None
        self._token_time: Optional[float] = None
        self._max_age = 50 * 60  # 50分钟
        self._token_lock = threading.Lock()
        self._initialized = True
    
    def get_token(self, force_refresh: bool = False) -> str:
        """获取有效的 Token（线程安全）"""
        with self._token_lock:
            # 检查是否有有效 token
            if not force_refresh and self._token and self._token_time:
                if time.time() - self._token_time < self._max_age:
                    return self._token
            
            if not self.config.username or not self.config.password:
                return f"Bearer {self.config.token}" if self.config.token else ""
            
            # 记录是否是首次获取
            is_first = self._token is None
            
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
                            # 只在首次获取时打印日志
                            if is_first:
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
    
    def _verify_zip_integrity(self, zip_path: Path) -> bool:
        """
        验证 ZIP 文件完整性
        检查 ZIP 文件结构是否完整（End-of-central-directory 签名）
        """
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # testzip() 会检查所有文件的 CRC
                # 返回第一个损坏文件的名称，如果没有损坏则返回 None
                bad_file = zf.testzip()
                if bad_file is not None:
                    logger.warning(f"ZIP 文件中存在损坏的文件: {bad_file}")
                    return False
                return True
        except zipfile.BadZipFile as e:
            logger.warning(f"无效的 ZIP 文件: {e}")
            return False
        except Exception as e:
            logger.warning(f"ZIP 验证异常: {e}")
            return False
    
    def get_download_url(self, filename: str, headers: Dict[str, str]) -> Optional[Tuple[str, str]]:
        """获取文件的下载 URL，返回 (url, found_path)"""
        for i, template in enumerate(self.config.path_templates):
            dw_path = template.format(filename=filename)
            payload = {"uris": [dw_path]}
            path_name = template.split("/")[-2]
            
            try:
                r = requests.post(self.config.api_url, json=payload, headers=headers, timeout=8)
                data = r.json()
                
                if data.get("code") != 0:
                    msg = data.get("msg", "")
                    if "Login required" in msg or data.get("code") == 401:
                        return None  # Token 过期
                    # 文件不存在于此路径，继续尝试下一个
                    continue
                
                url_data = data.get("data", {})
                if isinstance(url_data, dict) and "urls" in url_data:
                    urls_list = url_data["urls"]
                    if urls_list and isinstance(urls_list[0], dict):
                        url = urls_list[0].get("url")
                        if url:
                            return url, path_name
            except requests.exceptions.Timeout:
                logger.warning(f"API 超时 ({path_name})")
                continue
            except Exception:
                continue
        
        return None
    
    def download_file(self, filename: str, target_path: Path, 
                      progress_callback=None, resume: bool = True) -> bool:
        """
        下载单个文件（支持断点续传和自适应文件名匹配）
        
        自动尝试多个候选文件名，直到找到匹配的文件：
        - 例如：1202_111045_111345_1_rere_1.json
          → 尝试 1202_111045_111345_1.zip
          → 尝试 1202_111045_111345.zip
          → 尝试 1202_111045_111345_1_rere_1.zip
        
        Args:
            filename: 文件名
            target_path: 目标路径
            progress_callback: 进度回调 (downloaded, total)
            resume: 是否启用断点续传
        """
        from .utils import get_zip_name_candidates
        
        # 生成候选文件名列表
        stem = filename.replace('.zip', '')
        candidates = get_zip_name_candidates(stem)
        
        logger.debug(f"生成候选文件名: {candidates}")
        
        # 尝试每个候选文件名
        for idx, candidate_filename in enumerate(candidates, 1):
            logger.info(f"尝试候选 {idx}/{len(candidates)}: {candidate_filename}")
            
            success = self._try_download_single(
                candidate_filename, target_path, 
                progress_callback, resume
            )
            
            if success:
                logger.info(f"✓ 成功匹配文件名: {candidate_filename}")
                return True
            else:
                logger.debug(f"✗ 候选失败: {candidate_filename}")
        
        logger.warning(f"✗ 所有候选文件名都失败: {candidates}")
        return False
    
    def _try_download_single(self, filename: str, target_path: Path,
                             progress_callback=None, resume: bool = True) -> bool:
        """
        尝试下载单个文件名（内部方法）
        
        Args:
            filename: 文件名
            target_path: 目标路径
            progress_callback: 进度回调
            resume: 是否启用断点续传
        
        Returns:
            是否下载成功
        """
        temp_file = target_path.with_suffix('.zip.tmp')
        
        token = self.token_manager.get_token()
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/json",
            "Authorization": token,
        }
        
        for attempt in range(2):
            try:
                result = self.get_download_url(filename, headers)
                
                if result is None:
                    if attempt == 0:
                        token = self.token_manager.get_token(force_refresh=True)
                        headers["Authorization"] = token
                        continue
                    logger.warning(f"无法获取下载URL: {filename}")
                    return False
                
                url, found_path = result
                
                # 检查是否可以断点续传
                downloaded = 0
                download_headers = {"User-Agent": "Mozilla/5.0"}
                
                if resume and temp_file.exists():
                    downloaded = temp_file.stat().st_size
                    if downloaded > 0:
                        download_headers["Range"] = f"bytes={downloaded}-"
                
                with requests.get(url, headers=download_headers, stream=True, timeout=(15, 60)) as r:
                    # 检查服务器是否支持断点续传
                    if r.status_code == 206:  # Partial Content
                        # 服务器支持断点续传
                        content_range = r.headers.get('content-range', '')
                        if content_range:
                            # 格式: bytes start-end/total
                            total_size = int(content_range.split('/')[-1])
                        else:
                            total_size = downloaded + int(r.headers.get('content-length', 0))
                        mode = 'ab'  # 追加模式
                    elif r.status_code == 200:
                        # 服务器不支持断点续传，从头开始
                        total_size = int(r.headers.get('content-length', 0))
                        downloaded = 0
                        mode = 'wb'  # 覆盖模式
                    else:
                        r.raise_for_status()
                        return False
                    
                    with open(temp_file, mode) as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                if progress_callback:
                                    progress_callback(downloaded, total_size)
                
                # 验证完整性 - 第一步：检查文件大小
                if total_size > 0:
                    actual_size = temp_file.stat().st_size
                    if actual_size != total_size:
                        logger.warning(f"下载不完整: 预期 {total_size}, 实际 {actual_size} - {filename}")
                        # 不删除临时文件，下次可以继续
                        continue
                
                # 验证完整性 - 第二步：检查 ZIP 文件结构
                if not self._verify_zip_integrity(temp_file):
                    logger.warning(f"ZIP 文件损坏，删除临时文件重新下载 - {filename}")
                    temp_file.unlink()
                    continue
                
                if target_path.exists():
                    target_path.unlink()
                temp_file.rename(target_path)
                return True
                
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                logger.warning(f"网络错误 (尝试 {attempt+1}/2): {type(e).__name__} - {filename}")
                # 不删除临时文件，保留断点续传能力
                if attempt == 0:
                    time.sleep(1)
                    continue
                return False
            except Exception as e:
                logger.error(f"下载异常: {type(e).__name__}: {str(e)[:100]} - {filename}")
                # 不删除临时文件，保留断点续传能力
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
