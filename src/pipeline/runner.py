"""
流水线运行器
负责编排整个处理流程，支持串行/并行模式
"""
import sys
import time
import logging
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

from .config import get_config, PipelineConfig
from .ssh_client import SSHClient
from .downloader import Downloader
from .processor import RemoteProcessor
from .server_logger import ServerLogger
from .tracker import Tracker, TrackingRecord
from .state import StateManager, ProcessStatus
from .nas_backup import NASBackup
from .utils import normalize_zip_name

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """流水线执行结果"""
    downloaded: List[str] = field(default_factory=list)
    download_failed: List[str] = field(default_factory=list)
    skipped_server_exists: List[str] = field(default_factory=list)
    uploaded: List[str] = field(default_factory=list)
    processed: List[str] = field(default_factory=list)
    check_passed: List[str] = field(default_factory=list)
    check_failed: List[str] = field(default_factory=list)
    moved_to_final: List[str] = field(default_factory=list)
    backed_up: List[str] = field(default_factory=list)  # NAS备份成功的数据包
    backup_failed: List[str] = field(default_factory=list)  # NAS备份失败的数据包
    keyframe_counts: Dict[str, int] = field(default_factory=dict)
    errors: Dict[str, List[tuple]] = field(default_factory=dict)
    
    def log_error(self, stem: str, step: str, msg: str):
        if stem not in self.errors:
            self.errors[stem] = []
        self.errors[stem].append((step, msg))


class ProgressTracker:
    """进度追踪器"""
    
    def __init__(self, total: int, title: str = "处理进度"):
        self.total = total
        self.completed = 0
        self.success = 0
        self.failed = 0
        self.title = title
        self.lock = threading.Lock()
        self.start_time = datetime.now()
    
    def update(self, success: bool = True, name: str = ""):
        with self.lock:
            self.completed += 1
            if success:
                self.success += 1
            else:
                self.failed += 1
            self._display(name, success)
    
    def _display(self, name: str, success: bool):
        percent = self.completed / self.total * 100 if self.total > 0 else 0
        width = 25
        filled = int(width * self.completed / self.total) if self.total > 0 else 0
        bar = '━' * filled + '╸' + '─' * (width - filled - 1) if filled < width else '━' * width
        status = "✓" if success else "✗"
        
        sys.stdout.write(f'\r\033[K')
        sys.stdout.write(f'[{bar}] {self.completed}/{self.total} ({percent:.0f}%) │ {status} {name[:30]:<30}')
        sys.stdout.flush()
        
        if self.completed >= self.total:
            print()
    
    def summary(self):
        elapsed = (datetime.now() - self.start_time).seconds
        mins, secs = divmod(elapsed, 60)
        print(f"\n{'─'*50}")
        print(f"  📊 {self.title} 完成")
        print(f"  ✓ 成功: {self.success}  ✗ 失败: {self.failed}  ⏱ 耗时: {mins}分{secs}秒")
        print(f"{'─'*50}")


class SSHConnectionPool:
    """SSH 连接池，用于并行模式复用连接"""
    
    def __init__(self, size: int = 3):
        self._pool: Queue = Queue()
        self._size = size
        self._created = 0
        self._lock = threading.Lock()
        self._scripts_deployed = False
    
    def get(self) -> Optional[SSHClient]:
        """获取一个连接"""
        # 先尝试从池中获取
        if not self._pool.empty():
            try:
                return self._pool.get_nowait()
            except:
                pass
        
        # 创建新连接
        with self._lock:
            if self._created < self._size:
                ssh = SSHClient()
                if ssh.connect():
                    self._created += 1
                    return ssh
        
        # 等待可用连接
        return self._pool.get(timeout=60)
    
    def put(self, ssh: SSHClient):
        """归还连接"""
        if ssh and ssh.is_connected:
            self._pool.put(ssh)
    
    def close_all(self):
        """关闭所有连接"""
        while not self._pool.empty():
            try:
                ssh = self._pool.get_nowait()
                ssh.close()
            except:
                pass


class PipelineRunner:
    """流水线运行器"""
    
    def __init__(self, json_dir: str, local_zip_dir: str = None, config: PipelineConfig = None):
        self.config = config or get_config()
        self.json_dir = Path(json_dir)
        
        # 本地目录
        base_dir = Path(local_zip_dir) if local_zip_dir else Path(self.config.local_temp_dir)
        self.local_zip_dir = base_dir / "zips"
        self.local_check_dir = base_dir / "check_data"
        self.local_zip_dir.mkdir(parents=True, exist_ok=True)
        self.local_check_dir.mkdir(parents=True, exist_ok=True)
        
        # 组件
        self.downloader = Downloader(self.config.dataweave)
        self.result = PipelineResult()
        self._lock = threading.Lock()
        self._deploy_lock = threading.Lock()
        self._scripts_deployed = False
        self.server_logger: Optional[ServerLogger] = None
        self.nas_backup: Optional[NASBackup] = None
        
        # 状态管理器（断点续传支持）
        self.state_manager = StateManager(base_dir)
    
    def run(self, mode: str = "optimized", workers: int = None):
        """
        运行流水线
        mode: optimized (下载并行+服务器串行), parallel (全并行), streaming (流式)
        """
        workers = workers or self.config.max_workers
        
        print()
        print("╔" + "═" * 50 + "╗")
        print(f"║  📦 标注数据处理流水线 ({mode}模式)".ljust(51) + "║")
        print("╚" + "═" * 50 + "╝")
        print(f"  📁 JSON目录: {self.json_dir}")
        
        json_files = list(self.json_dir.glob("*.json"))
        if not json_files:
            print("  ⚠ 未找到 JSON 文件")
            return self.result
        
        print(f"  📋 共 {len(json_files)} 个文件")
        
        # 初始化NAS备份（使用上下文管理器）
        with NASBackup() as nas_backup:
            self.nas_backup = nas_backup
            
            with SSHClient() as ssh:
                if not ssh.is_connected:
                    print("  ✗ 无法连接服务器")
                    return self.result
                
                print(f"  🔗 已连接服务器: {ssh.server.ip}")
                
                processor = RemoteProcessor(ssh, self.config)
                processor.deploy_scripts()
                
                # 初始化服务器日志
                self.server_logger = ServerLogger(ssh)
                print(f"  📋 服务器日志: {self.server_logger.log_file}")
                
                # 确保目录存在
                ssh.mkdir_p(ssh.server.zip_dir)
                ssh.mkdir_p(ssh.server.process_dir)
                
                # 注意：不再自动清理临时文件，以支持断点续传
                # 如需清理，请手动调用 uploader.cleanup_incomplete(force=True)
                
                # 获取服务器状态
                state = processor.get_server_state()
                print(f"  📊 服务器: {len(state['zip_files'])} ZIPs / {len(state['processed_dirs'])} 已完成")
                
                # 统计本地已下载的文件（只统计数量，不验证完整性）
                local_zip_files = list(self.local_zip_dir.glob("*.zip"))
                print(f"  💾 本地ZIP: {len(local_zip_files)} 个")
                
                # 过滤需要处理的文件，跳过的文件立即更新飞书
                files_to_process = []
                tracker = Tracker()
                for json_file in json_files:
                    stem = json_file.stem
                    if stem in state['processed_dirs']:
                        # 尝试获取关键帧数量，如果失败则重新处理
                        logger.info(f"[{stem}] 检查final_dir中的数据完整性...")
                        kf = processor.get_keyframe_count(f"{ssh.server.final_dir}/{stem}")
                        if kf > 0:
                            # 服务器上已完成且数据完整的文件，记录为跳过
                            logger.info(f"[{stem}] ✓ 已在final_dir中 (关键帧: {kf})，跳过所有步骤")
                            self.result.skipped_server_exists.append(stem)
                            self.result.check_passed.append(stem)
                            self.result.keyframe_counts[stem] = kf
                            # 立即更新飞书
                            logger.info(f"[{stem}] 更新飞书表格...")
                            self._track_single_to_feishu(tracker, stem, silent=True)
                            logger.info(f"[{stem}] ✓ 飞书已更新")
                        else:
                            # 数据不完整，需要重新处理
                            logger.warning(f"[{stem}] ✗ 在final_dir但数据不完整，将重新处理")
                            files_to_process.append((json_file, stem))
                    else:
                        files_to_process.append((json_file, stem))
                
                skipped = len(json_files) - len(files_to_process)
                if skipped > 0:
                    print(f"  ⏭ 跳过(服务器已完成): {skipped} 个")
                
                if not files_to_process:
                    print("  ✓ 所有文件都已处理完成")
                    self._print_summary()
                    return self.result
                
                # 计算实际需要下载的数量
                local_stems = set(f.stem for f in local_zip_files)
                need_download = 0
                for json_file, stem in files_to_process:
                    zip_name = f"{stem}.zip"
                    if zip_name not in state['zip_files'] and stem not in local_stems:
                        need_download += 1
                
                print(f"  📦 待处理: {len(files_to_process)} 个 (需下载: {need_download})")
                if mode != "streaming":
                    print(f"  🧵 并发数: {workers}")
                print()
                
                if mode == "optimized":
                    self._run_optimized(ssh, processor, files_to_process, state, workers)
                elif mode == "parallel":
                    self._run_parallel(processor, files_to_process, state, workers)
                else:
                    self._run_streaming(ssh, processor, files_to_process, state)
        
        self._print_summary()
        
        # 注意：飞书同步已在 _track_single_to_feishu 中逐个完成
        # 不再调用 _track_to_feishu 避免重复同步
        
        return self.result
    
    def _run_optimized(self, ssh: SSHClient, processor: RemoteProcessor,
                       files: List[tuple], state: Dict, workers: int):
        """优化模式：下载并行 + 服务器操作串行"""
        
        # 阶段1: 并行下载
        print("=" * 50)
        print("  📥 阶段1: 并行下载 ZIP 文件")
        print("=" * 50)
        
        files_to_download = []
        skipped_local = 0
        skipped_server = 0
        for json_file, stem in files:
            # 规范化文件名用于查找ZIP
            normalized_stem = normalize_zip_name(stem)
            zip_name = f"{normalized_stem}.zip"
            local_zip = self.local_zip_dir / zip_name
            
            if zip_name in state['zip_files']:
                self.result.skipped_server_exists.append(stem)
                skipped_server += 1
                continue
            # 只检查文件存在且大小>0，不验证完整性（避免卡顿）
            if local_zip.exists() and local_zip.stat().st_size > 0:
                self.result.downloaded.append(stem)
                skipped_local += 1
                continue
            files_to_download.append((stem, zip_name, local_zip))
        
        if skipped_server > 0 or skipped_local > 0:
            print(f"  跳过: 服务器已有 {skipped_server} 个, 本地已有 {skipped_local} 个")
        
        if files_to_download:
            print(f"  需下载: {len(files_to_download)} 个文件 (并发: {workers})")
            
            # 预先获取 token，避免在进度条显示期间输出日志
            self.downloader.token_manager.get_token()
            
            # 尝试使用 tqdm 进度条
            try:
                from tqdm import tqdm
                use_tqdm = True
            except ImportError:
                use_tqdm = False
            
            # 并行下载
            download_status = {}  # stem -> status
            status_lock = threading.Lock()
            active_downloads = {}  # stem -> (downloaded, total)
            
            if use_tqdm:
                # 单进度条：显示文件数 + 下载流量
                file_pbar = tqdm(total=len(files_to_download), desc="  下载进度", 
                                unit="个", ncols=80, leave=True)
                total_bytes = [0]
                downloaded_bytes = [0]
                last_update = [time.time()]
                start_time = [time.time()]
                
                def make_progress_callback(stem):
                    def callback(downloaded, total):
                        with status_lock:
                            if stem not in active_downloads:
                                active_downloads[stem] = (0, total)
                                if total > 0:
                                    total_bytes[0] += total
                            old_downloaded, _ = active_downloads[stem]
                            delta = downloaded - old_downloaded
                            if delta > 0:
                                active_downloads[stem] = (downloaded, total)
                                downloaded_bytes[0] += delta
                                # 限制刷新频率，避免闪烁
                                now = time.time()
                                if now - last_update[0] > 0.2:
                                    last_update[0] = now
                                    # 计算下载速度
                                    elapsed = now - start_time[0]
                                    speed = downloaded_bytes[0] / elapsed / 1024 / 1024 if elapsed > 0 else 0
                                    file_pbar.set_postfix_str(f"{downloaded_bytes[0]/1024/1024:.0f}MB {speed:.1f}MB/s", refresh=True)
                    return callback
            else:
                make_progress_callback = lambda stem: None
            
            def download_task(stem, zip_name, local_zip):
                try:
                    progress_cb = make_progress_callback(stem)
                    success = self.downloader.download_file(zip_name, local_zip, progress_callback=progress_cb)
                    with status_lock:
                        download_status[stem] = success
                    return stem, success
                except Exception as e:
                    with status_lock:
                        download_status[stem] = False
                    logger.error(f"下载异常 {stem}: {e}")
                    return stem, False
            
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(download_task, stem, zip_name, local_zip) 
                          for stem, zip_name, local_zip in files_to_download]
                
                for future in as_completed(futures):
                    stem, success = future.result()
                    if use_tqdm:
                        file_pbar.update(1)
                        status = "✓" if success else "✗"
                        file_pbar.set_postfix_str(f"{status} {stem[:20]}")
                    else:
                        status = "✓" if success else "✗"
                        sys.stdout.write(f'\r\033[K  [{len(download_status)}/{len(futures)}] {status} {stem[:40]}')
                        sys.stdout.flush()
                    
                    if success:
                        self.result.downloaded.append(stem)
                    else:
                        self.result.download_failed.append(stem)
            
            if use_tqdm:
                file_pbar.close()
            else:
                print()
            
            # 下载汇总
            success_count = len(self.result.downloaded) - skipped_local
            fail_count = len(self.result.download_failed)
            print(f"  📊 下载完成: ✓ {success_count}  ✗ {fail_count}")
        else:
            print("  所有文件已下载或服务器已存在")
        
        # 阶段2: 串行服务器操作
        print()
        print("=" * 50)
        print("  🔄 阶段2: 串行服务器操作")
        print("=" * 50)
        
        files_for_server = [(jf, stem) for jf, stem in files if stem not in self.result.download_failed]
        
        if not files_for_server:
            print("  没有需要处理的文件")
            return
        
        # 计算实际需要上传的文件数量（排除服务器已有ZIP的）
        need_upload_count = sum(1 for jf, stem in files_for_server 
                                if f"{stem}.zip" not in state['zip_files'] 
                                and (self.local_zip_dir / f"{stem}.zip").exists())
        
        print(f"  待处理: {len(files_for_server)} 个 (需上传: {need_upload_count} 个)")
        
        progress = ProgressTracker(len(files_for_server), "服务器处理")
        tracker = Tracker()
        upload_idx = 0
        
        for idx, (json_file, stem) in enumerate(files_for_server, 1):
            zip_name = f"{stem}.zip"
            # 判断是否需要上传
            need_upload = zip_name not in state['zip_files'] and (self.local_zip_dir / zip_name).exists()
            if need_upload:
                upload_idx += 1
                success = self._process_single(ssh, processor, json_file, stem, state, upload_idx, need_upload_count)
            else:
                success = self._process_single(ssh, processor, json_file, stem, state, 0, 0)
            # 只在成功时同步飞书
            if success:
                self._track_single_to_feishu(tracker, stem, silent=True)
            progress.update(success=success, name=stem)
        
        progress.summary()
    
    def _run_parallel(self, processor: RemoteProcessor, files: List[tuple], 
                      state: Dict, workers: int):
        """全并行模式：使用连接池复用 SSH 连接"""
        progress = ProgressTracker(len(files), "并行处理")
        pool = SSHConnectionPool(size=workers)
        tracker = Tracker()
        
        try:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {}
                for json_file, stem in files:
                    future = executor.submit(
                        self._process_with_pool, 
                        pool, json_file, stem, state
                    )
                    futures[future] = stem
                
                for future in as_completed(futures):
                    stem = futures[future]
                    try:
                        success = future.result()
                        # 只在成功时同步飞书
                        if success:
                            self._track_single_to_feishu(tracker, stem, silent=True)
                        progress.update(success=success, name=stem)
                    except Exception as e:
                        logger.error(f"并行处理异常 {stem}: {e}")
                        progress.update(success=False, name=f"{stem} (异常)")
        finally:
            pool.close_all()
        
        progress.summary()
    
    def _run_streaming(self, ssh: SSHClient, processor: RemoteProcessor,
                       files: List[tuple], state: Dict):
        """流式模式：下载一个处理一个，每完成一个立即同步飞书"""
        progress = ProgressTracker(len(files), "流式处理")
        tracker = Tracker()
        
        # 预计算需要下载和上传的文件
        local_stems = set(f.stem for f in self.local_zip_dir.glob("*.zip"))
        need_download_list = []
        need_upload_list = []
        
        for json_file, stem in files:
            # 规范化文件名用于查找ZIP
            normalized_stem = normalize_zip_name(stem)
            zip_name = f"{normalized_stem}.zip"
            if zip_name not in state['zip_files'] and stem not in local_stems:
                need_download_list.append(stem)
            if zip_name not in state['zip_files']:
                need_upload_list.append(stem)
        
        download_idx = 0
        upload_idx = 0
        need_download_count = len(need_download_list)
        need_upload_count = len(need_upload_list)
        
        for json_file, stem in files:
            zip_name = f"{stem}.zip"
            
            # 计算当前文件的进度索引
            need_download = stem in need_download_list
            need_upload = stem in need_upload_list
            
            if need_download:
                download_idx += 1
                current_idx = download_idx
                total_count = need_download_count
            elif need_upload:
                upload_idx += 1
                current_idx = upload_idx
                total_count = need_upload_count
            else:
                current_idx = 0
                total_count = 0
            
            success = self._process_single(ssh, processor, json_file, stem, state, current_idx, total_count)
            # 只在成功时同步飞书
            if success:
                self._track_single_to_feishu(tracker, stem, silent=True)
            progress.update(success=success, name=stem)
        
        progress.summary()
    
    def _process_single(self, ssh: SSHClient, processor: RemoteProcessor,
                        json_file: Path, stem: str, state: Dict,
                        current_idx: int = 0, total_count: int = 0) -> bool:
        """处理单个文件（使用共享SSH连接）"""
        # 规范化文件名用于查找ZIP
        normalized_stem = normalize_zip_name(stem)
        zip_name = f"{normalized_stem}.zip"
        local_zip = self.local_zip_dir / zip_name
        server = ssh.server
        
        # 检查服务器是否已有 ZIP 文件（可能带有 processed_ 前缀）
        server_has_zip = zip_name in state['zip_files']
        actual_zip_name = state.get('zip_file_map', {}).get(zip_name, zip_name)
        remote_zip = f"{server.zip_dir}/{actual_zip_name}"
        
        # 检查是否可以从中间状态恢复
        skip_download = self.state_manager.can_skip_download(stem)
        skip_upload = self.state_manager.can_skip_upload(stem)
        
        # 检查 process_dir 中是否已有解压的数据
        in_processing = stem in state.get('processing_dirs', set())
        
        # 检查本地文件是否存在（不验证完整性，避免卡顿）
        local_exists = local_zip.exists() and local_zip.stat().st_size > 0
        
        try:
            # 步骤1: 下载ZIP（如果需要）
            if not skip_download and not server_has_zip and not local_exists:
                logger.info(f"[{stem}] ⬇ 下载ZIP...")
                download_start = [time.time()]
                last_print = [0]
                
                def download_progress(downloaded, total):
                    now = time.time()
                    if now - last_print[0] < 0.3:
                        return
                    last_print[0] = now
                    elapsed = now - download_start[0]
                    speed = downloaded / elapsed / 1024 / 1024 if elapsed > 0 else 0
                    percent = downloaded / total * 100 if total > 0 else 0
                    sys.stdout.write(f'\r\033[K  ⬇ 下载 {stem[:20]}: {downloaded/1024/1024:.1f}/{total/1024/1024:.1f}MB ({percent:.0f}%) {speed:.1f}MB/s')
                    sys.stdout.flush()
                
                if not self.downloader.download_file(zip_name, local_zip, progress_callback=download_progress):
                    print()
                    logger.error(f"[{stem}] 下载失败")
                    self.result.log_error(stem, "下载", "下载失败")
                    self.result.check_failed.append(stem)
                    self.state_manager.update(stem, ProcessStatus.FAILED, "下载失败")
                    return False
                print()
                self.result.downloaded.append(stem)
                self.state_manager.update(stem, ProcessStatus.DOWNLOADED)
            else:
                if server_has_zip:
                    logger.info(f"[{stem}] ⏭ 跳过下载 (服务器已有)")
                elif local_exists:
                    logger.info(f"[{stem}] ⏭ 跳过下载 (本地已有)")
                elif skip_download:
                    logger.info(f"[{stem}] ⏭ 跳过下载 (断点续传)")
            
            # 步骤2: 上传ZIP到服务器（如果需要）
            if not skip_upload and not server_has_zip and local_zip.exists():
                logger.info(f"[{stem}] ⬆ 上传ZIP...")
                upload_start = [time.time()]
                last_print = [0]
                
                def upload_progress(transferred, total):
                    now = time.time()
                    if now - last_print[0] < 0.3:
                        return
                    last_print[0] = now
                    elapsed = now - upload_start[0]
                    speed = transferred / elapsed / 1024 / 1024 if elapsed > 0 else 0
                    percent = transferred / total * 100 if total > 0 else 0
                    sys.stdout.write(f'\r\033[K  ⬆ 上传 {stem[:20]}: {transferred/1024/1024:.1f}/{total/1024/1024:.1f}MB ({percent:.0f}%) {speed:.1f}MB/s')
                    sys.stdout.flush()
                
                if not ssh.upload_file(str(local_zip), remote_zip, progress_callback=upload_progress):
                    print()
                    logger.error(f"[{stem}] 上传失败")
                    self.result.log_error(stem, "上传", "上传失败")
                    self.result.check_failed.append(stem)
                    self.state_manager.update(stem, ProcessStatus.FAILED, "上传失败")
                    return False
                print()
                self.result.uploaded.append(stem)
                self.state_manager.update(stem, ProcessStatus.UPLOADED)
            else:
                if server_has_zip:
                    logger.info(f"[{stem}] ⏭ 跳过上传 (服务器已有)")
                elif skip_upload:
                    logger.info(f"[{stem}] ⏭ 跳过上传 (断点续传)")
            
            # 步骤3-5: 解压ZIP并上传JSON
            data_dir = f"{server.process_dir}/{stem}"
            need_extract = True
            
            if in_processing:
                # 目录已存在，验证数据完整性
                logger.info(f"[{stem}] 🔍 验证数据完整性...")
                kf_check = processor.get_keyframe_count(data_dir)
                if kf_check > 0:
                    logger.info(f"[{stem}] ✓ 数据完整，跳过解压")
                    need_extract = False
                else:
                    logger.warning(f"[{stem}] ✗ 数据不完整，重新解压")
                    ssh.exec_command(f"rm -rf '{data_dir}'")
            
            if need_extract:
                # 尝试解压，失败时清理并重试一次
                max_retries = 2
                for attempt in range(max_retries):
                    success, err = processor.process_zip(remote_zip, str(json_file), stem)
                    if success:
                        break
                    
                    # 解压失败
                    if attempt < max_retries - 1:
                        # 还有重试机会，清理并重试
                        logger.warning(f"[{stem}] 解压失败 (尝试 {attempt + 1}/{max_retries}): {err[:100]}")
                        logger.info(f"[{stem}] 清理不完整数据并重试...")
                        ssh.exec_command(f"rm -rf '{data_dir}'")
                    else:
                        # 最后一次尝试也失败了
                        logger.error(f"[{stem}] 解压失败 ({max_retries}次尝试): {err}")
                        logger.info(f"[{stem}] 清理不完整的数据...")
                        ssh.exec_command(f"rm -rf '{data_dir}'")
                        self.result.log_error(stem, "处理", err)
                        self.result.check_failed.append(stem)
                        self.state_manager.update(stem, ProcessStatus.FAILED, err)
                        return False
            
            self.result.processed.append(stem)
            self.state_manager.update(stem, ProcessStatus.PROCESSED)
            
            # 检查标注质量
            logger.info(f"[{stem}] 🔍 检查质量...")
            passed, issue_count, report = processor.check_annotations(data_dir, stem)
            
            # 获取关键帧数量
            kf = processor.get_keyframe_count(data_dir)
            self.result.keyframe_counts[stem] = kf
            
            if not passed:
                logger.error(f"[{stem}] 检查未通过: 发现 {issue_count} 个问题帧")
                self.result.log_error(stem, "检查", f"发现 {issue_count} 个问题帧")
                self.result.check_failed.append(stem)
                self.state_manager.update(stem, ProcessStatus.CHECKED, f"检查失败: {issue_count} 个问题帧")
                local_report = self.local_check_dir / f"report_{stem}.txt"
                ssh.download_file(report, str(local_report))
                return False
            
            logger.info(f"[{stem}] ✓ 质量检查通过 ({kf}帧)")
            self.result.check_passed.append(stem)
            self.state_manager.update(stem, ProcessStatus.CHECKED)
            
            # 移动到final_dir
            logger.info(f"[{stem}] 📁 移动到final_dir...")
            success, dst = processor.move_to_final(stem)
            if success:
                logger.info(f"[{stem}] ✓ 完成")
                self.result.moved_to_final.append(stem)
                if self.server_logger:
                    self.server_logger.log_success(stem, kf)
                self.state_manager.update(stem, ProcessStatus.COMPLETED)
                
                # 备份到NAS（如果启用）
                if self.nas_backup and self.nas_backup.is_enabled:
                    print(f"  💾 备份到NAS: {stem}")
                    logger.info(f"[{stem}] 💾 备份到NAS...")
                    backup_success, backup_msg = self.nas_backup.backup_data(
                        source_dir=dst,
                        final_dir=server.final_dir,
                        data_name=stem
                    )
                    if backup_success:
                        print(f"  ✓ NAS备份成功: {stem}")
                        logger.info(f"[{stem}] ✓ NAS备份成功")
                        self.result.backed_up.append(stem)
                    else:
                        print(f"  ✗ NAS备份失败: {stem} - {backup_msg}")
                        logger.warning(f"[{stem}] ✗ NAS备份失败: {backup_msg}")
                        self.result.backup_failed.append(stem)
                        # 根据配置决定是否继续
                        backup_config = self.nas_backup.config.get('backup', {})
                        if backup_config.get('on_error', 'continue') == 'stop':
                            self.result.log_error(stem, "NAS备份", backup_msg)
                            return False
            else:
                logger.error(f"[{stem}] ✗ 移动失败: {dst}")
                self.result.log_error(stem, "移动", dst)
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"[{stem}] 异常: {e}")
            self.result.log_error(stem, "异常", str(e))
            self.result.check_failed.append(stem)
            self.state_manager.update(stem, ProcessStatus.FAILED, str(e))
            if self.server_logger:
                self.server_logger.log_failure(stem, str(e))
            return False
    
    def _process_with_pool(self, pool: SSHConnectionPool, json_file: Path, 
                           stem: str, state: Dict) -> bool:
        """使用连接池处理单个文件"""
        ssh = None
        try:
            ssh = pool.get()
            if not ssh or not ssh.is_connected:
                self.result.log_error(stem, "连接", "无法获取SSH连接")
                with self._lock:
                    self.result.check_failed.append(stem)
                return False
            
            processor = RemoteProcessor(ssh, self.config)
            
            # 线程安全的脚本部署（只部署一次）
            with self._deploy_lock:
                if not self._scripts_deployed:
                    processor.deploy_scripts()
                    self._scripts_deployed = True
            
            return self._process_single(ssh, processor, json_file, stem, state)
        finally:
            if ssh:
                pool.put(ssh)
    
    def _process_single_threaded(self, json_file: Path, stem: str, state: Dict) -> bool:
        """处理单个文件（独立SSH连接，用于并行模式）- 已弃用，保留兼容"""
        with SSHClient() as ssh:
            if not ssh.is_connected:
                self.result.log_error(stem, "连接", "SSH连接失败")
                with self._lock:
                    self.result.check_failed.append(stem)
                return False
            
            processor = RemoteProcessor(ssh, self.config)
            processor.deploy_scripts()
            return self._process_single(ssh, processor, json_file, stem, state)
    
    def _print_summary(self):
        """打印执行汇总"""
        print()
        print("╔" + "═" * 50 + "╗")
        print("║  📊 执行汇总".ljust(51) + "║")
        print("╠" + "═" * 50 + "╣")
        
        stats = [
            ("⏭ 跳过(已存在)", len(self.result.skipped_server_exists)),
            ("⬇ 下载成功", len(self.result.downloaded)),
            ("⬇ 下载失败", len(self.result.download_failed)),
            ("⬆ 上传成功", len(self.result.uploaded)),
            ("⚙ 处理成功", len(self.result.processed)),
            ("✓ 检查通过", len(self.result.check_passed)),
            ("✗ 检查失败", len(self.result.check_failed)),
            ("📁 已移动", len(self.result.moved_to_final)),
            ("💾 NAS备份成功", len(self.result.backed_up)),
            ("💾 NAS备份失败", len(self.result.backup_failed)),
        ]
        
        total_kf = sum(self.result.keyframe_counts.values())
        if total_kf > 0:
            stats.append(("📊 总关键帧", total_kf))
        
        for label, count in stats:
            line = f"║  {label}: {count}"
            print(line.ljust(51) + "║")
        
        print("╚" + "═" * 50 + "╝")
        
        if self.result.check_failed:
            print()
            print("  ⚠ 检查未通过的数据:")
            for name in self.result.check_failed:
                print(f"    • {name}")
        
        if self.result.errors:
            print()
            print("  ❌ 失败详情:")
            for stem, error_list in self.result.errors.items():
                print(f"    ┌─ {stem}")
                for step, msg in error_list:
                    # 显示完整错误信息，支持多行
                    print(f"    │  [{step}]")
                    for line in msg.split('\n'):
                        if line.strip():
                            print(f"    │    {line}")
                print(f"    └─")
    
    def _track_single_to_feishu(self, tracker: Tracker, stem: str, silent: bool = False):
        """单个数据包完成后立即同步飞书"""
        try:
            kf = self.result.keyframe_counts.get(stem, 0)
            status = "已完成" if stem in self.result.check_passed else "检查不通过"
            uploaded = stem in self.result.moved_to_final or stem in self.result.skipped_server_exists
            
            record = TrackingRecord(
                name=stem,
                keyframe_count=kf,
                annotation_status=status,
                uploaded=uploaded,
            )
            
            result = tracker.track([record], str(self.json_dir), "configs/pipeline.yaml")
            if result and not silent:
                logger.info(f"飞书已同步: {stem}")
        except Exception as e:
            logger.warning(f"飞书同步失败 {stem}: {e}")
    
    def _track_to_feishu(self):
        """将处理结果同步到飞书表格（包括跳过的文件）"""
        try:
            tracker = Tracker()
            
            # 收集所有需要同步的记录
            records = []
            all_names = set()
            all_names.update(self.result.skipped_server_exists)
            all_names.update(self.result.check_passed)
            all_names.update(self.result.check_failed)
            all_names.update(self.result.moved_to_final)
            
            for name in sorted(all_names):
                status = "已完成" if name in self.result.check_passed else "检查不通过"
                uploaded = name in self.result.moved_to_final or name in self.result.skipped_server_exists
                records.append(TrackingRecord(
                    name=name,
                    keyframe_count=self.result.keyframe_counts.get(name, 0),
                    annotation_status=status,
                    uploaded=uploaded,
                ))
            
            if not records:
                return
            
            print()
            print(f"  📤 同步到飞书: {len(records)} 条记录...")
            
            result = tracker.track(records, str(self.json_dir), "configs/pipeline.yaml")
            
            if result:
                created = result.get('created', 0)
                updated = result.get('updated', 0)
                if isinstance(created, list):
                    created = len(created)
                if isinstance(updated, list):
                    updated = len(updated)
                print(f"  ✅ 飞书同步完成: 新增 {created}, 更新 {updated}")
        except Exception as e:
            logger.warning(f"飞书追踪失败: {e}")
            print(f"  ⚠ 飞书同步失败: {e}")
