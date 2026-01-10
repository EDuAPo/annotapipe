"""
流水线运行器
负责编排整个处理流程，支持串行/并行模式
"""
import sys
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
from .tracker import Tracker, create_tracking_records

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
            
            # 清理残留的临时文件（上次异常中断可能留下的）
            cleaned = ssh.cleanup_uploading_files(ssh.server.zip_dir)
            if cleaned > 0:
                print(f"  🧹 清理残留临时文件: {cleaned} 个")
            
            # 获取服务器状态
            state = processor.get_server_state()
            print(f"  📊 服务器状态: {len(state['zip_files'])} ZIPs / {len(state['processed_dirs'])} 已完成")
            
            # 过滤需要处理的文件
            files_to_process = []
            for json_file in json_files:
                stem = json_file.stem
                if stem in state['processed_dirs']:
                    # 服务器上已完成的文件，记录为跳过
                    self.result.skipped_server_exists.append(stem)
                    self.result.check_passed.append(stem)
                    # 获取关键帧数量
                    kf = processor.get_keyframe_count(f"{ssh.server.final_dir}/{stem}")
                    self.result.keyframe_counts[stem] = kf
                else:
                    files_to_process.append((json_file, stem))
            
            skipped = len(json_files) - len(files_to_process)
            if skipped > 0:
                print(f"  ⏭ 跳过已完成: {skipped} 个")
            
            if not files_to_process:
                print("  ✓ 所有文件都已处理完成")
                self._print_summary()
                # 飞书追踪：即使全部跳过也要同步
                self._track_to_feishu()
                return self.result
            
            print(f"  📦 待处理: {len(files_to_process)} 个文件")
            print(f"  🧵 并发数: {workers}")
            print()
            
            if mode == "optimized":
                self._run_optimized(ssh, processor, files_to_process, state, workers)
            elif mode == "parallel":
                self._run_parallel(processor, files_to_process, state, workers)
            else:
                self._run_streaming(ssh, processor, files_to_process, state)
        
        self._print_summary()
        
        # 飞书追踪：记录所有处理过的数据（包括跳过的）
        self._track_to_feishu()
        
        return self.result
    
    def _run_optimized(self, ssh: SSHClient, processor: RemoteProcessor,
                       files: List[tuple], state: Dict, workers: int):
        """优化模式：下载并行 + 服务器操作串行"""
        
        # 阶段1: 并行下载
        print("=" * 50)
        print("  📥 阶段1: 并行下载 ZIP 文件")
        print("=" * 50)
        
        files_to_download = []
        for json_file, stem in files:
            zip_name = f"{stem}.zip"
            local_zip = self.local_zip_dir / zip_name
            
            if zip_name in state['zip_files']:
                self.result.skipped_server_exists.append(stem)
                continue
            if self.downloader.is_valid_zip(local_zip):
                self.result.downloaded.append(stem)
                continue
            files_to_download.append((stem, zip_name, local_zip))
        
        if files_to_download:
            print(f"  需下载: {len(files_to_download)} 个文件")
            progress = ProgressTracker(len(files_to_download), "下载进度")
            
            with ThreadPoolExecutor(max_workers=self.config.download_workers) as executor:
                futures = {}
                for stem, zip_name, local_zip in files_to_download:
                    future = executor.submit(self.downloader.download_file, zip_name, local_zip)
                    futures[future] = stem
                
                for future in as_completed(futures):
                    stem = futures[future]
                    try:
                        success = future.result()
                        with self._lock:
                            if success:
                                self.result.downloaded.append(stem)
                            else:
                                self.result.download_failed.append(stem)
                        progress.update(success=success, name=stem)
                    except Exception:
                        with self._lock:
                            self.result.download_failed.append(stem)
                        progress.update(success=False, name=f"{stem} (异常)")
            
            progress.summary()
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
        
        progress = ProgressTracker(len(files_for_server), "服务器处理")
        
        for json_file, stem in files_for_server:
            success = self._process_single(ssh, processor, json_file, stem, state)
            progress.update(success=success, name=stem)
        
        progress.summary()
    
    def _run_parallel(self, processor: RemoteProcessor, files: List[tuple], 
                      state: Dict, workers: int):
        """全并行模式：使用连接池复用 SSH 连接"""
        progress = ProgressTracker(len(files), "并行处理")
        pool = SSHConnectionPool(size=workers)
        
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
                        progress.update(success=success, name=stem)
                    except Exception as e:
                        logger.error(f"并行处理异常 {stem}: {e}")
                        progress.update(success=False, name=f"{stem} (异常)")
        finally:
            pool.close_all()
        
        progress.summary()
    
    def _run_streaming(self, ssh: SSHClient, processor: RemoteProcessor,
                       files: List[tuple], state: Dict):
        """流式模式：下载一个处理一个"""
        progress = ProgressTracker(len(files), "流式处理")
        
        for json_file, stem in files:
            success = self._process_single(ssh, processor, json_file, stem, state)
            progress.update(success=success, name=stem)
        
        progress.summary()
    
    def _process_single(self, ssh: SSHClient, processor: RemoteProcessor,
                        json_file: Path, stem: str, state: Dict) -> bool:
        """处理单个文件（使用共享SSH连接）"""
        zip_name = f"{stem}.zip"
        local_zip = self.local_zip_dir / zip_name
        server = ssh.server
        remote_zip = f"{server.zip_dir}/{zip_name}"
        
        try:
            # 下载
            if zip_name not in state['zip_files'] and not self.downloader.is_valid_zip(local_zip):
                if not self.downloader.download_file(zip_name, local_zip):
                    self.result.log_error(stem, "下载", "下载失败")
                    self.result.check_failed.append(stem)
                    return False
                self.result.downloaded.append(stem)
            
            # 上传
            if zip_name not in state['zip_files'] and local_zip.exists():
                if not ssh.upload_file(str(local_zip), remote_zip):
                    self.result.log_error(stem, "上传", "上传失败")
                    self.result.check_failed.append(stem)
                    return False
                self.result.uploaded.append(stem)
            
            # 处理
            success, err = processor.process_zip(remote_zip, str(json_file), stem)
            if not success:
                self.result.log_error(stem, "处理", err)
                self.result.check_failed.append(stem)
                return False
            self.result.processed.append(stem)
            
            # 检查
            data_dir = f"{server.process_dir}/{stem}"
            passed, issue_count, report = processor.check_annotations(data_dir, stem)
            
            # 获取关键帧数量
            kf = processor.get_keyframe_count(data_dir)
            self.result.keyframe_counts[stem] = kf
            
            if not passed:
                self.result.log_error(stem, "检查", f"发现 {issue_count} 个问题帧")
                self.result.check_failed.append(stem)
                # 下载报告
                local_report = self.local_check_dir / f"report_{stem}.txt"
                ssh.download_file(report, str(local_report))
                return False
            
            self.result.check_passed.append(stem)
            
            # 移动
            success, dst = processor.move_to_final(stem)
            if success:
                self.result.moved_to_final.append(stem)
                # 清理本地ZIP
                if local_zip.exists():
                    local_zip.unlink()
                # 记录服务器日志
                if self.server_logger:
                    self.server_logger.log_success(stem, kf)
            else:
                self.result.log_error(stem, "移动", dst)
            
            return True
            
        except Exception as e:
            self.result.log_error(stem, "异常", str(e))
            self.result.check_failed.append(stem)
            # 记录失败日志
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
                    display_msg = msg[:60] + "..." if len(msg) > 60 else msg
                    print(f"    │  [{step}] {display_msg}")
                print(f"    └─")
    
    def _track_to_feishu(self):
        """将处理结果同步到飞书表格（包括跳过的文件）"""
        try:
            tracker = Tracker()
            records = create_tracking_records(self.result, self.result.keyframe_counts)
            
            if not records:
                logger.info("没有需要追踪的记录")
                return
            
            print()
            print(f"  📤 同步到飞书: {len(records)} 条记录...")
            
            result = tracker.track(records, str(self.json_dir))
            
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
