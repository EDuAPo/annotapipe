"""
服务器端日志模块
在远程服务器上记录数据处理日志，便于追溯
"""
import json
import socket
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

from .ssh_client import SSHClient
from .config import get_config

logger = logging.getLogger(__name__)

# 默认日志路径
DEFAULT_LOG_DIR = "/data02/logs"
DEFAULT_LOG_FILE = "pipeline.log"


@dataclass
class ProcessingRecord:
    """处理记录"""
    timestamp: str
    data_name: str
    status: str  # success, failed, skipped
    keyframe_count: int = 0
    error_message: str = ""
    source_host: str = ""
    source_user: str = ""
    duration_seconds: float = 0
    steps_completed: List[str] = None
    
    def __post_init__(self):
        if self.steps_completed is None:
            self.steps_completed = []
        if not self.source_host:
            self.source_host = socket.gethostname()
        if not self.source_user:
            import getpass
            self.source_user = getpass.getuser()
    
    def to_json(self) -> str:
        """转换为 JSON 字符串"""
        return json.dumps(asdict(self), ensure_ascii=False)
    
    @classmethod
    def from_json(cls, json_str: str) -> "ProcessingRecord":
        """从 JSON 字符串创建"""
        data = json.loads(json_str)
        return cls(**data)


class ServerLogger:
    """服务器端日志记录器"""
    
    def __init__(self, ssh: SSHClient, log_dir: str = None):
        self.ssh = ssh
        self.config = get_config()
        self.log_dir = log_dir or DEFAULT_LOG_DIR
        self.log_file = f"{self.log_dir}/{DEFAULT_LOG_FILE}"
        self._ensure_log_dir()
    
    def _ensure_log_dir(self):
        """确保日志目录存在"""
        self.ssh.mkdir_p(self.log_dir)
    
    def log(self, record: ProcessingRecord):
        """记录一条处理日志"""
        log_line = record.to_json()
        
        # 追加到日志文件
        cmd = f"echo '{log_line}' >> '{self.log_file}'"
        status, _, err = self.ssh.exec_command(cmd)
        
        if status != 0:
            logger.warning(f"写入服务器日志失败: {err}")
        else:
            logger.debug(f"服务器日志已记录: {record.data_name}")
    
    def log_success(self, data_name: str, keyframe_count: int = 0, 
                    duration: float = 0, steps: List[str] = None):
        """记录成功处理"""
        record = ProcessingRecord(
            timestamp=datetime.now().isoformat(),
            data_name=data_name,
            status="success",
            keyframe_count=keyframe_count,
            duration_seconds=duration,
            steps_completed=steps or ["download", "upload", "process", "check", "move"]
        )
        self.log(record)
    
    def log_failure(self, data_name: str, error_message: str,
                    keyframe_count: int = 0, duration: float = 0,
                    steps: List[str] = None):
        """记录处理失败"""
        record = ProcessingRecord(
            timestamp=datetime.now().isoformat(),
            data_name=data_name,
            status="failed",
            keyframe_count=keyframe_count,
            error_message=error_message,
            duration_seconds=duration,
            steps_completed=steps or []
        )
        self.log(record)
    
    def log_skipped(self, data_name: str, keyframe_count: int = 0):
        """记录跳过（已存在）"""
        record = ProcessingRecord(
            timestamp=datetime.now().isoformat(),
            data_name=data_name,
            status="skipped",
            keyframe_count=keyframe_count,
            steps_completed=[]
        )
        self.log(record)
    
    def get_recent_logs(self, count: int = 50) -> List[ProcessingRecord]:
        """获取最近的日志记录"""
        cmd = f"tail -n {count} '{self.log_file}' 2>/dev/null || true"
        status, out, _ = self.ssh.exec_command(cmd)
        
        records = []
        if out:
            for line in out.splitlines():
                line = line.strip()
                if line:
                    try:
                        records.append(ProcessingRecord.from_json(line))
                    except Exception:
                        pass
        
        return records
    
    def get_logs_by_date(self, date: str) -> List[ProcessingRecord]:
        """获取指定日期的日志（格式：2024-01-10）"""
        cmd = f"grep '{date}' '{self.log_file}' 2>/dev/null || true"
        status, out, _ = self.ssh.exec_command(cmd)
        
        records = []
        if out:
            for line in out.splitlines():
                line = line.strip()
                if line:
                    try:
                        records.append(ProcessingRecord.from_json(line))
                    except Exception:
                        pass
        
        return records
    
    def get_failed_logs(self, count: int = 100) -> List[ProcessingRecord]:
        """获取失败的日志记录"""
        cmd = f"grep '\"status\": \"failed\"' '{self.log_file}' | tail -n {count} 2>/dev/null || true"
        status, out, _ = self.ssh.exec_command(cmd)
        
        records = []
        if out:
            for line in out.splitlines():
                line = line.strip()
                if line:
                    try:
                        records.append(ProcessingRecord.from_json(line))
                    except Exception:
                        pass
        
        return records
    
    def get_statistics(self) -> Dict:
        """获取日志统计信息"""
        stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "total_keyframes": 0,
        }
        
        # 统计总数
        cmd = f"wc -l < '{self.log_file}' 2>/dev/null || echo 0"
        _, out, _ = self.ssh.exec_command(cmd)
        stats["total"] = int(out.strip()) if out.strip().isdigit() else 0
        
        # 统计成功数
        cmd = f"grep -c '\"status\": \"success\"' '{self.log_file}' 2>/dev/null || echo 0"
        _, out, _ = self.ssh.exec_command(cmd)
        stats["success"] = int(out.strip()) if out.strip().isdigit() else 0
        
        # 统计失败数
        cmd = f"grep -c '\"status\": \"failed\"' '{self.log_file}' 2>/dev/null || echo 0"
        _, out, _ = self.ssh.exec_command(cmd)
        stats["failed"] = int(out.strip()) if out.strip().isdigit() else 0
        
        # 统计跳过数
        cmd = f"grep -c '\"status\": \"skipped\"' '{self.log_file}' 2>/dev/null || echo 0"
        _, out, _ = self.ssh.exec_command(cmd)
        stats["skipped"] = int(out.strip()) if out.strip().isdigit() else 0
        
        return stats
    
    def print_summary(self):
        """打印日志摘要"""
        stats = self.get_statistics()
        
        print()
        print("╔" + "═" * 40 + "╗")
        print("║  📋 服务器处理日志统计".ljust(41) + "║")
        print("╠" + "═" * 40 + "╣")
        print(f"║  总记录数: {stats['total']}".ljust(41) + "║")
        print(f"║  成功: {stats['success']}".ljust(41) + "║")
        print(f"║  失败: {stats['failed']}".ljust(41) + "║")
        print(f"║  跳过: {stats['skipped']}".ljust(41) + "║")
        print("╚" + "═" * 40 + "╝")
    
    def rotate_logs(self, max_size_mb: int = 100):
        """日志轮转（当日志文件超过指定大小时）"""
        # 检查文件大小
        cmd = f"stat -f%z '{self.log_file}' 2>/dev/null || stat -c%s '{self.log_file}' 2>/dev/null || echo 0"
        _, out, _ = self.ssh.exec_command(cmd)
        
        try:
            size_bytes = int(out.strip())
            size_mb = size_bytes / (1024 * 1024)
            
            if size_mb > max_size_mb:
                # 轮转日志
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_file = f"{self.log_file}.{timestamp}"
                
                self.ssh.exec_command(f"mv '{self.log_file}' '{backup_file}'")
                self.ssh.exec_command(f"gzip '{backup_file}'")
                
                logger.info(f"日志已轮转: {backup_file}.gz")
        except Exception as e:
            logger.warning(f"日志轮转失败: {e}")
