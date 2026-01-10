"""
标注数据处理流水线
模块化重构版本

使用方法:
    from src.pipeline import PipelineRunner
    
    runner = PipelineRunner("/path/to/jsons")
    result = runner.run(mode="optimized", workers=3)
"""

from .config import (
    PipelineConfig,
    ServerConfig,
    DataWeaveConfig,
    get_config,
    set_config,
)
from .ssh_client import SSHClient, create_ssh_client
from .downloader import Downloader, TokenManager
from .uploader import Uploader
from .processor import RemoteProcessor
from .checker import AnnotationChecker
from .tracker import Tracker, TrackingRecord, create_tracking_records
from .server_logger import ServerLogger, ProcessingRecord
from .runner import PipelineRunner, PipelineResult, ProgressTracker

__all__ = [
    # 配置
    "PipelineConfig",
    "ServerConfig", 
    "DataWeaveConfig",
    "get_config",
    "set_config",
    # SSH
    "SSHClient",
    "create_ssh_client",
    # 下载
    "Downloader",
    "TokenManager",
    # 上传
    "Uploader",
    # 处理
    "RemoteProcessor",
    # 检查
    "AnnotationChecker",
    # 追踪
    "Tracker",
    "TrackingRecord",
    "create_tracking_records",
    # 服务器日志
    "ServerLogger",
    "ProcessingRecord",
    # 运行器
    "PipelineRunner",
    "PipelineResult",
    "ProgressTracker",
]

__version__ = "2.0.0"
__name__ = "AnnotaPipe"
