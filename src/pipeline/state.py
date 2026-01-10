"""
状态管理模块
负责持久化流水线处理状态，支持断点续传
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from enum import Enum
import threading

logger = logging.getLogger(__name__)


class ProcessStatus(str, Enum):
    """处理状态枚举"""
    PENDING = "pending"
    DOWNLOADED = "downloaded"
    UPLOADED = "uploaded"
    PROCESSED = "processed"
    CHECKED = "checked"
    COMPLETED = "completed"
    FAILED = "failed"


class StateManager:
    """状态管理器，支持断点续传"""
    
    def __init__(self, state_dir: Path):
        self.state_file = state_dir / "pipeline_state.json"
        self._state: Dict[str, dict] = {}
        self._lock = threading.Lock()
        self._load()
    
    def _load(self):
        """加载状态文件"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    self._state = json.load(f)
                logger.info(f"📋 加载状态文件: {len(self._state)} 条记录")
            except Exception as e:
                logger.warning(f"状态文件加载失败: {e}")
                self._state = {}
    
    def _save(self):
        """保存状态文件"""
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self._state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"状态文件保存失败: {e}")
    
    def get_status(self, stem: str) -> Optional[str]:
        """获取文件状态"""
        with self._lock:
            return self._state.get(stem, {}).get("status")
    
    def update(self, stem: str, status: ProcessStatus, error: str = None):
        """更新文件状态"""
        with self._lock:
            self._state[stem] = {
                "status": status.value,
                "updated_at": datetime.now().isoformat(),
                "error": error
            }
            self._save()
    
    def is_completed(self, stem: str) -> bool:
        """检查是否已完成"""
        return self.get_status(stem) == ProcessStatus.COMPLETED.value
    
    def can_skip_download(self, stem: str) -> bool:
        """检查是否可以跳过下载"""
        status = self.get_status(stem)
        return status in [
            ProcessStatus.DOWNLOADED.value,
            ProcessStatus.UPLOADED.value,
            ProcessStatus.PROCESSED.value,
            ProcessStatus.CHECKED.value,
            ProcessStatus.COMPLETED.value
        ]
    
    def can_skip_upload(self, stem: str) -> bool:
        """检查是否可以跳过上传"""
        status = self.get_status(stem)
        return status in [
            ProcessStatus.UPLOADED.value,
            ProcessStatus.PROCESSED.value,
            ProcessStatus.CHECKED.value,
            ProcessStatus.COMPLETED.value
        ]
    
    def get_resumable(self) -> Dict[str, str]:
        """获取可恢复的任务（非完成、非失败）"""
        with self._lock:
            return {
                stem: info["status"]
                for stem, info in self._state.items()
                if info.get("status") not in [
                    ProcessStatus.COMPLETED.value,
                    ProcessStatus.FAILED.value
                ]
            }
    
    def clear_failed(self):
        """清除失败状态，允许重试"""
        with self._lock:
            for stem in list(self._state.keys()):
                if self._state[stem].get("status") == ProcessStatus.FAILED.value:
                    del self._state[stem]
            self._save()
