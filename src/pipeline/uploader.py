"""
上传模块
负责将本地 ZIP 文件上传到远程服务器
"""
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple

from .ssh_client import SSHClient
from .config import get_config

logger = logging.getLogger(__name__)


class Uploader:
    """ZIP 文件上传器"""
    
    def __init__(self, ssh: SSHClient):
        self.ssh = ssh
        self.config = get_config()
    
    def get_remote_files(self) -> Set[str]:
        """获取服务器上已有的 ZIP 文件（去除 processed_ 前缀）"""
        server = self.ssh.server
        files = set()
        
        for name in self.ssh.list_files(server.zip_dir, "*.zip"):
            if name.startswith("processed_"):
                files.add(name[len("processed_"):])
            else:
                files.add(name)
        
        return files
    
    def upload_file(self, local_path: Path, progress_callback=None, 
                    verify: bool = True) -> Tuple[bool, str]:
        """
        上传单个文件到服务器（支持断点续传 + 分块校验）
        
        Args:
            local_path: 本地文件路径
            progress_callback: 进度回调
            verify: 是否验证完整性（默认启用，确保数据完整性）
        
        返回 (success, error_message)
        """
        server = self.ssh.server
        remote_path = f"{server.zip_dir}/{local_path.name}"
        
        if not local_path.exists():
            return False, f"本地文件不存在: {local_path}"
        
        # 大文件使用断点续传 + 分块校验确保数据完整性
        success = self.ssh.upload_file(
            str(local_path), remote_path, 
            progress_callback=progress_callback,
            verify=verify,
            resume=True
        )
        
        if success:
            return True, ""
        else:
            return False, "上传失败（临时文件已保留，下次可断点续传）"
    
    def upload_batch(self, files: List[Path], 
                     skip_existing: bool = True,
                     server_exists: Set[str] = None) -> Dict[str, bool]:
        """
        批量上传文件
        返回 {filename: success}
        """
        results = {}
        server_exists = server_exists or self.get_remote_files()
        
        for local_path in files:
            filename = local_path.name
            
            # 跳过服务器已存在的
            if skip_existing and filename in server_exists:
                logger.info(f"跳过已存在: {filename}")
                results[filename] = True
                continue
            
            success, err = self.upload_file(local_path)
            results[filename] = success
            
            if success:
                logger.info(f"上传成功: {filename}")
            else:
                logger.error(f"上传失败: {filename} - {err}")
        
        return results
    
    def cleanup_incomplete(self, force: bool = False):
        """
        清理服务器上不完整的上传文件（.uploading 临时文件）
        
        ⚠️ 警告：这会删除所有临时文件，破坏断点续传功能！
        只有在确定不需要断点续传时才调用此方法。
        
        Args:
            force: 是否强制清理（不提示警告）
        """
        server = self.ssh.server
        
        status, out, _ = self.ssh.exec_command(
            f"ls {server.zip_dir}/*.uploading 2>/dev/null || true"
        )
        
        if out:
            uploading_files = [f.strip() for f in out.splitlines() if f.strip()]
            if uploading_files:
                if not force:
                    logger.warning(f"⚠️ 即将清理 {len(uploading_files)} 个临时文件，这会破坏断点续传功能！")
                logger.info(f"🧹 发现 {len(uploading_files)} 个未完成的上传，正在清理...")
                for f in uploading_files:
                    self.ssh.exec_command(f"rm -f '{f}'")
                    logger.info(f"  已删除: {Path(f).name}")
                logger.info(f"✅ 清理完成")
        else:
            logger.info(f"✅ 没有需要清理的临时文件")
