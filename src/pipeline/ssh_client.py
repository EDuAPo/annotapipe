"""
SSH 客户端模块
封装 SSH/SFTP 操作，支持连接池和重试
"""
import logging
from pathlib import Path
from typing import Optional, Tuple
import paramiko

from .config import ServerConfig, get_config

logger = logging.getLogger(__name__)


class SSHClient:
    """SSH 客户端，封装常用操作"""
    
    def __init__(self, server: ServerConfig = None):
        self.server = server or get_config().get_available_server()
        self._ssh: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None
    
    @property
    def is_connected(self) -> bool:
        return self._ssh is not None and self._ssh.get_transport() is not None
    
    def connect(self, timeout: int = 10) -> bool:
        """建立 SSH 连接"""
        if self.is_connected:
            return True
        
        try:
            self._ssh = paramiko.SSHClient()
            self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._ssh.connect(
                self.server.ip, 
                username=self.server.user, 
                timeout=timeout
            )
            self._sftp = self._ssh.open_sftp()
            logger.info(f"✅ SSH 连接成功: {self.server.ip}")
            return True
        except Exception as e:
            logger.error(f"SSH 连接失败: {e}")
            self._ssh = None
            self._sftp = None
            return False
    
    def close(self):
        """关闭连接"""
        if self._sftp:
            self._sftp.close()
        if self._ssh:
            self._ssh.close()
        self._ssh = None
        self._sftp = None
    
    def exec_command(self, cmd: str, timeout: int = 60) -> Tuple[int, str, str]:
        """执行远程命令"""
        if not self.is_connected:
            return -1, "", "Not connected"
        
        try:
            stdin, stdout, stderr = self._ssh.exec_command(cmd, timeout=timeout)
            exit_status = stdout.channel.recv_exit_status()
            out = stdout.read().decode().strip()
            err = stderr.read().decode().strip()
            return exit_status, out, err
        except Exception as e:
            return -1, "", str(e)
    
    def upload_file(self, local_path: str, remote_path: str, 
                    progress_callback=None, verify_md5: bool = True) -> bool:
        """
        上传文件（使用临时文件确保完整性）
        
        流程:
        1. 上传到 .uploading 临时文件
        2. 验证文件大小
        3. 验证 MD5 校验和（默认启用）
        4. 成功后重命名为正式文件
        5. 失败时自动清理临时文件
        """
        if not self.is_connected:
            return False
        
        temp_path = f"{remote_path}.uploading"
        local_size = Path(local_path).stat().st_size
        
        try:
            # 上传到临时文件
            self._sftp.put(str(local_path), temp_path, callback=progress_callback)
            
            # 验证文件大小
            remote_stat = self._sftp.stat(temp_path)
            if remote_stat.st_size != local_size:
                self.exec_command(f"rm -f '{temp_path}'")
                raise Exception(f"上传不完整: 本地 {local_size}, 远程 {remote_stat.st_size}")
            
            # 可选：验证 MD5（对于大文件可能较慢）
            if verify_md5:
                import hashlib
                with open(local_path, 'rb') as f:
                    local_md5 = hashlib.md5(f.read()).hexdigest()
                status, remote_md5, _ = self.exec_command(f"md5sum '{temp_path}' | cut -d' ' -f1")
                if status == 0 and remote_md5.strip() != local_md5:
                    self.exec_command(f"rm -f '{temp_path}'")
                    raise Exception(f"MD5 校验失败")
            
            # 如果目标文件已存在，先删除
            self.exec_command(f"rm -f '{remote_path}'")
            
            # 重命名为正式文件（原子操作）
            status, _, err = self.exec_command(f"mv '{temp_path}' '{remote_path}'")
            if status != 0:
                raise Exception(f"重命名失败: {err}")
            
            return True
        except Exception as e:
            # 清理临时文件
            self.exec_command(f"rm -f '{temp_path}'")
            logger.error(f"上传失败: {e}")
            return False
    
    def cleanup_uploading_files(self, remote_dir: str) -> int:
        """清理指定目录下的 .uploading 临时文件"""
        status, out, _ = self.exec_command(f"ls {remote_dir}/*.uploading 2>/dev/null || true")
        if not out:
            return 0
        
        files = [f.strip() for f in out.splitlines() if f.strip()]
        for f in files:
            self.exec_command(f"rm -f '{f}'")
            logger.info(f"🧹 清理残留临时文件: {Path(f).name}")
        return len(files)
    
    def download_file(self, remote_path: str, local_path: str,
                      progress_callback=None) -> bool:
        """下载文件"""
        if not self.is_connected:
            return False
        
        try:
            self._sftp.get(remote_path, str(local_path), callback=progress_callback)
            return True
        except Exception as e:
            logger.error(f"下载失败: {e}")
            return False
    
    def file_exists(self, remote_path: str) -> bool:
        """检查远程文件是否存在"""
        status, out, _ = self.exec_command(f"test -e '{remote_path}' && echo exists")
        return 'exists' in out
    
    def dir_exists(self, remote_path: str) -> bool:
        """检查远程目录是否存在"""
        status, out, _ = self.exec_command(f"test -d '{remote_path}' && echo exists")
        return 'exists' in out
    
    def mkdir_p(self, remote_path: str) -> bool:
        """创建远程目录（递归）"""
        status, _, _ = self.exec_command(f"mkdir -p '{remote_path}'")
        return status == 0
    
    def list_files(self, remote_dir: str, pattern: str = "*") -> list:
        """列出远程目录中的文件"""
        status, out, _ = self.exec_command(f"ls {remote_dir}/{pattern} 2>/dev/null || true")
        if not out:
            return []
        return [Path(f.strip()).name for f in out.splitlines() if f.strip()]
    
    def list_dirs(self, remote_dir: str) -> list:
        """列出远程目录中的子目录"""
        status, out, _ = self.exec_command(f"ls -d {remote_dir}/*/ 2>/dev/null || true")
        if not out:
            return []
        return [Path(d.strip().rstrip('/')).name for d in out.splitlines() if d.strip()]
    
    def write_file(self, remote_path: str, content: str):
        """写入远程文件"""
        if not self.is_connected:
            return
        with self._sftp.file(remote_path, 'w') as f:
            f.write(content)
    
    def read_file(self, remote_path: str) -> Optional[str]:
        """读取远程文件"""
        if not self.is_connected:
            return None
        try:
            with self._sftp.file(remote_path, 'r') as f:
                return f.read().decode() if hasattr(f.read(), 'decode') else f.read()
        except Exception:
            return None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def create_ssh_client(server: ServerConfig = None) -> SSHClient:
    """创建 SSH 客户端的工厂函数"""
    return SSHClient(server)
