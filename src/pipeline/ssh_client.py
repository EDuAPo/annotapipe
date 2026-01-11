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
                    progress_callback=None, verify: bool = True,
                    resume: bool = True, chunk_size: int = 32 * 1024 * 1024) -> bool:
        """
        上传文件（支持断点续传 + 完整性验证）
        
        Args:
            local_path: 本地文件路径
            remote_path: 远程文件路径
            progress_callback: 进度回调 (transferred, total)
            verify: 是否验证完整性（默认启用）
            resume: 是否启用断点续传（默认启用）
            chunk_size: 传输分块大小，默认 32MB
        
        流程:
        1. 检查远程临时文件，获取已上传大小
        2. 验证已上传部分的完整性
        3. 从断点位置继续上传
        4. 验证文件大小
        5. 最终验证完整文件 MD5
        6. 成功后重命名为正式文件
        """
        import hashlib
        import time
        
        if not self.is_connected:
            logger.error("❌ SSH 未连接，无法上传")
            return False
        
        filename = Path(local_path).name
        temp_path = f"{remote_path}.uploading"
        local_size = Path(local_path).stat().st_size
        
        def format_size(size: int) -> str:
            """格式化文件大小"""
            if size >= 1024**3:
                return f"{size / (1024**3):.2f}GB"
            elif size >= 1024**2:
                return f"{size / (1024**2):.2f}MB"
            else:
                return f"{size / 1024:.2f}KB"
        
        def format_speed(speed: float) -> str:
            """格式化速度"""
            if speed >= 1024**2:
                return f"{speed / (1024**2):.2f}MB/s"
            elif speed >= 1024:
                return f"{speed / 1024:.2f}KB/s"
            else:
                return f"{speed:.2f}B/s"
        
        def calc_file_md5(filepath: str, max_bytes: int = None) -> str:
            """计算文件 MD5（可指定最大字节数）"""
            md5_hash = hashlib.md5()
            bytes_read = 0
            with open(filepath, 'rb') as f:
                while True:
                    if max_bytes and bytes_read >= max_bytes:
                        break
                    read_size = chunk_size
                    if max_bytes:
                        read_size = min(chunk_size, max_bytes - bytes_read)
                    data = f.read(read_size)
                    if not data:
                        break
                    md5_hash.update(data)
                    bytes_read += len(data)
            return md5_hash.hexdigest()
        
        logger.info(f"📤 开始上传: {filename} ({format_size(local_size)})")
        logger.info(f"   本地路径: {local_path}")
        logger.info(f"   远程路径: {remote_path}")
        
        try:
            # 检查已上传大小（断点续传）
            uploaded_size = 0
            if resume:
                try:
                    remote_stat = self._sftp.stat(temp_path)
                    uploaded_size = remote_stat.st_size
                    logger.info(f"📁 发现临时文件: {format_size(uploaded_size)}")
                except FileNotFoundError:
                    uploaded_size = 0
                    logger.info(f"📁 无临时文件，从头开始上传")
            
            # 验证已上传部分的完整性（这是断点续传可靠性的关键）
            if uploaded_size > 0 and verify:
                logger.info(f"🔍 验证已上传部分: {format_size(uploaded_size)}...")
                
                # 先用 sync 确保远程文件数据落盘
                self.exec_command("sync")
                
                # 重新获取文件大小（确保是落盘后的真实大小）
                try:
                    remote_stat = self._sftp.stat(temp_path)
                    actual_size = remote_stat.st_size
                    if actual_size != uploaded_size:
                        logger.warning(f"⚠️ 文件大小变化: {uploaded_size} -> {actual_size}")
                        uploaded_size = actual_size
                except Exception as e:
                    logger.warning(f"⚠️ 无法获取文件大小: {e}")
                    uploaded_size = 0
                
                if uploaded_size > 0:
                    # 计算本地对应部分的 MD5
                    logger.info(f"   计算本地 MD5 (前 {format_size(uploaded_size)})...")
                    verify_start = time.time()
                    local_partial_md5 = calc_file_md5(local_path, uploaded_size)
                    logger.info(f"   本地 MD5: {local_partial_md5[:16]}... (耗时 {time.time() - verify_start:.1f}秒)")
                    
                    # 计算远程已上传部分的 MD5（使用完整文件的 MD5，因为临时文件就是已上传部分）
                    logger.info(f"   计算远程 MD5...")
                    verify_start = time.time()
                    status, remote_partial_md5, err = self.exec_command(
                        f"md5sum '{temp_path}' | cut -d' ' -f1",
                        timeout=3600
                    )
                    
                    if status != 0:
                        logger.warning(f"⚠️ 远程 MD5 计算失败: {err}")
                        logger.warning(f"⚠️ 将删除临时文件，重新上传")
                        self.exec_command(f"rm -f '{temp_path}'")
                        uploaded_size = 0
                    elif remote_partial_md5.strip() != local_partial_md5:
                        logger.warning(f"⚠️ MD5 不匹配!")
                        logger.warning(f"   本地: {local_partial_md5}")
                        logger.warning(f"   远程: {remote_partial_md5.strip()}")
                        logger.warning(f"⚠️ 将删除临时文件，重新上传")
                        self.exec_command(f"rm -f '{temp_path}'")
                        uploaded_size = 0
                    else:
                        logger.info(f"✅ 已上传部分校验通过 (耗时 {time.time() - verify_start:.1f}秒)")
            
            # 显示断点续传信息
            if uploaded_size > 0:
                remaining = local_size - uploaded_size
                logger.info(f"🔄 断点续传: {format_size(uploaded_size)} / {format_size(local_size)} ({uploaded_size * 100 / local_size:.1f}%)")
                logger.info(f"   剩余: {format_size(remaining)}")
            
            # 如果已完成，跳过上传
            if uploaded_size >= local_size:
                logger.info(f"✅ 文件已完整上传，跳过传输阶段")
            else:
                # 分块上传（支持断点续传）
                start_time = time.time()
                start_size = uploaded_size
                last_log_time = start_time
                
                with open(local_path, 'rb') as local_file:
                    local_file.seek(uploaded_size)
                    
                    # 追加模式打开远程文件
                    mode = 'ab' if uploaded_size > 0 else 'wb'
                    with self._sftp.file(temp_path, mode) as remote_file:
                        remote_file.set_pipelined(True)
                        
                        while uploaded_size < local_size:
                            chunk = local_file.read(chunk_size)
                            if not chunk:
                                break
                            remote_file.write(chunk)
                            uploaded_size += len(chunk)
                            
                            if progress_callback:
                                progress_callback(uploaded_size, local_size)
                            
                            # 每 10 秒输出一次进度日志
                            current_time = time.time()
                            if current_time - last_log_time >= 10:
                                elapsed = current_time - start_time
                                transferred = uploaded_size - start_size
                                speed = transferred / elapsed if elapsed > 0 else 0
                                remaining = local_size - uploaded_size
                                eta = remaining / speed if speed > 0 else 0
                                
                                logger.info(f"📊 上传进度: {format_size(uploaded_size)} / {format_size(local_size)} "
                                          f"({uploaded_size * 100 / local_size:.1f}%) | "
                                          f"速度: {format_speed(speed)} | "
                                          f"剩余: {format_size(remaining)} | "
                                          f"预计: {int(eta // 60)}分{int(eta % 60)}秒")
                                last_log_time = current_time
                
                # 上传完成统计
                elapsed = time.time() - start_time
                transferred = uploaded_size - start_size
                avg_speed = transferred / elapsed if elapsed > 0 else 0
                logger.info(f"📤 传输完成: {format_size(transferred)} in {int(elapsed // 60)}分{int(elapsed % 60)}秒 (平均 {format_speed(avg_speed)})")
            
            # 最终验证文件大小
            logger.info(f"🔍 验证文件大小...")
            remote_stat = self._sftp.stat(temp_path)
            if remote_stat.st_size != local_size:
                logger.error(f"❌ 文件大小不匹配!")
                logger.error(f"   本地: {local_size} bytes ({format_size(local_size)})")
                logger.error(f"   远程: {remote_stat.st_size} bytes ({format_size(remote_stat.st_size)})")
                raise Exception(f"上传不完整: 本地 {local_size}, 远程 {remote_stat.st_size}")
            logger.info(f"✅ 文件大小匹配: {format_size(local_size)}")
            
            # 最终完整性验证
            if verify:
                logger.info(f"🔍 最终完整性验证 (计算完整文件 MD5)...")
                
                # 计算本地完整文件 MD5
                logger.info(f"   计算本地 MD5...")
                md5_start = time.time()
                local_md5 = calc_file_md5(local_path)
                logger.info(f"   本地 MD5: {local_md5} (耗时 {time.time() - md5_start:.1f}秒)")
                
                # 计算远程完整文件 MD5
                logger.info(f"   计算远程 MD5...")
                md5_start = time.time()
                status, remote_md5, err = self.exec_command(
                    f"md5sum '{temp_path}' | cut -d' ' -f1", 
                    timeout=7200  # 40GB 文件可能需要较长时间
                )
                
                if status != 0:
                    logger.error(f"❌ 远程 MD5 计算失败: {err}")
                    # 不删除临时文件，保留断点续传能力
                    raise Exception(f"最终 MD5 校验失败: 远程计算失败 - {err}")
                
                remote_md5 = remote_md5.strip()
                logger.info(f"   远程 MD5: {remote_md5} (耗时 {time.time() - md5_start:.1f}秒)")
                
                if remote_md5 != local_md5:
                    logger.error(f"❌ MD5 校验失败!")
                    logger.error(f"   本地: {local_md5}")
                    logger.error(f"   远程: {remote_md5}")
                    # MD5 不匹配说明数据损坏，删除临时文件
                    self.exec_command(f"rm -f '{temp_path}'")
                    raise Exception(f"最终 MD5 校验失败: 数据损坏，本地 {local_md5}, 远程 {remote_md5}")
                
                logger.info(f"✅ 完整性验证通过!")
            
            # 如果目标文件已存在，先删除
            if self.file_exists(remote_path):
                logger.info(f"🗑️ 删除已存在的目标文件...")
                self.exec_command(f"rm -f '{remote_path}'")
            
            # 重命名为正式文件（原子操作）
            logger.info(f"📝 重命名临时文件为正式文件...")
            status, _, err = self.exec_command(f"mv '{temp_path}' '{remote_path}'")
            if status != 0:
                logger.error(f"❌ 重命名失败: {err}")
                raise Exception(f"重命名失败: {err}")
            
            logger.info(f"🎉 上传成功: {filename}")
            return True
            
        except Exception as e:
            # 断点续传模式下不删除临时文件，以便下次继续
            if resume:
                logger.info(f"💾 保留临时文件以便断点续传: {temp_path}")
            else:
                logger.info(f"🗑️ 清理临时文件...")
                self.exec_command(f"rm -f '{temp_path}'")
            logger.error(f"❌ 上传失败: {filename} - {e}")
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
