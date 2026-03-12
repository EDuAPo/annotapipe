"""
NAS备份工具
用于备份服务器上已处理的数据到群晖NAS
支持断点续传和增量备份
"""
import os
import sys
import time
import json
import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

# 需要安装的依赖：paramiko
try:
    import paramiko
except ImportError as e:
    print(f"缺少依赖库，请安装: pip install paramiko")
    print(f"错误: {e}")
    sys.exit(1)

logger = logging.getLogger(__name__)

# ========== SFTP传输优化配置 ==========
# 增大窗口大小和包大小可以显著提高大文件传输速度
SFTP_WINDOW_SIZE = 64 * 1024 * 1024  # 64MB窗口 (默认 2MB)
SFTP_MAX_PACKET_SIZE = 64 * 1024      # 64KB包大小 (默认 32KB)
# =============================================

# ========== NFS配置 ==========
# NFS比SMB快约10-20倍，推荐使用
USE_NFS = True  # True=使用NFS（更快），False=使用SMB
NFS_MOUNT_POINT = "/mnt/nas_backup"  # NFS挂载点
NFS_EXPORT = "/volume1/public"  # NAS导出路径
# =============================

# ========== 远程备份模式 ==========
# 网络分析结果：
# - 本地PC (192.168.2.31) 无法访问服务器 (222.223.112.212)  
# - 服务器 (222.223.112.212) 无法访问NAS (192.168.2.41)
# - 本地PC 可以访问NAS
# 结论：只能使用本地PC中转模式
REMOTE_BACKUP_MODE = False  # False=本地中转模式（实际可用）
NAS_IP = "192.168.2.41"  # NAS IP地址
SERVER_NFS_MOUNT = "/mnt/nas_backup"  # 服务器上NFS挂载点（暂不可用）
# ================================

# ========== 配置区域 - 请根据需要修改 ==========
SERVER_HOST = "222.223.112.212"  # 服务器IP地址
SERVER_USER = "user"    # 服务器用户名
USE_PASSWORD = True     # True=使用密码认证, False=使用SSH密钥
SERVER_PASSWORD = "G8#fB3$nY1*vP6&tW4"  # 如果使用密码，填写这里
SERVER_KEY_PATH = "/path/to/your/ssh/key"  # 如果使用密钥，填写私钥文件路径
SOURCE_PATH = "/data01/dataset/lines"  # 服务器源数据路径
NAS_SHARE = "public"  # NAS共享名
TARGET_PATH = "from_rere/lines"   # NAS目标路径（相对于共享根目录）
MAX_WORKERS = 4       # 并发数
STATE_FILE = "nas_backup_state.json"  # 状态文件路径
# =============================================


@dataclass
class BackupConfig:
    """备份配置"""
    server_host: str
    server_user: str
    use_password: bool
    server_password: str
    server_key_path: str
    nas_ip: str = NAS_IP
    nas_user: str = "SYSC"
    nas_password: str = "Nas123456"
    nas_share: str = NAS_SHARE
    source_path: str = SOURCE_PATH
    target_path: str = TARGET_PATH
    max_workers: int = MAX_WORKERS
    state_file: str = STATE_FILE
    # NFS配置
    use_nfs: bool = USE_NFS
    nfs_mount_point: str = NFS_MOUNT_POINT
    nfs_export: str = NFS_EXPORT
    # 远程备份模式
    remote_backup_mode: bool = REMOTE_BACKUP_MODE
    server_nfs_mount: str = SERVER_NFS_MOUNT


@dataclass
class BackupResult:
    """备份结果"""
    total_files: int = 0
    backed_up: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)
    errors: Dict[str, str] = field(default_factory=dict)


class ProgressTracker:
    """进度跟踪器"""
    
    def __init__(self, total_files: int):
        self.total_files = total_files
        self.completed = 0
        self.failed = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.current_file = ""
        self.speed_history = []
        self.lock = threading.Lock()
        self.last_displayed_percentage = -1.0  # 上次显示的百分比
        
    def update_progress(self, filename: str, success: bool):
        """更新进度"""
        with self.lock:
            self.current_file = filename
            if success:
                self.completed += 1
            else:
                self.failed += 1
            self.last_update_time = time.time()
            self._display_progress()
    
    def update_speed(self, speed_mbps: float):
        """更新速度历史"""
        with self.lock:
            self.speed_history.append(speed_mbps)
            if len(self.speed_history) > 20:  # 保留最近20个速度记录
                self.speed_history.pop(0)
    
    def _display_progress(self):
        """显示进度（单行显示）"""
        elapsed = time.time() - self.start_time
        progress = self.completed + self.failed
        percentage = (progress / self.total_files) * 100 if self.total_files > 0 else 0
        
        # 检查是否需要更新显示（对于大量文件，更频繁更新）
        update_threshold = max(1.0, 100.0 / self.total_files)  # 至少每完成1个文件更新一次
        if abs(percentage - self.last_displayed_percentage) < update_threshold and progress < self.total_files:
            return  # 不需要更新显示
        
        self.last_displayed_percentage = percentage
        
        # 计算平均速度
        avg_speed = 0
        if self.speed_history:
            avg_speed = sum(self.speed_history) / len(self.speed_history)
        
        # 估算剩余时间（基于已用时间和进度）
        eta = ""
        if progress > 0 and progress < self.total_files:
            elapsed = time.time() - self.start_time
            remaining_files = self.total_files - progress
            # 使用已用时间估算剩余时间
            eta_seconds = (elapsed / progress) * remaining_files
            
            if eta_seconds < 60:
                eta = f"{int(eta_seconds)}s"
            elif eta_seconds < 3600:
                eta = f"{int(eta_seconds/60)}m"
            else:
                eta = f"{eta_seconds/3600:.1f}h"
        
        # 构建简洁的进度条
        bar_width = 30
        filled = int(bar_width * progress / self.total_files) if self.total_files > 0 else 0
        bar = "█" * filled + "░" * (bar_width - filled)
        
        # 简洁的单行显示
        status_line = f"\r[{bar}] {percentage:.1f}% ({progress}/{self.total_files})"
        
        if avg_speed > 0:
            status_line += f" | {avg_speed:.1f}MB/s"
        
        if eta:
            status_line += f" | ETA:{eta}"
        
        print(status_line, end="", flush=True)
        
        # 如果完成，换行
        if progress >= self.total_files:
            print()


class NASBackup:
    """NAS备份器"""

    def __init__(self, config: BackupConfig):
        self.config = config
        self.ssh_client = None
        self.ssh_clients_pool = []  # SSH连接池
        self.result = BackupResult()
        self.state = self._load_state()
        self.progress_tracker = None
        self.pool_lock = threading.Lock()  # 连接池锁
        self.shutdown_flag = threading.Event()  # 停止标志

    def _load_state(self) -> Dict:
        """加载备份状态"""
        if os.path.exists(self.config.state_file):
            try:
                with open(self.config.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载状态文件失败: {e}")
        return {}

    def _get_ssh_client(self):
        """从连接池获取SSH客户端（线程安全）"""
        with self.pool_lock:
            if self.ssh_clients_pool:
                return self.ssh_clients_pool.pop()
            
            # 创建新连接
            try:
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                
                connect_kwargs = {
                    'hostname': self.config.server_host,
                    'username': self.config.server_user,
                }
                
                if self.config.use_password:
                    connect_kwargs['password'] = self.config.server_password
                else:
                    connect_kwargs['key_filename'] = self.config.server_key_path
                
                ssh_client.connect(**connect_kwargs)
                
                # 优化传输层参数（增大窗口和包大小）
                transport = ssh_client.get_transport()
                if transport:
                    transport.default_window_size = SFTP_WINDOW_SIZE
                    transport.default_max_packet_size = SFTP_MAX_PACKET_SIZE
                
                return ssh_client
            except Exception as e:
                logger.error(f"创建SSH连接失败: {e}")
                return None
    
    def _return_ssh_client(self, client):
        """将SSH客户端返回到连接池"""
        if client:
            with self.pool_lock:
                self.ssh_clients_pool.append(client)

    def connect_server(self) -> bool:
        """测试连接到服务器"""
        try:
            # 创建临时连接来测试连接性
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            connect_kwargs = {
                'hostname': self.config.server_host,
                'username': self.config.server_user,
                'timeout': 10,  # 添加10秒超时
            }
            
            if self.config.use_password:
                connect_kwargs['password'] = self.config.server_password
            else:
                if not os.path.exists(self.config.server_key_path):
                    print(f"错误：SSH密钥文件不存在: {self.config.server_key_path}")
                    print("请确保配置正确的SSH私钥文件路径")
                    return False
                connect_kwargs['key_filename'] = self.config.server_key_path
            
            ssh_client.connect(**connect_kwargs)
            ssh_client.close()  # 立即关闭测试连接
            logger.info(f"已连接到服务器: {self.config.server_host}")
            return True
        except Exception as e:
            logger.error(f"连接服务器失败: {e}")
            print(f"连接服务器失败: {e}")
            if self.config.use_password:
                print("请检查服务器IP、用户名和密码是否正确")
            else:
                print("请检查：")
                print("1. 服务器IP和用户名是否正确")
                print("2. SSH密钥文件路径是否正确")
                print("3. 密钥文件权限是否正确 (建议 chmod 600)")
            return False

    def connect_nas(self) -> bool:
        """连接到NAS"""
        if self.config.use_nfs:
            return self._connect_nas_nfs()
        else:
            return self._connect_nas_smb()

    def _connect_nas_nfs(self) -> bool:
        """使用NFS连接到NAS"""
        try:
            import subprocess
            mount_point = self.config.nfs_mount_point
            
            # 检查是否已挂载
            result = subprocess.run(['mountpoint', '-q', mount_point], capture_output=True)
            if result.returncode == 0:
                # 已挂载，测试写入权限
                test_file = f"{mount_point}/.nfs_test_{os.getpid()}"
                try:
                    with open(test_file, 'w') as f:
                        f.write('test')
                    os.remove(test_file)
                    logger.info(f"NFS已挂载且可写: {mount_point}")
                    return True
                except PermissionError:
                    print(f"错误：NFS挂载点 {mount_point} 无写入权限")
                    return False
            
            # 尝试挂载
            print(f"正在挂载NFS: {self.config.nas_ip}:{self.config.nfs_export} -> {mount_point}")
            os.makedirs(mount_point, exist_ok=True)
            mount_cmd = [
                'sudo', 'mount', '-t', 'nfs', '-o', 'vers=3,rw',
                f'{self.config.nas_ip}:{self.config.nfs_export}',
                mount_point
            ]
            result = subprocess.run(mount_cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                raise Exception(f"NFS挂载失败: {result.stderr}")
            
            logger.info(f"NFS挂载成功: {mount_point}")
            return True
            
        except Exception as e:
            logger.error(f"NFS连接失败: {e}")
            print(f"NFS连接失败: {e}")
            print("请检查：")
            print("1. NFS服务是否已启用")
            print("2. 挂载点权限是否正确")
            print("3. NAS是否允许本机IP访问")
            return False

    def _connect_nas_smb(self) -> bool:
        """使用SMB连接到NAS"""
        try:
            import subprocess
            # 测试smbclient连接
            cmd = [
                'smbclient', f'//{self.config.nas_ip}/{self.config.nas_share}',
                '-U', f'{self.config.nas_user}%{self.config.nas_password}',
                '-c', 'ls'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"已连接到NAS: {self.config.nas_ip}:{self.config.nas_share}")
                return True
            else:
                raise Exception(f"smbclient连接失败: {result.stderr}")
        except Exception as e:
            logger.error(f"连接NAS失败: {e}")
            print(f"连接NAS失败: {e}")
            print("请检查：")
            print("1. NAS IP地址是否正确")
            print("2. NAS用户名和密码是否正确")
            print("3. NAS共享名是否正确")
            print("4. NAS是否启用了SMB服务")
            print("5. 网络连接是否正常")
            print("6. 是否安装了smbclient: sudo apt install smbclient")
            return False

    def get_server_files(self) -> List[tuple]:
        """获取服务器上需要备份的文件列表（包含大小）"""
        ssh_client = self._get_ssh_client()
        if not ssh_client:
            return []
            
        try:
            # 使用find命令递归获取所有文件，并同时获取大小
            cmd = f"find '{self.config.source_path}' -type f -exec stat -c'%s %n' {{}} \\;"
            stdin, stdout, stderr = ssh_client.exec_command(cmd)
            output = stdout.read().decode('utf-8').strip()
            
            files_with_sizes = []
            for line in output.split('\n'):
                if line.strip():
                    parts = line.split(' ', 1)
                    if len(parts) == 2:
                        try:
                            size = int(parts[0])
                            path = parts[1]
                            files_with_sizes.append((path, size))
                        except ValueError:
                            continue
            
            logger.info(f"发现 {len(files_with_sizes)} 个文件待备份")
            return files_with_sizes
        except Exception as e:
            logger.error(f"获取服务器文件列表失败: {e}")
            return []
        finally:
            self._return_ssh_client(ssh_client)

    def get_file_size(self, remote_path: str) -> int:
        """获取服务器上文件的大小"""
        ssh_client = self._get_ssh_client()
        if not ssh_client:
            return 0
            
        try:
            cmd = f"stat -c%s '{remote_path}'"
            stdin, stdout, stderr = ssh_client.exec_command(cmd)
            size = int(stdout.read().decode('utf-8').strip())
            return size
        except Exception as e:
            logger.error(f"获取文件大小失败 {remote_path}: {e}")
            return 0
        finally:
            self._return_ssh_client(ssh_client)

    def check_nas_file(self, relative_path: str, expected_size: int) -> bool:
        """检查NAS上文件是否存在且完整"""
        if self.config.use_nfs:
            return self._check_nas_file_nfs(relative_path, expected_size)
        else:
            return self._check_nas_file_smb(relative_path, expected_size)

    def _check_nas_file_nfs(self, relative_path: str, expected_size: int) -> bool:
        """通过NFS检查文件是否存在且完整"""
        try:
            nas_path = os.path.join(
                self.config.nfs_mount_point,
                self.config.target_path,
                relative_path
            )
            if os.path.exists(nas_path):
                actual_size = os.path.getsize(nas_path)
                return actual_size == expected_size
            return False
        except Exception:
            return False

    def _check_nas_file_smb(self, relative_path: str, expected_size: int) -> bool:
        """通过SMB检查文件是否存在且完整"""
        try:
            import subprocess
            nas_path = f"{self.config.target_path}/{relative_path}".replace('//', '/')
            if nas_path.startswith('/'):
                nas_path = nas_path[1:]
            
            # 使用smbclient获取文件信息
            cmd = [
                'smbclient', f'//{self.config.nas_ip}/{self.config.nas_share}',
                '-U', f'{self.config.nas_user}%{self.config.nas_password}',
                '-c', f'allinfo "{nas_path}"'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                return False  # 文件不存在
            
            # 解析输出获取文件大小
            # smbclient allinfo 输出包含 "size:" 行
            for line in result.stdout.split('\n'):
                if line.strip().startswith('size:'):
                    try:
                        nas_size = int(line.split(':', 1)[1].strip())
                        return nas_size == expected_size
                    except (ValueError, IndexError):
                        break
            
            # 如果无法解析大小，假设文件存在但不完整
            return False
        except Exception:
            return False

    def backup_file(self, remote_path: str, relative_path: str, file_size: int) -> bool:
        """备份单个文件"""
        try:
            # 检查停止标志
            if self.shutdown_flag.is_set():
                return False
            
            # 检查是否已备份且完整
            if self.check_nas_file(relative_path, file_size):
                self.result.skipped.append(relative_path)
                if self.progress_tracker:
                    self.progress_tracker.update_progress(relative_path, True)
                return True

            # 检查停止标志
            if self.shutdown_flag.is_set():
                return False

            # 根据文件大小选择备份方法
            success = False
            if file_size > 1024**3:  # >1GB
                success = self._backup_large_file(remote_path, relative_path, file_size)
            else:
                success = self._backup_small_file(remote_path, relative_path)

            # 更新进度
            if self.progress_tracker:
                self.progress_tracker.update_progress(relative_path, success)
            
            return success

        except Exception as e:
            logger.error(f"备份文件失败 {remote_path}: {e}")
            self.result.failed.append(relative_path)
            self.result.errors[relative_path] = str(e)
            
            # 更新进度（失败）
            if self.progress_tracker:
                self.progress_tracker.update_progress(relative_path, False)
            
            return False

    def _upload_to_nas(self, local_path: str, relative_path: str):
        """上传文件到NAS（自动选择NFS或SMB）"""
        if self.config.use_nfs:
            self._upload_via_nfs(local_path, relative_path)
        else:
            nas_path = f"{self.config.target_path}/{relative_path}".replace('//', '/')
            if nas_path.startswith('/'):
                nas_path = nas_path[1:]
            self._upload_via_smbclient(local_path, nas_path)

    def _upload_via_nfs(self, local_path: str, relative_path: str):
        """使用NFS直接复制文件到NAS（最快）"""
        import shutil
        
        # 构建NFS目标路径
        nas_path = os.path.join(
            self.config.nfs_mount_point,
            self.config.target_path,
            relative_path
        )
        
        # 确保目标目录存在
        os.makedirs(os.path.dirname(nas_path), exist_ok=True)
        
        # 直接复制文件
        shutil.copy2(local_path, nas_path)

    def _upload_via_smbclient(self, local_path: str, nas_path: str):
        """使用smbclient上传文件到NAS"""
        import subprocess
        cmd = [
            'smbclient', f'//{self.config.nas_ip}/{self.config.nas_share}',
            '-U', f'{self.config.nas_user}%{self.config.nas_password}',
            '-c', f'put "{local_path}" "{nas_path}"'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            error_msg = result.stderr.strip() if result.stderr else f"smbclient返回码: {result.returncode}"
            raise Exception(f"smbclient上传失败: {error_msg}")

    def _backup_file_sftp(self, remote_path: str, relative_path: str, file_size: int) -> bool:
        """使用SFTP下载+SMB上传备份文件（稳定可靠）"""
        start_time = time.time()
        temp_path = None
        
        try:
            import tempfile
            
            # 1. 使用SFTP从服务器下载到本地临时文件
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_path = temp_file.name
            
            ssh_client = self._get_ssh_client()
            if not ssh_client:
                raise Exception("无法建立SSH连接")
            
            try:
                with ssh_client.open_sftp() as sftp:
                    sftp.get(remote_path, temp_path)
            finally:
                self._return_ssh_client(ssh_client)
            
            # 检查文件是否下载成功
            if not os.path.exists(temp_path):
                raise Exception(f"文件下载失败: {temp_path}")
            
            # 2. 上传到NAS（自动选择NFS或SMB）
            self._upload_to_nas(temp_path, relative_path)
            
            # 计算传输速度
            actual_size = os.path.getsize(temp_path)
            elapsed = time.time() - start_time
            if elapsed > 0:
                speed_mbps = (actual_size / (1024 * 1024)) / elapsed
                if self.progress_tracker:
                    self.progress_tracker.update_speed(speed_mbps)
            
            # 更新状态
            self.state[relative_path] = {'backed_up': True, 'size': actual_size}
            self.result.backed_up.append(relative_path)
            return True
            
        except Exception as e:
            logger.error(f"备份失败 {remote_path}: {e}")
            self.result.failed.append(relative_path)
            self.result.errors[relative_path] = str(e)
            return False
            
        finally:
            # 清理临时文件
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)

    def _run_remote_backup(self):
        """远程备份模式：在服务器上执行rsync直接备份到NAS"""
        print("="*60)
        print("远程备份模式：服务器直接备份到NAS")
        print("="*60)
        print(f"服务器: {self.config.server_host}")
        print(f"源路径: {self.config.source_path}")
        print(f"NAS: {self.config.nas_ip}:{self.config.nfs_export}")
        print(f"目标路径: {self.config.server_nfs_mount}/{self.config.target_path}")
        print()
        
        # 连接服务器
        if not self.connect_server():
            print("❌ 连接服务器失败")
            return False
        
        try:
            # 获取一个SSH客户端
            ssh_client = self._get_ssh_client()
            if not ssh_client:
                print("❌ 无法获取SSH连接")
                return False
            
            try:
                # 1. 检查/创建挂载点
                print("检查NFS挂载...")
                mount_point = self.config.server_nfs_mount
                
                # 检查是否已挂载
                stdin, stdout, stderr = ssh_client.exec_command(f"mountpoint -q {mount_point} && echo 'mounted' || echo 'not mounted'")
                result = stdout.read().decode().strip()
                
                if result == 'not mounted':
                    print(f"挂载NFS: {self.config.nas_ip}:{self.config.nfs_export} -> {mount_point}")
                    
                    # 检查是否安装了nfs-common
                    stdin, stdout, stderr = ssh_client.exec_command("which mount.nfs || echo 'not found'")
                    nfs_check = stdout.read().decode().strip()
                    if 'not found' in nfs_check:
                        print("安装NFS客户端（可能需要几分钟）...")
                        # 使用channel执行，可以实时看到输出
                        channel = ssh_client.get_transport().open_session()
                        channel.exec_command("sudo apt-get update -q && sudo apt-get install -y nfs-common")
                        while not channel.exit_status_ready():
                            if channel.recv_ready():
                                print(channel.recv(1024).decode(), end='', flush=True)
                            time.sleep(0.5)
                        exit_code = channel.recv_exit_status()
                        if exit_code != 0:
                            print(f"❌ 安装NFS客户端失败")
                            return False
                        print("✅ NFS客户端已安装")
                    
                    # 创建挂载点
                    ssh_client.exec_command(f"sudo mkdir -p {mount_point}")
                    time.sleep(0.5)
                    
                    # 挂载NFS
                    mount_cmd = f"sudo mount -t nfs -o vers=3,nolock {self.config.nas_ip}:{self.config.nfs_export} {mount_point}"
                    stdin, stdout, stderr = ssh_client.exec_command(mount_cmd)
                    exit_status = stdout.channel.recv_exit_status()
                    
                    if exit_status != 0:
                        error = stderr.read().decode()
                        print(f"❌ NFS挂载失败: {error}")
                        return False
                    
                    print("✅ NFS挂载成功")
                else:
                    print("✅ NFS已挂载")
                
                # 2. 确保目标目录存在
                target_dir = f"{mount_point}/{self.config.target_path}"
                ssh_client.exec_command(f"mkdir -p {target_dir}")
                time.sleep(0.3)
                
                # 3. 执行rsync备份
                source = self.config.source_path.rstrip('/') + '/'
                dest = target_dir.rstrip('/') + '/'
                
                print()
                print(f"开始rsync备份:")
                print(f"  源: {source}")
                print(f"  目标: {dest}")
                print()
                
                # rsync命令：带进度显示
                rsync_cmd = f"rsync -avh --progress --stats {source} {dest}"
                print(f"执行: {rsync_cmd}")
                print("-" * 60)
                
                # 使用channel执行，实时获取输出
                channel = ssh_client.get_transport().open_session()
                channel.exec_command(rsync_cmd)
                
                # 实时读取输出
                while True:
                    if channel.recv_ready():
                        output = channel.recv(4096).decode('utf-8', errors='replace')
                        print(output, end='', flush=True)
                    if channel.recv_stderr_ready():
                        error = channel.recv_stderr(4096).decode('utf-8', errors='replace')
                        print(error, end='', flush=True)
                    if channel.exit_status_ready():
                        # 读取剩余输出
                        while channel.recv_ready():
                            output = channel.recv(4096).decode('utf-8', errors='replace')
                            print(output, end='', flush=True)
                        break
                    time.sleep(0.1)
                
                exit_status = channel.recv_exit_status()
                
                print("-" * 60)
                if exit_status == 0:
                    print("✅ 备份完成!")
                    return True
                else:
                    print(f"❌ rsync退出码: {exit_status}")
                    return False
                    
            finally:
                self._return_ssh_client(ssh_client)
                
        finally:
            self._cleanup()

    def _backup_large_file(self, remote_path: str, relative_path: str, file_size: int) -> bool:
        """备份大文件（>1GB）- 使用SFTP下载+SMB上传"""
        return self._backup_file_sftp(remote_path, relative_path, file_size)

    def _backup_small_file(self, remote_path: str, relative_path: str) -> bool:
        """备份小文件 - 使用SFTP下载+SMB上传"""
        # 对于小文件，文件大小已在backup_file中传入，这里使用0作为占位符
        # 实际大小会在_backup_file_sftp中从下载的文件获取
        return self._backup_file_sftp(remote_path, relative_path, 0)

    def run_backup(self):
        """运行备份过程"""
        # 检查是否使用远程备份模式
        if self.config.remote_backup_mode:
            self._run_remote_backup()
            return
            
        print("开始NAS备份...")
        print(f"服务器: {self.config.server_host}:{self.config.source_path}")
        print(f"NAS: {self.config.nas_ip}:{self.config.nas_share}/{self.config.target_path}")

        # 连接
        if not self.connect_server():
            print("连接服务器失败")
            return

        if not self.connect_nas():
            print("连接NAS失败")
            return

        try:
            # 获取文件列表
            files_with_sizes = self.get_server_files()
            if not files_with_sizes:
                print("没有找到需要备份的文件")
                return

            self.result.total_files = len(files_with_sizes)

            # 过滤已备份的文件
            files_to_backup = []
            for remote_path, file_size in files_with_sizes:
                relative_path = os.path.relpath(remote_path, self.config.source_path)
                if self.state.get(relative_path, {}).get('backed_up'):
                    self.result.skipped.append(relative_path)
                else:
                    files_to_backup.append((remote_path, relative_path, file_size))

            print(f"总文件数: {len(files_with_sizes)}")
            print(f"需备份: {len(files_to_backup)}")
            print(f"已跳过: {len(self.result.skipped)}")

            if not files_to_backup:
                print("所有文件已备份完成")
                return

            # 初始化进度跟踪器
            self.progress_tracker = ProgressTracker(len(files_to_backup))

            # 并行备份
            executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
            try:
                futures = [executor.submit(self.backup_file, remote_path, relative_path, file_size)
                          for remote_path, relative_path, file_size in files_to_backup]

                for future in as_completed(futures):
                    if self.shutdown_flag.is_set():
                        break
                    future.result()  # 等待完成

            except KeyboardInterrupt:
                print("\n\n收到中断信号，正在停止备份...")
                self.shutdown_flag.set()
                executor.shutdown(wait=False, cancel_futures=True)
                print("备份已中断")
            else:
                executor.shutdown(wait=True)
            finally:
                # 完成进度显示
                if self.progress_tracker:
                    self.progress_tracker._display_progress()

                # 保存状态
                self._save_state()

                # 输出结果
                self._print_summary()

        except KeyboardInterrupt:
            print("\n\n备份被用户中断")
        finally:
            self._cleanup()

    def _print_summary(self):
        """打印备份汇总"""
        print("\n" + "="*50)
        print("备份完成汇总")
        print("="*50)
        print(f"总文件数: {self.result.total_files}")
        print(f"成功备份: {len(self.result.backed_up)}")
        print(f"已跳过: {len(self.result.skipped)}")
        print(f"失败: {len(self.result.failed)}")

        if self.result.failed:
            print("\n失败的文件:")
            for f in self.result.failed:
                print(f"  - {f}: {self.result.errors.get(f, '未知错误')}")

    def _cleanup(self):
        """清理连接"""
        if self.ssh_client:
            self.ssh_client.close()
        
        # 清理连接池中的所有连接
        with self.pool_lock:
            for client in self.ssh_clients_pool:
                try:
                    client.close()
                except:
                    pass
            self.ssh_clients_pool.clear()

    def _save_state(self):
        """保存备份状态"""
        try:
            with open(self.config.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            logger.info(f"状态已保存到 {self.config.state_file}")
        except Exception as e:
            logger.error(f"保存状态文件失败: {e}")



def test_nas_connection():
    """测试NAS连接"""
    config = BackupConfig(
        server_host="dummy",
        server_user="dummy", 
        use_password=False,
        server_password="dummy",
        server_key_path="dummy"
    )
    
    backup = NASBackup(config)
    success = backup.connect_nas()
    backup._cleanup()
    return success


def main():
    """主函数"""
    # 检查必需配置
    if SERVER_HOST == "your_server_ip":
        print("错误：请在脚本中配置 SERVER_HOST")
        sys.exit(1)
    if SERVER_USER == "your_username":
        print("错误：请在脚本中配置 SERVER_USER")
        sys.exit(1)
    if USE_PASSWORD and SERVER_PASSWORD == "your_password":
        print("错误：请在脚本中配置 SERVER_PASSWORD")
        sys.exit(1)
    if not USE_PASSWORD and SERVER_KEY_PATH == "/path/to/your/ssh/key":
        print("错误：请在脚本中配置 SERVER_KEY_PATH")
        sys.exit(1)

    # 跳过NAS连接测试，直接开始备份（连接测试会在run_backup中进行）
    config = BackupConfig(
        server_host=SERVER_HOST,
        server_user=SERVER_USER,
        use_password=USE_PASSWORD,
        server_password=SERVER_PASSWORD,
        server_key_path=SERVER_KEY_PATH
    )

    backup = NASBackup(config)
    backup.run_backup()


if __name__ == "__main__":
    main()