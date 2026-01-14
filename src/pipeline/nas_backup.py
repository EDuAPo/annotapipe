"""
NAS备份模块
负责将处理完成的数据备份到群晖NAS
"""
import os
import time
import logging
import subprocess
from pathlib import Path
from typing import Dict, Optional, Tuple
import yaml

logger = logging.getLogger(__name__)


class NASBackup:
    """NAS备份管理器"""
    
    def __init__(self, config_path: str = "configs/nas_backup.yaml"):
        self.config_path = Path(config_path)
        self.config: Dict = {}
        self.mounted = False
        self.mount_point: Optional[Path] = None
        self._load_config()
    
    def _load_config(self):
        """加载配置"""
        try:
            if not self.config_path.exists():
                logger.warning(f"NAS备份配置文件不存在: {self.config_path}")
                return
            
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f) or {}
            
            # 从环境变量获取密码
            nas_password = os.environ.get('NAS_PASSWORD', '')
            if not nas_password:
                logger.warning("NAS_PASSWORD环境变量未设置")
            
            self.config['nas']['password'] = nas_password
            
            if self.is_enabled:
                logger.info("✅ NAS备份模块初始化成功")
        except Exception as e:
            logger.error(f"NAS备份配置加载失败: {e}")
    
    @property
    def is_enabled(self) -> bool:
        """检查NAS备份是否启用"""
        return self.config.get('nas', {}).get('enabled', False)
    
    def mount(self) -> bool:
        """挂载NAS共享目录"""
        if self.mounted:
            logger.debug("NAS已挂载")
            return True
        
        if not self.is_enabled:
            logger.warning("NAS备份未启用")
            return False
        
        nas_config = self.config.get('nas', {})
        mount_config = nas_config.get('mount', {})
        
        host = nas_config.get('host')
        share = nas_config.get('share')
        username = nas_config.get('username')
        password = nas_config.get('password', '')
        
        self.mount_point = Path(mount_config.get('local_mount_point', '/mnt/nas_backup'))
        options = mount_config.get('options', 'vers=3.0')
        
        if not all([host, share, username]):
            logger.error("NAS配置信息不完整")
            return False
        
        # 创建挂载点
        try:
            self.mount_point.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"创建挂载点失败: {e}")
            return False
        
        # 检查是否已挂载
        result = subprocess.run(['mountpoint', '-q', str(self.mount_point)])
        if result.returncode == 0:
            logger.info(f"✓ NAS已挂载: {self.mount_point}")
            self.mounted = True
            return True
        
        # 挂载NAS
        smb_path = f"//{host}/{share}"
        credentials = f"username={username},password={password}"
        mount_cmd = [
            'sudo', 'mount', '-t', 'cifs',
            smb_path,
            str(self.mount_point),
            '-o', f"{credentials},{options}"
        ]
        
        logger.info(f"📁 挂载NAS: {smb_path} -> {self.mount_point}")
        
        try:
            result = subprocess.run(
                mount_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                logger.info(f"✓ NAS挂载成功")
                self.mounted = True
                return True
            else:
                logger.error(f"NAS挂载失败: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"NAS挂载异常: {e}")
            return False
    
    def unmount(self) -> bool:
        """卸载NAS共享目录"""
        if not self.mounted or not self.mount_point:
            return True
        
        logger.info(f"📁 卸载NAS: {self.mount_point}")
        
        try:
            result = subprocess.run(
                ['sudo', 'umount', str(self.mount_point)],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                logger.info("✓ NAS卸载成功")
                self.mounted = False
                return True
            else:
                logger.warning(f"NAS卸载失败: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"NAS卸载异常: {e}")
            return False
    
    def get_target_path(self, final_dir: str) -> Optional[Path]:
        """根据final_dir获取NAS目标路径"""
        if not self.mount_point:
            return None
        
        path_mappings = self.config.get('path_mappings', {})
        
        # 查找匹配的映射
        for source_path, target_subdir in path_mappings.items():
            if final_dir.startswith(source_path):
                return self.mount_point / target_subdir
        
        logger.warning(f"未找到路径映射: {final_dir}")
        return None
    
    def backup_data(self, source_dir: str, final_dir: str, data_name: str) -> Tuple[bool, str]:
        """
        备份数据到NAS
        
        Args:
            source_dir: 源数据目录（完整路径）
            final_dir: final_dir路径（用于确定目标路径）
            data_name: 数据包名称
        
        Returns:
            (success, message)
        """
        if not self.is_enabled:
            return True, "NAS备份未启用"
        
        # 确保已挂载
        if not self.mounted:
            if not self.mount():
                return False, "NAS挂载失败"
        
        # 获取目标路径
        target_base = self.get_target_path(final_dir)
        if not target_base:
            return False, f"未找到路径映射: {final_dir}"
        
        target_dir = target_base / data_name
        
        # 创建目标目录
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            return False, f"创建目标目录失败: {e}"
        
        # 获取备份配置
        backup_config = self.config.get('backup', {})
        rsync_options = backup_config.get('rsync_options', ['-av', '--progress'])
        retry_count = backup_config.get('retry_count', 2)
        retry_delay = backup_config.get('retry_delay', 5)
        
        # 构建rsync命令
        rsync_cmd = ['rsync'] + rsync_options + [
            f"{source_dir}/",  # 源目录（末尾加/表示复制目录内容）
            f"{target_dir}/"   # 目标目录
        ]
        
        logger.info(f"📦 备份数据: {data_name}")
        logger.debug(f"  源: {source_dir}")
        logger.debug(f"  目标: {target_dir}")
        
        # 尝试备份，支持重试
        for attempt in range(retry_count + 1):
            try:
                result = subprocess.run(
                    rsync_cmd,
                    capture_output=True,
                    text=True,
                    timeout=3600  # 1小时超时
                )
                
                if result.returncode == 0:
                    logger.info(f"✓ 备份成功: {data_name}")
                    return True, "备份成功"
                else:
                    error_msg = result.stderr.strip()
                    if attempt < retry_count:
                        logger.warning(f"备份失败 (尝试 {attempt + 1}/{retry_count + 1}): {error_msg}")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"备份失败 ({retry_count + 1}次尝试): {error_msg}")
                        return False, f"备份失败: {error_msg}"
            except subprocess.TimeoutExpired:
                if attempt < retry_count:
                    logger.warning(f"备份超时 (尝试 {attempt + 1}/{retry_count + 1})")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"备份超时 ({retry_count + 1}次尝试)")
                    return False, "备份超时"
            except Exception as e:
                if attempt < retry_count:
                    logger.warning(f"备份异常 (尝试 {attempt + 1}/{retry_count + 1}): {e}")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"备份异常 ({retry_count + 1}次尝试): {e}")
                    return False, f"备份异常: {e}"
        
        return False, "备份失败"
    
    def __enter__(self):
        """上下文管理器入口"""
        if self.is_enabled:
            self.mount()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        if self.is_enabled and self.config.get('nas', {}).get('mount', {}).get('auto_unmount', True):
            self.unmount()
