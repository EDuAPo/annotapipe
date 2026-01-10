"""
配置管理模块 v2.0
集中管理所有配置项，支持YAML文件加载和环境变量覆盖
"""
import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import yaml


# 配置文件默认路径
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent.parent / "configs" / "pipeline.yaml"


@dataclass
class ServerConfig:
    """服务器配置"""
    ip: str
    user: str
    zip_dir: str
    process_dir: str
    final_dir: str
    name: str = "primary"
    priority: int = 1
    enabled: bool = True
    
    def __post_init__(self):
        # 从环境变量获取密码
        env_key = f"SERVER_{self.name.upper()}_PASSWORD"
        self.password = os.environ.get(env_key, "")


@dataclass
class DataWeaveConfig:
    """DataWeave API 配置"""
    base_url: str = "https://dataweave.enableai.cn/api/v4"
    username: str = ""
    password: str = ""
    token: str = ""
    path_templates: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        # 环境变量优先
        self.username = os.environ.get("DATAWEAVE_USERNAME", self.username)
        self.password = os.environ.get("DATAWEAVE_PASSWORD", self.password)
        self.token = os.environ.get("DATAWEAVE_AUTH_TOKEN", self.token)
        
        if not self.path_templates:
            self.path_templates = [
                "dataweave://my/TO_RERE/盲区数据/{filename}",
                "dataweave://my/TO_RERE/7Lidar_data/{filename}",
            ]
    
    @property
    def api_url(self) -> str:
        return f"{self.base_url}/file/url"
    
    @property
    def login_url(self) -> str:
        return f"{self.base_url}/session/token"


@dataclass
class PipelineConfig:
    """流水线配置"""
    servers: List[ServerConfig] = field(default_factory=list)
    dataweave: DataWeaveConfig = field(default_factory=DataWeaveConfig)
    
    # 本地目录
    local_temp_dir: str = "/tmp/pipeline_downzips/"
    local_check_dir: Optional[str] = None
    
    # 处理选项
    zip_after_process: str = "rename"
    rename_json: bool = True
    check_config_path: str = "configs/check_rules.yaml"
    
    # 并发配置
    max_workers: int = 3
    download_workers: int = 5
    batch_size: int = 20
    
    def __post_init__(self):
        if not self.servers:
            self.servers = [
                ServerConfig(
                    name="primary",
                    ip="222.223.112.212",
                    user="user",
                    zip_dir="/data02/rere_zips",
                    process_dir="/data02/processing",
                    final_dir="/data02/dataset/scenesnew",
                    priority=1
                ),
            ]
    
    def get_available_server(self) -> Optional[ServerConfig]:
        """获取可用的服务器（按优先级）"""
        import paramiko
        
        for server in sorted(self.servers, key=lambda s: s.priority):
            if not server.enabled:
                continue
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(server.ip, username=server.user, timeout=5)
                ssh.close()
                return server
            except Exception:
                continue
        return self.servers[0] if self.servers else None
    
    @classmethod
    def load(cls, config_path: str = None) -> "PipelineConfig":
        """从 YAML 文件加载配置"""
        path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
        
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            return cls._from_dict(data)
        return cls()
    
    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "PipelineConfig":
        """从字典创建配置"""
        # 解析服务器配置
        servers = []
        for s in data.get('servers', []):
            servers.append(ServerConfig(**s))
        
        # 解析 DataWeave 配置
        dw_data = data.get('dataweave', {})
        dataweave = DataWeaveConfig(**dw_data) if dw_data else DataWeaveConfig()
        
        # 解析本地目录配置
        local_cfg = data.get('local', {})
        
        # 解析处理选项
        proc_cfg = data.get('processing', {})
        
        # 解析并发配置
        conc_cfg = data.get('concurrency', {})
        
        return cls(
            servers=servers or None,
            dataweave=dataweave,
            local_temp_dir=local_cfg.get('temp_dir', cls.local_temp_dir),
            local_check_dir=local_cfg.get('check_dir'),
            zip_after_process=proc_cfg.get('zip_after_process', 'rename'),
            rename_json=proc_cfg.get('rename_json', True),
            check_config_path=proc_cfg.get('check_config_path', 'configs/check_rules.yaml'),
            max_workers=conc_cfg.get('max_workers', 3),
            download_workers=conc_cfg.get('download_workers', 5),
            batch_size=conc_cfg.get('batch_size', 20),
        )


# 全局配置实例
_config: Optional[PipelineConfig] = None


def get_config() -> PipelineConfig:
    """获取全局配置"""
    global _config
    if _config is None:
        _config = PipelineConfig.load()
    return _config


def set_config(config: PipelineConfig):
    """设置全局配置"""
    global _config
    _config = config


def load_env_file(env_path: str = None):
    """加载 .env 文件到环境变量"""
    path = Path(env_path) if env_path else Path("configs/.env")
    if not path.exists():
        return
    
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key.strip(), value.strip())
