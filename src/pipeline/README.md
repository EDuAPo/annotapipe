# Pipeline 模块

标注数据处理流水线的核心模块，采用模块化架构设计，负责从 DataWeave 平台下载数据、上传到远程服务器、处理和检查标注质量。

> 🎉 **模块状态**: v1.0 已完成，功能稳定

## 📁 模块结构

```
src/pipeline/
├── __init__.py          # 模块导出
├── config.py            # 配置管理
├── ssh_client.py        # SSH/SFTP 客户端
├── downloader.py        # 文件下载器
├── uploader.py          # 文件上传器
├── processor.py         # 远程处理器
├── checker.py           # 标注检查器
├── tracker.py           # 进度追踪器
├── server_logger.py     # 服务器日志
├── runner.py            # 流水线运行器
└── README.md            # 本文档

src/remote_scripts/      # 远程执行脚本（单一数据源）
├── zip_worker.py        # ZIP 解压处理脚本
└── annotation_checker.py # 标注质量检查脚本
```

## 🔧 模块说明

### config.py - 配置管理
```python
from pipeline.config import get_config, PipelineConfig

config = get_config()
server = config.get_available_server()
```

核心类：
- `ServerConfig`: 服务器配置（host, port, user, 目录路径等）
- `DataWeaveConfig`: DataWeave API 配置
- `PipelineConfig`: 流水线总配置

### ssh_client.py - SSH 客户端
```python
from pipeline.ssh_client import SSHClient, create_ssh_client

with create_ssh_client() as ssh:
    status, out, err = ssh.exec_command("ls -la")
    ssh.upload_file("local.zip", "/remote/path.zip")
```

功能：
- SSH 命令执行
- SFTP 文件上传/下载
- 远程文件/目录操作
- 自动重连机制

### downloader.py - 文件下载器
```python
from pipeline.downloader import Downloader

downloader = Downloader()
success = downloader.download_file("filename.zip", target_path)
```

功能：
- DataWeave API Token 管理（自动刷新）
- 多路径模板查找
- 断点续传支持
- 批量下载

### uploader.py - 文件上传器
```python
from pipeline.uploader import Uploader

uploader = Uploader(ssh)
success, msg = uploader.upload_file(local_path)
```

功能：
- SFTP 批量上传
- 进度回调
- 重复文件检测
- 不完整文件清理

### processor.py - 远程处理器
```python
from pipeline.processor import RemoteProcessor

processor = RemoteProcessor(ssh)
processor.deploy_scripts()  # 部署远程脚本
success, err = processor.process_zip(zip_path, json_path, stem)
```

功能：
- 远程脚本部署（从 `remote_scripts/` 目录动态加载）
- ZIP 解压和目录结构调整
- 标注质量检查
- 数据移动到最终目录

### checker.py - 标注检查器
```python
from pipeline.checker import AnnotationChecker

checker = AnnotationChecker(ssh)
passed, issue_count, report = checker.check(data_dir, stem)
```

功能：
- 标注质量规则检查
- 检查报告生成
- 关键帧数量统计
- 批量检查

### tracker.py - 进度追踪器
```python
from pipeline.tracker import Tracker, create_tracking_records

tracker = Tracker()
records = create_tracking_records(result, keyframe_counts)
tracker.track(records, json_dir)
```

功能：
- 本地报告生成
- 飞书表格同步（可选）
- 属性自动检测

### server_logger.py - 服务器日志
```python
from pipeline.server_logger import ServerLogger

logger = ServerLogger(ssh)
logger.log_success(data_name, keyframe_count)
logger.print_summary()
```

功能：
- 处理记录持久化
- 日志查询和统计
- 日志轮转

### runner.py - 流水线运行器
```python
from pipeline.runner import PipelineRunner

runner = PipelineRunner(json_dir="/path/to/jsons")
runner.run(mode="optimized", workers=3)
```

运行模式：
- `optimized`: 优化模式（默认），智能调度
- `streaming`: 流式模式，逐个处理
- `parallel`: 并行模式，多线程处理

## 🚀 快速开始

### 基本使用
```python
from pipeline import PipelineRunner

# 创建运行器
runner = PipelineRunner(
    json_dir="data/",           # JSON 文件目录
    local_zip_dir="/tmp/zips"   # 本地 ZIP 缓存目录
)

# 运行流水线
runner.run(mode="optimized", workers=3)
```

### 命令行使用
```bash
python run_pipeline.py --json_dir data/ --mode parallel --workers 4
```

## 📐 架构设计

### 单一数据源原则

远程脚本采用单一数据源设计：

```
src/remote_scripts/           # 唯一的脚本源
├── zip_worker.py            # ZIP 处理脚本
└── annotation_checker.py    # 检查脚本

src/pipeline/processor.py    # 动态加载脚本
└── _load_script(name)       # 从 remote_scripts/ 读取
```

`processor.py` 通过 `_load_script()` 函数动态读取脚本内容：

```python
LOCAL_SCRIPTS_DIR = Path(__file__).parent.parent / "remote_scripts"

def _load_script(name: str) -> str:
    """从 remote_scripts 目录加载脚本内容"""
    script_path = LOCAL_SCRIPTS_DIR / name
    return script_path.read_text(encoding='utf-8')
```

好处：
- 消除代码重复
- 脚本可独立测试
- 维护更简单

### 模块依赖关系

```
runner.py
    ├── config.py
    ├── ssh_client.py
    ├── downloader.py
    ├── uploader.py
    ├── processor.py
    │   └── remote_scripts/*.py (动态加载)
    ├── checker.py
    ├── tracker.py
    └── server_logger.py
```

### 处理流程

```
1. 下载 ZIP (downloader)
       ↓
2. 上传到服务器 (uploader)
       ↓
3. 解压并处理 (processor)
       ↓
4. 质量检查 (checker)
       ↓
5. 移动到最终目录 (processor)
       ↓
6. 记录和追踪 (tracker, server_logger)
```

## ⚙️ 配置文件

### configs/pipeline.yaml
```yaml
# 服务器配置
servers:
  - host: "192.168.1.100"
    port: 22
    user: "admin"
    password: "xxx"
    zip_dir: "/data02/rere_zips"
    process_dir: "/data02/processing"
    final_dir: "/data02/"

# DataWeave 配置
dataweave:
  base_url: "https://api.dataweave.com"
  username: "user"
  password: "pass"

# 流水线配置
pipeline:
  rename_json: true
  zip_after_process: "rename"  # rename/delete/keep
  check_config_path: "configs/check_rules.yaml"
```

## 🔍 错误处理

每个模块都有完善的错误处理：

```python
try:
    success, err = processor.process_zip(zip_path, json_path, stem)
    if not success:
        logger.log_failure(stem, err)
except Exception as e:
    logger.error(f"处理失败: {e}")
```

## 📊 日志输出

```
[10:30:15] ✅ 下载完成: 1209_134548_134748.zip
[10:30:20] ✅ 上传完成: 1209_134548_134748.zip
[10:30:45] ✅ 处理完成: 1209_134548_134748
[10:30:50] ✅ 检查通过: 1209_134548_134748 (200 帧)
[10:30:51] ✅ 移动完成: /data02/1209_134548_134748

处理完成! 成功: 95/100, 失败: 5
```

## 🧪 测试

```bash
# 测试单个模块
python -c "from pipeline.ssh_client import SSHClient; print('OK')"

# 测试远程脚本加载
python -c "from pipeline.processor import _load_script; print(_load_script('zip_worker.py')[:100])"
```

## 📝 开发指南

### 添加新的远程脚本

1. 在 `src/remote_scripts/` 创建脚本文件
2. 在 `processor.py` 中使用 `_load_script()` 加载
3. 添加部署和执行逻辑

### 添加新的处理步骤

1. 创建新模块（如 `validator.py`）
2. 在 `runner.py` 中集成
3. 更新配置和文档
