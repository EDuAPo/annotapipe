# AnnotaPipe

**标注数据自动化处理流水线** - 用于从 DataWeave 平台下载数据、上传到远程服务器、处理和检查标注质量。

> 🎉 **项目状态**: v1.0 已完成，进入收尾阶段

## ✨ 功能特性

- 🚀 多种运行模式：优化模式、并行模式、流式模式
- 📥 自动从 DataWeave 下载标注数据
- 📤 SFTP 批量上传到远程服务器
- 🔍 标注质量自动检查
- 📊 飞书多维表格同步（默认启用）
- 📝 处理日志和进度追踪

## 📁 项目结构

```
annotapipe/
├── run_pipeline.py          # 命令行入口
├── configs/                  # 配置文件
│   ├── pipeline.yaml        # 流水线主配置
│   ├── check_rules.yaml     # 标注检查规则
│   ├── feishu.yaml          # 飞书配置
│   └── .env.example         # 环境变量模板
├── src/
│   ├── pipeline/            # 核心流水线模块
│   │   ├── runner.py        # 流水线运行器
│   │   ├── downloader.py    # 文件下载器
│   │   ├── uploader.py      # 文件上传器
│   │   ├── processor.py     # 远程处理器
│   │   ├── checker.py       # 标注检查器
│   │   ├── tracker.py       # 进度追踪器
│   │   └── ...
│   └── remote_scripts/      # 远程执行脚本
│       ├── zip_worker.py
│       └── annotation_checker.py
├── tools/                   # 辅助工具
│   ├── annotation_stats.py  # 标注统计
│   ├── debug_feishu.py      # 飞书表格调试
│   └── keyframe_counter.py  # 关键帧计数
├── tests/                   # 测试脚本
└── data/                    # 数据目录
```

## 📦 依赖

| 包名 | 说明 |
|------|------|
| `requests` | HTTP 请求（下载文件） |
| `pyyaml` | YAML 配置解析 |
| `paramiko` | SSH/SFTP 客户端 |
| `numpy` | 数值计算（标注检查） |

新环境部署只需：

```bash
pip install -r requirements.txt
```


## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境

```bash
# 复制环境变量模板
cp configs/.env.example configs/.env

# 编辑 .env 填入实际凭证
vim configs/.env
```

### 3. 运行流水线

```bash
# 基本用法
python run_pipeline.py --json_dir ./data

# 指定配置文件
python run_pipeline.py --json_dir ./data --config configs/pipeline.yaml

# 并行模式，4 个工作线程
python run_pipeline.py --json_dir ./data --mode parallel --workers 4

# 流式模式
python run_pipeline.py --json_dir ./data --mode streaming
```

## ⚙️ 运行模式

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| `optimized` | 下载并行 + 服务器操作串行（默认） | 推荐，平衡效率和稳定性 |
| `parallel` | 全并行模式，多线程独立处理 | 大批量数据处理 |
| `streaming` | 流式模式，逐个处理 | 调试或小批量数据 |

## 📋 命令行参数

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| `--json_dir` | `-j` | JSON 文件目录（必需） | - |
| `--zip_dir` | `-z` | 本地 ZIP 缓存目录 | 自动 |
| `--mode` | `-m` | 运行模式 | `optimized` |
| `--workers` | `-w` | 并发数 | `3` |
| `--config` | `-c` | 配置文件路径 | - |

## 🔧 配置说明

详细配置说明请参考 [configs/README.md](configs/README.md)

### 环境变量

| 变量名 | 说明 |
|--------|------|
| `DATAWEAVE_USERNAME` | DataWeave 用户名 |
| `DATAWEAVE_PASSWORD` | DataWeave 密码 |
| `SERVER_PRIMARY_PASSWORD` | 主服务器 SSH 密码 |
| `FEISHU_APP_ID` | 飞书应用 ID |
| `FEISHU_APP_SECRET` | 飞书应用密钥 |

## 📊 处理流程

```
JSON 文件 → 下载 ZIP → 上传服务器 → 解压处理 → 质量检查 → 移动到最终目录
                                                    ↓
                                            记录和追踪（本地/飞书）
```

### 数据完整性保护

#### 下载完整性
- 验证文件大小与 `Content-Length` 一致
- 使用 `zipfile.testzip()` 验证 ZIP 文件 CRC 校验
- 下载失败自动重试 3 次

#### 上传完整性
- 上传到临时文件 `.uploading`，成功后才重命名
- 验证文件大小（本地 vs 远程）
- 验证 MD5 校验和（默认启用）
- 失败时自动清理临时文件
- 流水线启动时自动清理残留的临时文件

#### 异常中断处理
| 场景 | 处理方式 |
|------|----------|
| 上传中断 | 临时文件残留，下次启动时自动清理 |
| 验证失败 | 删除临时文件，返回失败 |
| 服务器已存在 | 跳过上传，继续处理 |

### 飞书追踪

流水线结束后自动同步到飞书多维表格：
- 按"数据包名称"字段匹配，已存在则更新，不存在则新增
- 跳过的数据（服务器已存在）也会被记录
- 记录字段：数据包名称、标注情况、关键帧数、上传状态、更新时间

### 服务器端操作

| 步骤 | 操作 | 说明 |
|------|------|------|
| 1 | 脚本部署 | 上传 `zip_worker.py`、`annotation_checker.py` 到 `/tmp/` |
| 2 | 状态检查 | 扫描已有 ZIP 和已完成目录，跳过重复处理 |
| 3 | ZIP 处理 | 解压 ZIP，用新 JSON 替换 `sample.json` |
| 4 | 质量检查 | 执行标注检查，生成报告到 `{process_dir}/reports/` |
| 5 | 移动数据 | 检查通过后移动到最终目录（已存在则覆盖） |

### 服务器目录结构

```
服务器:
├── {zip_dir}/              # ZIP 存放目录
│   ├── xxx.zip             # 待处理
│   └── processed_xxx.zip   # 已处理（配置为 rename 时）
├── {process_dir}/          # 处理中目录
│   ├── {stem}/             # 解压后的数据
│   └── reports/            # 检查报告
│       └── report_{stem}.txt
└── {final_dir}/            # 最终目录（检查通过后）
    └── {stem}/             # 完成的数据
```

## 📖 更多文档

- [Pipeline 模块详解](src/pipeline/README.md)
- [配置文件说明](configs/README.md)

## License

MIT
