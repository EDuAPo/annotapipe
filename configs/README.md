# 配置文件说明

本目录包含 annotation_checker 项目的所有配置文件。

## 📁 文件结构

```
configs/
├── pipeline.yaml       # 流水线主配置
├── check_rules.yaml    # 标注检查规则
├── feishu.yaml         # 飞书多维表格配置
├── .env.example        # 环境变量模板
└── README.md           # 本文档
```

## 🔧 配置文件说明

### pipeline.yaml
流水线运行的核心配置，包括：
- 服务器连接配置（IP、用户名、目录路径）
- DataWeave API 配置
- 本地目录配置
- 并发和处理选项

### check_rules.yaml
标注质量检查规则，包括：
- 坐标系配置
- 各类别尺寸约束（vehicle、pedestrian、cone、sign）
- 可视化参数
- 批量处理选项

### feishu.yaml
飞书多维表格同步配置，包括：
- 表格 Token 和 ID
- 字段映射
- 属性关键词匹配规则

## 🔐 敏感信息处理

敏感凭证（密码、密钥等）不应直接写入配置文件，请使用环境变量：

1. 复制 `.env.example` 为 `.env`
2. 填入实际的凭证值
3. 确保 `.env` 已添加到 `.gitignore`

支持的环境变量：
| 变量名 | 说明 |
|--------|------|
| `DATAWEAVE_USERNAME` | DataWeave 用户名 |
| `DATAWEAVE_PASSWORD` | DataWeave 密码 |
| `DATAWEAVE_AUTH_TOKEN` | DataWeave Token（可选） |
| `SERVER_PRIMARY_PASSWORD` | 主服务器 SSH 密码 |
| `FEISHU_APP_ID` | 飞书应用 ID |
| `FEISHU_APP_SECRET` | 飞书应用密钥 |

## 🚀 快速开始

```bash
# 1. 复制环境变量模板
cp configs/.env.example configs/.env

# 2. 编辑 .env 填入实际凭证
vim configs/.env

# 3. 根据需要修改 pipeline.yaml 中的路径配置

# 4. 运行流水线
python run_pipeline.py --json_dir ./data --config configs/pipeline.yaml
```

## ⚠️ 注意事项

1. **不要提交敏感信息**：`.env` 文件和包含密码的配置文件不应提交到 Git
2. **路径配置**：确保 `pipeline.yaml` 中的本地路径和服务器路径正确
3. **字段映射**：如果飞书表格结构变化，需要更新 `feishu.yaml` 中的 `field_mapping`
