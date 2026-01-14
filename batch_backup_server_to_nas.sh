#!/bin/bash
# 直接从服务器备份到NAS（不经过本地）
# 使用方法: bash batch_backup_server_to_nas.sh
# 
# 功能特性:
# - SSH 连接测试
# - 断点续传（跳过已备份的目录）
# - 失败自动重试
# - 详细的进度显示

set -e  # 遇到错误立即退出

# 配置
SERVER="user@222.223.112.212"
SERVER_DIR="/data02/dataset/scenesnew"
NAS_MOUNT="/mnt/nas_backup/from_rere/boxes"
LOG_FILE="backup_$(date +%Y%m%d_%H%M%S).log"
RETRY_COUNT=2
RETRY_DELAY=5

# 备份模式
# - skip: 跳过已存在的目录（快速，但不会更新已有数据）
# - incremental: 增量备份，同步差异（推荐，rsync 会自动跳过相同文件）
# - force: 强制重新备份，清理已存在的目录后重新备份（用于修复损坏的数据）
BACKUP_MODE="${BACKUP_MODE:-incremental}"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "服务器到NAS直接备份工具"
echo "=========================================="
echo "服务器: $SERVER"
echo "源目录: $SERVER_DIR"
echo "NAS目标: $NAS_MOUNT"
echo "备份模式: $BACKUP_MODE"
echo "日志文件: $LOG_FILE"
echo "=========================================="
echo ""

# 显示模式说明
if [ "$BACKUP_MODE" = "skip" ]; then
    echo -e "${YELLOW}模式说明: 跳过已存在的目录（快速，但不会更新已有数据）${NC}"
elif [ "$BACKUP_MODE" = "force" ]; then
    echo -e "${RED}模式说明: 强制重新备份，会删除已存在的目录后重新备份（用于修复损坏数据）${NC}"
else
    echo -e "${GREEN}模式说明: 增量备份，rsync 会自动跳过相同文件，只同步差异${NC}"
fi
echo ""

# 检查NAS密码
if [ -z "$NAS_PASSWORD" ]; then
    echo -e "${RED}✗ 请设置NAS_PASSWORD环境变量${NC}"
    echo "  export NAS_PASSWORD='Nas123456'"
    exit 1
fi

# 测试SSH连接
echo "正在测试SSH连接..."
if ! ssh -o ConnectTimeout=10 -o BatchMode=yes $SERVER "echo 'SSH连接成功'" &>/dev/null; then
    echo -e "${RED}✗ SSH连接失败${NC}"
    echo ""
    echo "可能的原因:"
    echo "1. SSH密钥未配置 - 请运行: ssh-copy-id $SERVER"
    echo "2. 服务器地址或用户名错误"
    echo "3. 网络连接问题"
    echo ""
    echo "提示: 请确保可以无密码SSH登录服务器"
    echo "      ssh $SERVER"
    exit 1
fi
echo -e "${GREEN}✓ SSH连接正常${NC}"
echo ""

# 检查NAS是否已挂载
if ! mountpoint -q /mnt/nas_backup 2>/dev/null; then
    echo "正在挂载NAS..."
    sudo mount -t cifs //192.168.2.41/public /mnt/nas_backup \
        -o username=SYSC,password=${NAS_PASSWORD},vers=3.0,uid=1000,gid=1000,file_mode=0755,dir_mode=0755
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ NAS挂载失败${NC}"
        echo "  请检查NAS密码是否正确"
        exit 1
    fi
    echo -e "${GREEN}✓ NAS挂载成功${NC}"
else
    echo -e "${GREEN}✓ NAS已挂载${NC}"
fi

# 确保目标目录存在
if [ ! -d "$NAS_MOUNT" ]; then
    echo "正在创建目标目录..."
    mkdir -p "$NAS_MOUNT"
fi

# 获取服务器上的目录列表
echo ""
echo "正在获取服务器上的目录列表..."
DIRS=$(ssh $SERVER "ls -1 $SERVER_DIR" 2>/dev/null)

if [ -z "$DIRS" ]; then
    echo -e "${RED}✗ 无法获取目录列表${NC}"
    exit 1
fi

TOTAL=$(echo "$DIRS" | wc -l)
echo "找到 $TOTAL 个目录"

# 检查已备份的目录
EXISTING=0
for DIR in $DIRS; do
    if [ -d "$NAS_MOUNT/$DIR" ]; then
        EXISTING=$((EXISTING + 1))
    fi
done

if [ $EXISTING -gt 0 ]; then
    echo -e "${YELLOW}已备份: $EXISTING 个目录${NC}"
    echo "将跳过已存在的目录（增量备份）"
fi

NEED_BACKUP=$((TOTAL - EXISTING))
echo "需要备份: $NEED_BACKUP 个目录"
echo ""

# 询问确认
if [ $NEED_BACKUP -eq 0 ]; then
    echo -e "${GREEN}所有目录已备份完成！${NC}"
    exit 0
fi

read -p "是否继续备份? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "已取消"
    exit 0
fi

echo ""
echo "开始备份..."
echo "提示: 可以随时按 Ctrl+C 中断，下次运行会自动续传"
echo ""

# 初始化日志
echo "备份开始时间: $(date)" > "$LOG_FILE"
echo "服务器: $SERVER" >> "$LOG_FILE"
echo "源目录: $SERVER_DIR" >> "$LOG_FILE"
echo "目标目录: $NAS_MOUNT" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# 计数器
SUCCESS=0
FAILED=0
SKIPPED=0
CURRENT=0

# 捕获 Ctrl+C
trap 'echo ""; echo ""; echo "备份已中断，进度已保存"; echo "下次运行将自动续传"; exit 130' INT

# 逐个备份
for DIR in $DIRS; do
    CURRENT=$((CURRENT + 1))
    
    # 根据备份模式决定是否跳过
    if [ "$BACKUP_MODE" = "skip" ] && [ -d "$NAS_MOUNT/$DIR" ]; then
        echo "[$CURRENT/$TOTAL] ${YELLOW}跳过${NC}: $DIR (已存在)"
        SKIPPED=$((SKIPPED + 1))
        echo "[$CURRENT/$TOTAL] 跳过: $DIR (已存在)" >> "$LOG_FILE"
        continue
    fi
    
    # 强制模式：删除已存在的目录后重新备份
    if [ "$BACKUP_MODE" = "force" ] && [ -d "$NAS_MOUNT/$DIR" ]; then
        echo "[$CURRENT/$TOTAL] ${RED}强制重新备份${NC}: $DIR (删除旧数据)"
        rm -rf "$NAS_MOUNT/$DIR"
        echo "  已删除旧数据"
    elif [ -d "$NAS_MOUNT/$DIR" ]; then
        # 增量备份模式：即使目录存在也会同步差异
        echo "[$CURRENT/$TOTAL] ${GREEN}增量备份${NC}: $DIR (同步差异)"
    else
        echo "[$CURRENT/$TOTAL] 备份: $DIR"
    fi
    
    # 重试机制
    ATTEMPT=0
    RSYNC_SUCCESS=false
    
    while [ $ATTEMPT -lt $RETRY_COUNT ]; do
        ATTEMPT=$((ATTEMPT + 1))
        
        if [ $ATTEMPT -gt 1 ]; then
            echo "  重试 $ATTEMPT/$RETRY_COUNT..."
            sleep $RETRY_DELAY
        fi
        
        # 使用rsync直接从服务器到NAS
        rsync -avz --progress --partial --partial-dir=.rsync-partial \
            --timeout=300 \
            "$SERVER:$SERVER_DIR/$DIR/" \
            "$NAS_MOUNT/$DIR/" 2>&1 | tee -a "$LOG_FILE" | grep -E "(sent|received|total size|speedup)" || true
        
        RSYNC_EXIT=${PIPESTATUS[0]}
        if [ $RSYNC_EXIT -eq 0 ]; then
            RSYNC_SUCCESS=true
            break
        fi
    done
    
    if [ "$RSYNC_SUCCESS" = true ]; then
        SUCCESS=$((SUCCESS + 1))
        echo -e "  ${GREEN}✓ 成功${NC}"
        echo "[$CURRENT/$TOTAL] 成功: $DIR" >> "$LOG_FILE"
    else
        FAILED=$((FAILED + 1))
        echo -e "  ${RED}✗ 失败${NC} (退出码: $RSYNC_EXIT, 已重试 $RETRY_COUNT 次)"
        echo "[$CURRENT/$TOTAL] 失败: $DIR (退出码: $RSYNC_EXIT)" >> "$LOG_FILE"
    fi
    echo ""
done

# 汇总
echo "=========================================="
echo "备份完成"
echo "=========================================="
echo "总计: $TOTAL"
echo -e "${GREEN}成功: $SUCCESS${NC}"
echo -e "${YELLOW}跳过: $SKIPPED${NC}"
echo -e "${RED}失败: $FAILED${NC}"
echo "=========================================="
echo "日志文件: $LOG_FILE"

# 写入日志
echo "" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "备份结束时间: $(date)" >> "$LOG_FILE"
echo "总计: $TOTAL" >> "$LOG_FILE"
echo "成功: $SUCCESS" >> "$LOG_FILE"
echo "跳过: $SKIPPED" >> "$LOG_FILE"
echo "失败: $FAILED" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# 如果有失败，退出码为1
if [ $FAILED -gt 0 ]; then
    exit 1
fi
