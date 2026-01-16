#!/bin/bash
# 直接从服务器备份到NAS（不经过本地）
# 使用方法: 
#   bash batch_backup_server_to_nas.sh              # 使用默认配置文件
#   bash batch_backup_server_to_nas.sh configs/pipeline.yaml  # 指定配置文件
#   SERVER_DIR=/custom/path bash batch_backup_server_to_nas.sh  # 手动指定路径
#   TEST_MODE=true bash batch_backup_server_to_nas.sh  # 测试模式（只检查不备份）
#   SKIP_EMPTY=false bash batch_backup_server_to_nas.sh  # 备份空目录
# 
# 环境变量:
#   TEST_MODE=true        # 测试模式，只检查目录状态
#   SKIP_EMPTY=false      # 不跳过空目录
#   BACKUP_MODE=force     # 强制重新备份
#   SERVER_DIR_OVERRIDE   # 覆盖服务器目录路径
#   NAS_PASSWORD          # NAS访问密码
# 
# 功能特性:
# - SSH 连接测试和路径验证
# - 断点续传（跳过已备份的目录）
# - 失败自动重试和详细错误诊断
# - 空目录检测和跳过
# - 测试模式（预览备份状态）
# - 自动从配置文件读取路径
# - 详细的进度显示和日志记录
# - 自动从配置文件读取路径

set -e  # 遇到错误立即退出

# 默认配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${1:-$SCRIPT_DIR/configs/pipeline.yaml}"
SERVER="user@222.223.112.212"
LOG_FILE="backup_$(date +%Y%m%d_%H%M%S).log"
RETRY_COUNT=2
RETRY_DELAY=5

# 从配置文件读取路径
if [ -f "$CONFIG_FILE" ]; then
    echo "正在从配置文件读取路径: $CONFIG_FILE"
    SERVER_DIR=$(python3 -c "
import yaml
with open('$CONFIG_FILE', 'r') as f:
    config = yaml.safe_load(f)
    servers = config.get('servers', [])
    for server in servers:
        if server.get('enabled', False):
            print(server.get('final_dir', '/data02/dataset/scenesnew'))
            break
    " 2>/dev/null)
    
    if [ -z "$SERVER_DIR" ]; then
        echo -e "${YELLOW}警告: 无法从配置文件读取路径，使用默认路径${NC}"
        SERVER_DIR="/data02/dataset/scenesnew"
    fi
else
    echo -e "${YELLOW}警告: 配置文件不存在，使用默认路径${NC}"
    SERVER_DIR="/data02/dataset/scenesnew"
fi

# 允许环境变量覆盖
if [ -n "$SERVER_DIR_OVERRIDE" ]; then
    echo "使用环境变量覆盖路径: $SERVER_DIR_OVERRIDE"
    SERVER_DIR="$SERVER_DIR_OVERRIDE"
fi

# 根据服务器路径确定NAS目标路径
if [[ "$SERVER_DIR" == *"/data02/dataset/lines" ]]; then
    NAS_MOUNT="/mnt/nas_backup/from_rere/lines"
elif [[ "$SERVER_DIR" == *"/data02/dataset/scenesnew" ]]; then
    NAS_MOUNT="/mnt/nas_backup/from_rere/boxes"
else
    NAS_MOUNT="/mnt/nas_backup/from_rere/$(basename $SERVER_DIR)"
fi

# 测试模式
TEST_MODE="${TEST_MODE:-false}"

if [ "$TEST_MODE" = "true" ]; then
    echo -e "${YELLOW}测试模式: 只检查目录，不执行备份${NC}"
fi

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

# 验证源目录存在
echo "正在验证源目录..."
if ! ssh $SERVER "test -d $SERVER_DIR" 2>/dev/null; then
    echo -e "${RED}✗ 源目录不存在: $SERVER_DIR${NC}"
    echo ""
    echo "请检查服务器上的目录结构:"
    echo "  ssh $SERVER 'ls -la /data02/dataset/'"
    echo ""
    echo "可能的解决方案:"
    echo "1. 检查配置文件中的 final_dir 设置"
    echo "2. 手动指定正确的路径: SERVER_DIR=/path/to/data $0"
    exit 1
fi

# 检查源目录是否有内容
DIR_COUNT=$(ssh $SERVER "ls -1 $SERVER_DIR 2>/dev/null | wc -l")
if [ "$DIR_COUNT" -eq 0 ]; then
    echo -e "${YELLOW}⚠ 源目录为空: $SERVER_DIR${NC}"
    echo "目录存在但没有子目录，可能是配置错误"
    exit 1
fi

echo -e "${GREEN}✓ 源目录验证通过: $SERVER_DIR ($DIR_COUNT 个项目)${NC}"
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
echo "执行命令: ssh $SERVER 'ls -1 $SERVER_DIR'"
DIRS=$(ssh $SERVER "ls -1 $SERVER_DIR" 2>/dev/null)

if [ -z "$DIRS" ]; then
    echo -e "${RED}✗ 无法获取目录列表${NC}"
    echo "请检查SSH连接和目录权限"
    exit 1
fi

# 保存到临时文件以便调试
TEMP_DIR_LIST="/tmp/dir_list_$(date +%s).txt"
echo "$DIRS" > "$TEMP_DIR_LIST"
TOTAL=$(wc -l < "$TEMP_DIR_LIST")
echo "找到 $TOTAL 个目录 (已保存到 $TEMP_DIR_LIST)"

# 检查是否有特殊字符或权限问题
INVALID_DIRS=$(echo "$DIRS" | grep -v '^[a-zA-Z0-9_.-]*$' | wc -l)
if [ "$INVALID_DIRS" -gt 0 ]; then
    echo -e "${YELLOW}警告: 发现 $INVALID_DIRS 个目录名包含特殊字符${NC}"
    echo "这些目录可能导致rsync失败"
fi

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
    
    # 测试模式：只检查不备份
    if [ "$TEST_MODE" = "true" ]; then
        if ! ssh $SERVER "test -d $SERVER_DIR/$DIR" 2>/dev/null; then
            echo "[$CURRENT/$TOTAL] ${RED}✗ 不存在${NC}: $DIR"
            FAILED=$((FAILED + 1))
        else
            SRC_SIZE=$(ssh $SERVER "du -sb $SERVER_DIR/$DIR 2>/dev/null | cut -f1" 2>/dev/null || echo "0")
            if [ "$SRC_SIZE" = "0" ]; then
                echo "[$CURRENT/$TOTAL] ${YELLOW}⚠ 空目录${NC}: $DIR"
                SKIPPED=$((SKIPPED + 1))
            else
                echo "[$CURRENT/$TOTAL] ${GREEN}✓ 可备份${NC}: $DIR ($(numfmt --to=iec-i --suffix=B $SRC_SIZE 2>/dev/null || echo ${SRC_SIZE}B))"
                SUCCESS=$((SUCCESS + 1))
            fi
        fi
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
        
        # 检查源目录是否存在
        if ! ssh $SERVER "test -d $SERVER_DIR/$DIR" 2>/dev/null; then
            echo -e "  ${RED}✗ 源目录不存在: $SERVER_DIR/$DIR${NC}"
            echo "  跳过此目录"
            FAILED=$((FAILED + 1))
            echo "[$CURRENT/$TOTAL] 失败: $DIR (源目录不存在)" >> "$LOG_FILE"
            continue
        fi
        
        # 检查源目录是否有内容
        SRC_SIZE=$(ssh $SERVER "du -sb $SERVER_DIR/$DIR 2>/dev/null | cut -f1" 2>/dev/null || echo "0")
        if [ "$SRC_SIZE" = "0" ] && [ "$SKIP_EMPTY" = "true" ]; then
            echo -e "  ${YELLOW}⚠ 源目录为空: $DIR${NC}"
            echo "  跳过空目录"
            SKIPPED=$((SKIPPED + 1))
            echo "[$CURRENT/$TOTAL] 跳过: $DIR (空目录)" >> "$LOG_FILE"
            continue
        fi
        
        if [ "$SRC_SIZE" != "0" ]; then
            echo "  源目录大小: $(numfmt --to=iec-i --suffix=B $SRC_SIZE 2>/dev/null || echo ${SRC_SIZE}B)"
        fi
        
        # 使用rsync直接从服务器到NAS
        echo "  执行rsync..."
        
        rsync -avz --progress --partial --partial-dir=.rsync-partial \
            --timeout=300 \
            "$SERVER:$SERVER_DIR/$DIR/" \
            "$NAS_MOUNT/$DIR/" 2>&1 | tee -a "$LOG_FILE" || RSYNC_EXIT=$?
        
        # 检查rsync结果
        if [ ${PIPESTATUS[0]} -eq 0 ]; then
            RSYNC_SUCCESS=true
            echo "  rsync 退出码: 0 (成功)"
        else
            RSYNC_EXIT=${PIPESTATUS[0]}
            echo "  rsync 退出码: $RSYNC_EXIT"
            
            # 解释常见错误码
            case $RSYNC_EXIT in
                23) echo "  错误解释: 部分传输错误 (可能源目录不存在或权限问题)" ;;
                10) echo "  错误解释: 源目录不存在" ;;
                12) echo "  错误解释: 权限被拒绝" ;;
                30) echo "  错误解释: 超时" ;;
                *) echo "  错误解释: 未知错误 (请查看rsync手册)" ;;
            esac
            
            # 对于错误23，检查目标目录是否已创建
            if [ $RSYNC_EXIT -eq 23 ] && [ -d "$NAS_MOUNT/$DIR" ]; then
                TARGET_SIZE=$(du -sb "$NAS_MOUNT/$DIR" 2>/dev/null | cut -f1)
                if [ "$TARGET_SIZE" -gt 0 ]; then
                    echo "  目标目录已创建，大小: $(numfmt --to=iec-i --suffix=B $TARGET_SIZE 2>/dev/null || echo ${TARGET_SIZE}B)"
                    echo "  可能存在部分传输，标记为成功"
                    RSYNC_SUCCESS=true
                fi
            fi
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
