#!/bin/bash
# 批量备份 /data02/dataset/scenesnew 中的所有数据到NAS
# 使用方法: bash batch_backup_scenesnew.sh

SERVER="user@222.223.112.212"
SOURCE_DIR="/data02/dataset/scenesnew"
FINAL_DIR="/data02/dataset/scenesnew"

echo "=========================================="
echo "批量备份工具"
echo "=========================================="
echo "服务器: $SERVER"
echo "源目录: $SOURCE_DIR"
echo "目标: NAS from_rere/boxes"
echo "=========================================="
echo ""

# 获取服务器上的所有子目录
echo "正在获取服务器上的目录列表..."
DIRS=$(ssh $SERVER "ls -1 $SOURCE_DIR" 2>/dev/null)

if [ -z "$DIRS" ]; then
    echo "错误: 无法获取目录列表或目录为空"
    exit 1
fi

# 统计总数
TOTAL=$(echo "$DIRS" | wc -l)
echo "找到 $TOTAL 个目录需要备份"
echo ""

# 询问确认
read -p "是否继续备份? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "已取消"
    exit 0
fi

echo ""
echo "开始备份..."
echo ""

# 计数器
SUCCESS=0
FAILED=0
CURRENT=0

# 逐个备份
for DIR in $DIRS; do
    CURRENT=$((CURRENT + 1))
    echo "[$CURRENT/$TOTAL] 备份: $DIR"
    
    # 执行备份
    python3 tools/backup_to_nas.py \
        --source "$SOURCE_DIR/$DIR" \
        --final-dir "$FINAL_DIR" \
        --name "$DIR" 2>&1 | grep -E "(✓|✗|备份)"
    
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        SUCCESS=$((SUCCESS + 1))
        echo "  ✓ 成功"
    else
        FAILED=$((FAILED + 1))
        echo "  ✗ 失败"
    fi
    echo ""
done

echo "=========================================="
echo "备份完成"
echo "=========================================="
echo "总计: $TOTAL"
echo "成功: $SUCCESS"
echo "失败: $FAILED"
echo "=========================================="
