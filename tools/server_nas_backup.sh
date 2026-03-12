#!/bin/bash
# 服务器端NAS备份脚本
# 直接在服务器上运行，将数据备份到NAS
# 使用方法: ssh user@222.223.112.212 'bash -s' < server_nas_backup.sh

# ========== 配置 ==========
NAS_IP="192.168.2.41"
NFS_EXPORT="/volume1/public"
MOUNT_POINT="/mnt/nas_backup"
SOURCE_PATH="/data02/dataset/od_annotations/scenes/20260104_102458-102658"
TARGET_PATH="test"
# ==========================

echo "=== 服务器端NAS备份 ==="
echo "源路径: $SOURCE_PATH"
echo "目标: $NAS_IP:$NFS_EXPORT/$TARGET_PATH"
echo ""

# 检查源路径
if [ ! -d "$SOURCE_PATH" ]; then
    echo "错误: 源路径不存在: $SOURCE_PATH"
    exit 1
fi

# 创建挂载点
sudo mkdir -p "$MOUNT_POINT"

# 检查是否已挂载
if ! mountpoint -q "$MOUNT_POINT"; then
    echo "正在挂载NFS..."
    sudo mount -t nfs -o vers=3,rw "$NAS_IP:$NFS_EXPORT" "$MOUNT_POINT"
    if [ $? -ne 0 ]; then
        echo "错误: NFS挂载失败"
        exit 1
    fi
    echo "NFS挂载成功"
else
    echo "NFS已挂载"
fi

# 创建目标目录
TARGET_FULL="$MOUNT_POINT/$TARGET_PATH"
mkdir -p "$TARGET_FULL"

# 统计文件数
TOTAL_FILES=$(find "$SOURCE_PATH" -type f | wc -l)
echo "共 $TOTAL_FILES 个文件需要备份"
echo ""

# 使用rsync进行同步（带进度显示）
echo "开始同步..."
rsync -av --progress "$SOURCE_PATH/" "$TARGET_FULL/"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ 备份完成!"
    echo "备份位置: $TARGET_FULL"
else
    echo ""
    echo "❌ 备份过程中出现错误"
fi

# 显示磁盘使用情况
echo ""
echo "NAS磁盘使用情况:"
df -h "$MOUNT_POINT"
