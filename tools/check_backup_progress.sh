#!/bin/bash
# 备份进度查看脚本

STATE_FILE="nas_backup_state.json"

echo "=========================================="
echo "NAS备份进度查看"
echo "=========================================="
echo ""

# 检查进程是否运行
if ps aux | grep -v grep | grep -q "nas_backup.py"; then
    echo "✅ 备份进程正在运行"
    PID=$(ps aux | grep -v grep | grep "nas_backup.py" | awk '{print $2}')
    echo "   进程ID: $PID"
    echo ""
else
    echo "❌ 备份进程未运行"
    echo ""
fi

# 检查状态文件
if [ -f "$STATE_FILE" ]; then
    echo "📊 备份统计:"
    BACKED_UP=$(grep -c '"backed_up": true' "$STATE_FILE")
    TOTAL_ENTRIES=$(grep -c '"backed_up"' "$STATE_FILE")
    
    echo "   已备份文件: $BACKED_UP"
    echo "   状态文件条目: $TOTAL_ENTRIES"
    
    if [ $TOTAL_ENTRIES -gt 0 ]; then
        PERCENTAGE=$(awk "BEGIN {printf \"%.1f\", ($BACKED_UP/$TOTAL_ENTRIES)*100}")
        echo "   完成度: $PERCENTAGE%"
    fi
    
    echo ""
    echo "📁 最近备份的文件:"
    grep -B1 '"backed_up": true' "$STATE_FILE" | grep -v "backed_up" | grep -v "^--$" | tail -5 | sed 's/.*"\(.*\)".*/   - \1/'
else
    echo "⚠️  状态文件不存在: $STATE_FILE"
fi

echo ""
echo "=========================================="
echo "监控命令:"
echo "  实时查看日志: tail -f backup.log"
echo "  查看进程: ps aux | grep nas_backup.py"
echo "  再次检查: bash check_backup_progress.sh"
echo "=========================================="
