#!/usr/bin/env python3
"""
验证飞书表格中的记录
"""
import sys
sys.path.insert(0, '/home/zgw/projects/annotapipe')

from src.pipeline.tracker import FeishuTracker

tracker = FeishuTracker()

if not tracker.is_available:
    print("❌ 飞书追踪器不可用")
    sys.exit(1)

# 搜索测试记录
print("=" * 60)
print("搜索测试记录: TEST_LINE_DATA_20260113")
print("=" * 60)

record = tracker._search_record("TEST_LINE_DATA_20260113")

if record:
    print("✅ 找到记录!")
    print(f"Record ID: {record['record_id']}")
    print()
    print("字段值:")
    fields = record['fields']
    for field_name, field_value in fields.items():
        print(f"  {field_name}: {field_value}")
    print()
    print("=" * 60)
    print("验证结果:")
    print("=" * 60)
    print(f"  线段属性: {'✓' if fields.get('线段属性') else '✗'}")
    print(f"  上传data02/dataset/lines: {'✓' if fields.get('上传data02/dataset/lines') else '✗'}")
    print(f"  上传data02/dataset/scenesnew: {'✓' if fields.get('上传data02/dataset/scenesnew') else '✗'}")
    print(f"  上传nas: {'✓' if fields.get('上传nas') else '✗'}")
else:
    print("❌ 未找到记录")
