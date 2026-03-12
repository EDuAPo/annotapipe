#!/usr/bin/env python3
"""
测试上传路径更新修复
验证飞书表格的上传路径列能否正确更新
"""
import sys
sys.path.insert(0, '/home/zgw/projects/annotapipe')

from src.pipeline.tracker import Tracker, TrackingRecord

# 创建测试记录
test_record = TrackingRecord(
    name="TEST_LINE_DATA_20260113",
    keyframe_count=100,
    annotation_status="已完成",
    uploaded=True,
    final_dir="/data02/dataset/lines"  # 线段数据应该上传到 lines 目录
)

print("=" * 60)
print("测试上传路径更新修复")
print("=" * 60)
print(f"测试记录: {test_record.name}")
print(f"关键帧数: {test_record.keyframe_count}")
print(f"最终目录: {test_record.final_dir}")
print(f"已上传: {test_record.uploaded}")
print()

# 模拟线段数据路径
json_dir = "/media/zgw/T7/1.6线段导出/常规数据/"
print(f"JSON目录: {json_dir}")
print()

# 创建追踪器并同步
tracker = Tracker()
print("开始同步到飞书...")
result = tracker.track([test_record], json_dir)

if result:
    print()
    print("=" * 60)
    print("同步结果:")
    print("=" * 60)
    if isinstance(result.get('created'), list):
        print(f"  新增: {len(result.get('created', []))} 条")
        for name in result.get('created', []):
            print(f"    - {name}")
    if isinstance(result.get('updated'), list):
        print(f"  更新: {len(result.get('updated', []))} 条")
        for name in result.get('updated', []):
            print(f"    - {name}")
    print()
    print("✅ 请检查飞书表格中的记录:")
    print("   1. 线段属性 应该为 ✓")
    print("   2. 上传data02/dataset/lines 应该为 ✓")
    print("   3. 其他上传路径 应该为空或 ✗")
else:
    print("❌ 同步失败")
