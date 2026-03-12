#!/usr/bin/env python3
"""
简单测试：只创建基础字段
"""
import sys
sys.path.insert(0, '/home/zgw/projects/annotapipe')

from src.pipeline.tracker import Tracker, TrackingRecord

# 创建最简单的测试记录 - 不设置上传路径
test_record = TrackingRecord(
    name="TEST_SIMPLE_20260113",
    keyframe_count=50,
    annotation_status="已完成",
    uploaded=False,  # 不设置上传状态
    final_dir=None   # 不设置最终目录
)

print("=" * 60)
print("简单测试：只创建基础字段")
print("=" * 60)
print(f"测试记录: {test_record.name}")
print(f"关键帧数: {test_record.keyframe_count}")
print(f"已上传: {test_record.uploaded}")
print(f"最终目录: {test_record.final_dir}")
print()

# 不传json_dir，这样不会检测属性
tracker = Tracker()
print("开始同步到飞书（不设置属性和路径）...")
result = tracker.track([test_record], None)

if result:
    print()
    print("=" * 60)
    print("同步结果:")
    print("=" * 60)
    if isinstance(result.get('created'), list):
        print(f"  新增: {len(result.get('created', []))} 条")
    if isinstance(result.get('updated'), list):
        print(f"  更新: {len(result.get('updated', []))} 条")
    print()
    print("✅ 基础字段创建成功，现在测试更新...")
    
    # 现在测试更新 - 添加上传路径
    test_record.uploaded = True
    test_record.final_dir = "/data02/dataset/lines"
    
    print()
    print("=" * 60)
    print("测试更新：添加上传路径")
    print("=" * 60)
    print(f"已上传: {test_record.uploaded}")
    print(f"最终目录: {test_record.final_dir}")
    print()
    
    json_dir = "/media/zgw/T7/1.6线段导出/常规数据/"
    result2 = tracker.track([test_record], json_dir)
    
    if result2:
        print()
        print("=" * 60)
        print("更新结果:")
        print("=" * 60)
        if isinstance(result2.get('updated'), list):
            print(f"  更新: {len(result2.get('updated', []))} 条")
        print()
        print("✅ 请检查飞书表格验证:")
        print("   1. 线段属性 应该为 ✓")
        print("   2. 上传data02/dataset/lines 应该为 ✓")
else:
    print("❌ 同步失败")
