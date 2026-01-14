#!/usr/bin/env python3
"""
整理 ZIP 文件工具
将子目录中的所有 ZIP 文件移动到一个目录中
"""
import os
import shutil
import argparse
from pathlib import Path


def organize_zips(source_dir: str, target_dir: str, copy_mode: bool = False):
    """
    整理 ZIP 文件到目标目录
    
    Args:
        source_dir: 源目录（递归搜索）
        target_dir: 目标目录
        copy_mode: True=复制，False=移动
    """
    source_path = Path(source_dir)
    target_path = Path(target_dir)
    
    if not source_path.exists():
        print(f"❌ 源目录不存在: {source_dir}")
        return
    
    # 创建目标目录
    target_path.mkdir(parents=True, exist_ok=True)
    print(f"📁 源目录: {source_dir}")
    print(f"📂 目标目录: {target_dir}")
    print(f"🔧 模式: {'复制' if copy_mode else '移动'}")
    print()
    
    # 递归查找所有 ZIP 文件
    zip_files = list(source_path.rglob("*.zip"))
    print(f"📦 找到 {len(zip_files)} 个 ZIP 文件")
    print()
    
    if not zip_files:
        print("没有找到 ZIP 文件")
        return
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    for i, zip_file in enumerate(zip_files, 1):
        filename = zip_file.name
        target_file = target_path / filename
        
        # 检查目标文件是否已存在
        if target_file.exists():
            print(f"[{i}/{len(zip_files)}] ⏭ 跳过（已存在）: {filename}")
            skipped_count += 1
            continue
        
        try:
            if copy_mode:
                shutil.copy2(zip_file, target_file)
                action = "复制"
            else:
                shutil.move(str(zip_file), str(target_file))
                action = "移动"
            
            print(f"[{i}/{len(zip_files)}] ✅ {action}成功: {filename}")
            success_count += 1
        except Exception as e:
            print(f"[{i}/{len(zip_files)}] ❌ {action}失败: {filename} - {e}")
            failed_count += 1
    
    # 打印汇总
    print()
    print("=" * 60)
    print("  整理汇总")
    print("=" * 60)
    print(f"  ✅ 成功: {success_count}")
    print(f"  ❌ 失败: {failed_count}")
    print(f"  ⏭ 跳过: {skipped_count}")
    print(f"  📊 总计: {len(zip_files)}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='整理 ZIP 文件到一个目录',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 移动所有 ZIP 文件到目标目录
  python organize_zips.py --source /media/zgw/T71/0107out/ --target /media/zgw/T71/all_zips/
  
  # 复制而不是移动
  python organize_zips.py --source /media/zgw/T71/0107out/ --target /media/zgw/T71/all_zips/ --copy
        """
    )
    
    parser.add_argument(
        '--source', '-s',
        required=True,
        help='源目录（递归搜索所有子目录）'
    )
    parser.add_argument(
        '--target', '-t',
        required=True,
        help='目标目录（所有 ZIP 文件将整理到这里）'
    )
    parser.add_argument(
        '--copy', '-c',
        action='store_true',
        help='复制文件而不是移动（保留原文件）'
    )
    
    args = parser.parse_args()
    
    print()
    print("╔" + "═" * 58 + "╗")
    print("║  📦 ZIP 文件整理工具".ljust(59) + "║")
    print("╚" + "═" * 58 + "╝")
    print()
    
    organize_zips(args.source, args.target, args.copy)


if __name__ == '__main__':
    main()
