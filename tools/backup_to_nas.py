#!/usr/bin/env python3
"""
独立的NAS备份工具
可以单独运行，不依赖流水线
支持大文件传输和完整性检查
"""
import os
import sys
import argparse
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

# 手动加载环境变量
def load_env_file(env_path):
    """手动加载.env文件"""
    if Path(env_path).exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

load_env_file('configs/.env')

from src.pipeline.nas_backup import NASBackup


def backup_directory(source_dir: str, final_dir: str, data_name: str = None):
    """
    备份指定目录到NAS
    
    Args:
        source_dir: 源数据目录（完整路径）
        final_dir: final_dir路径（用于确定NAS目标路径）
        data_name: 数据包名称（如果不提供，使用源目录名称）
    """
    source_path = Path(source_dir)
    
    if not source_path.exists():
        print(f"✗ 源目录不存在: {source_dir}")
        return False
    
    if not data_name:
        data_name = source_path.name
    
    print(f"\n{'='*60}")
    print(f"  NAS备份工具")
    print(f"{'='*60}")
    print(f"  源目录: {source_dir}")
    print(f"  final_dir: {final_dir}")
    print(f"  数据名称: {data_name}")
    print(f"{'='*60}\n")
    
    # 使用上下文管理器自动管理NAS挂载/卸载
    with NASBackup() as nas:
        if not nas.is_enabled:
            print("✗ NAS备份未启用")
            print("  请在 configs/nas_backup.yaml 中设置 enabled: true")
            return False
        
        print(f"✓ NAS配置已加载")
        print(f"  - NAS主机: {nas.config.get('nas', {}).get('host')}")
        print(f"  - 挂载点: {nas.mount_point}")
        
        # 获取目标路径
        target_path = nas.get_target_path(final_dir)
        if not target_path:
            print(f"\n✗ 未找到路径映射: {final_dir}")
            print(f"  请在 configs/nas_backup.yaml 中配置 path_mappings")
            return False
        
        print(f"  - 目标路径: {target_path / data_name}")
        print()
        
        # 执行备份
        print(f"开始备份...")
        success, msg = nas.backup_data(
            source_dir=source_dir,
            final_dir=final_dir,
            data_name=data_name
        )
        
        if success:
            print(f"\n✓ 备份成功!")
            print(f"  {msg}")
            
            # 验证备份
            backup_dir = target_path / data_name
            if backup_dir.exists():
                # 统计文件数量和大小
                file_count = sum(1 for _ in backup_dir.rglob('*') if _.is_file())
                total_size = sum(f.stat().st_size for f in backup_dir.rglob('*') if f.is_file())
                size_gb = total_size / (1024**3)
                
                print(f"\n备份统计:")
                print(f"  - 文件数量: {file_count}")
                print(f"  - 总大小: {size_gb:.2f} GB")
                print(f"  - 备份位置: {backup_dir}")
            
            return True
        else:
            print(f"\n✗ 备份失败!")
            print(f"  {msg}")
            return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='独立的NAS备份工具 - 将数据备份到群晖NAS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 备份单个目录
  python tools/backup_to_nas.py \\
    --source /data02/dataset/scenesnew/1209_134548_134748 \\
    --final-dir /data02/dataset/scenesnew

  # 指定数据名称
  python tools/backup_to_nas.py \\
    --source /data02/dataset/scenesnew/1209_134548_134748 \\
    --final-dir /data02/dataset/scenesnew \\
    --name my_backup

  # 批量备份（使用通配符）
  for dir in /data02/dataset/scenesnew/*/; do
    python tools/backup_to_nas.py --source "$dir" --final-dir /data02/dataset/scenesnew
  done

特性:
  - 使用rsync增量备份，高效可靠
  - 支持大文件传输（30GB+）
  - 自动校验文件完整性
  - 失败自动重试
  - 断点续传支持
  - 自动挂载/卸载NAS
        """
    )
    
    parser.add_argument(
        '--source', '-s',
        required=True,
        help='源数据目录（完整路径）'
    )
    
    parser.add_argument(
        '--final-dir', '-f',
        required=True,
        help='final_dir路径（用于确定NAS目标路径）'
    )
    
    parser.add_argument(
        '--name', '-n',
        help='数据包名称（默认使用源目录名称）'
    )
    
    args = parser.parse_args()
    
    try:
        success = backup_directory(
            source_dir=args.source,
            final_dir=args.final_dir,
            data_name=args.name
        )
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\n✗ 用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ 发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
