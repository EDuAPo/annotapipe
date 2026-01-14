#!/usr/bin/env python3
"""
NAS备份功能测试脚本
测试NAS挂载、路径映射和备份功能
"""
import os
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

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


def test_nas_config():
    """测试NAS配置加载"""
    print("=" * 60)
    print("测试1: NAS配置加载")
    print("=" * 60)
    
    nas = NASBackup()
    
    print(f"✓ 配置文件加载成功")
    print(f"  - 启用状态: {nas.is_enabled}")
    print(f"  - NAS主机: {nas.config.get('nas', {}).get('host')}")
    print(f"  - 共享名称: {nas.config.get('nas', {}).get('share')}")
    print(f"  - 用户名: {nas.config.get('nas', {}).get('username')}")
    print(f"  - 挂载点: {nas.config.get('nas', {}).get('mount', {}).get('local_mount_point')}")
    print(f"  - 路径映射: {len(nas.config.get('path_mappings', {}))} 个")
    
    return nas


def test_nas_mount(nas):
    """测试NAS挂载"""
    print("\n" + "=" * 60)
    print("测试2: NAS挂载")
    print("=" * 60)
    
    if not nas.is_enabled:
        print("⚠ NAS备份未启用，跳过挂载测试")
        return False
    
    print("尝试挂载NAS...")
    success = nas.mount()
    
    if success:
        print(f"✓ NAS挂载成功: {nas.mount_point}")
        
        # 检查挂载点是否可访问
        if nas.mount_point and nas.mount_point.exists():
            print(f"✓ 挂载点可访问")
            
            # 尝试列出目录内容
            try:
                items = list(nas.mount_point.iterdir())
                print(f"✓ 挂载点内容: {len(items)} 个项目")
                for item in items[:5]:  # 只显示前5个
                    print(f"  - {item.name}")
                if len(items) > 5:
                    print(f"  ... 还有 {len(items) - 5} 个项目")
            except Exception as e:
                print(f"✗ 无法列出挂载点内容: {e}")
        else:
            print(f"✗ 挂载点不可访问: {nas.mount_point}")
            return False
    else:
        print(f"✗ NAS挂载失败")
        return False
    
    return True


def test_path_mapping(nas):
    """测试路径映射"""
    print("\n" + "=" * 60)
    print("测试3: 路径映射")
    print("=" * 60)
    
    if not nas.is_enabled:
        print("⚠ NAS备份未启用，跳过路径映射测试")
        return
    
    path_mappings = nas.config.get('path_mappings', {})
    print(f"配置的路径映射: {len(path_mappings)} 个")
    
    for final_dir, target_subdir in path_mappings.items():
        print(f"\n  源路径: {final_dir}")
        print(f"  目标路径: {target_subdir}")
        
        target_path = nas.get_target_path(final_dir)
        if target_path:
            print(f"  ✓ 映射成功: {target_path}")
            
            # 检查目标路径是否存在
            if target_path.exists():
                print(f"  ✓ 目标路径存在")
            else:
                print(f"  ⚠ 目标路径不存在（将在备份时创建）")
        else:
            print(f"  ✗ 映射失败")


def test_backup_simulation(nas):
    """测试备份功能（模拟）"""
    print("\n" + "=" * 60)
    print("测试4: 备份功能（模拟）")
    print("=" * 60)
    
    if not nas.is_enabled:
        print("⚠ NAS备份未启用，跳过备份测试")
        return
    
    if not nas.mounted:
        print("⚠ NAS未挂载，跳过备份测试")
        return
    
    # 创建一个临时测试目录
    test_source = Path("/tmp/nas_backup_test")
    test_source.mkdir(exist_ok=True)
    
    # 创建一些测试文件
    (test_source / "test_file.txt").write_text("This is a test file for NAS backup")
    (test_source / "subdir").mkdir(exist_ok=True)
    (test_source / "subdir" / "nested_file.txt").write_text("Nested test file")
    
    print(f"✓ 创建测试数据: {test_source}")
    
    # 使用第一个路径映射进行测试
    path_mappings = nas.config.get('path_mappings', {})
    if not path_mappings:
        print("✗ 没有配置路径映射")
        return
    
    final_dir = list(path_mappings.keys())[0]
    test_data_name = "nas_backup_test"
    
    print(f"\n尝试备份测试数据...")
    print(f"  源目录: {test_source}")
    print(f"  final_dir: {final_dir}")
    print(f"  数据名称: {test_data_name}")
    
    success, msg = nas.backup_data(
        source_dir=str(test_source),
        final_dir=final_dir,
        data_name=test_data_name
    )
    
    if success:
        print(f"✓ 备份成功: {msg}")
        
        # 验证备份结果
        target_path = nas.get_target_path(final_dir)
        if target_path:
            backup_dir = target_path / test_data_name
            if backup_dir.exists():
                print(f"✓ 备份目录存在: {backup_dir}")
                
                # 检查文件是否存在
                if (backup_dir / "test_file.txt").exists():
                    print(f"✓ 测试文件已备份")
                if (backup_dir / "subdir" / "nested_file.txt").exists():
                    print(f"✓ 嵌套文件已备份")
            else:
                print(f"✗ 备份目录不存在: {backup_dir}")
    else:
        print(f"✗ 备份失败: {msg}")
    
    # 清理测试数据
    import shutil
    shutil.rmtree(test_source, ignore_errors=True)
    print(f"\n✓ 清理测试数据")


def test_nas_unmount(nas):
    """测试NAS卸载"""
    print("\n" + "=" * 60)
    print("测试5: NAS卸载")
    print("=" * 60)
    
    if not nas.is_enabled:
        print("⚠ NAS备份未启用，跳过卸载测试")
        return
    
    if not nas.mounted:
        print("⚠ NAS未挂载，跳过卸载测试")
        return
    
    print("尝试卸载NAS...")
    success = nas.unmount()
    
    if success:
        print(f"✓ NAS卸载成功")
    else:
        print(f"✗ NAS卸载失败")


def main():
    """主测试函数"""
    print("\n" + "╔" + "═" * 58 + "╗")
    print("║" + " " * 18 + "NAS备份功能测试" + " " * 18 + "║")
    print("╚" + "═" * 58 + "╝\n")
    
    try:
        # 测试1: 配置加载
        nas = test_nas_config()
        
        # 测试2: NAS挂载
        mounted = test_nas_mount(nas)
        
        # 测试3: 路径映射
        test_path_mapping(nas)
        
        # 测试4: 备份功能（模拟）
        if mounted:
            test_backup_simulation(nas)
        
        # 测试5: NAS卸载
        test_nas_unmount(nas)
        
        print("\n" + "=" * 60)
        print("测试完成")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
