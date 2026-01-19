#!/usr/bin/env python3
"""
远程 ZIP 处理脚本
在服务器上解压 ZIP 文件，替换 JSON，调整目录结构

使用方法:
    python3 zip_worker.py --zip /path/to/file.zip --json /path/to/annotation.json --out /output/dir
"""
import os
import sys
import shutil
import zipfile
import argparse
from pathlib import Path


def find_data_root_in_zip(zip_file):
    """在ZIP文件中查找数据根目录（不解压）"""
    required = ["camera_cam_3M_front", "combined_scales", "ins.json", "sample.json"]
    namelist = zip_file.namelist()
    
    # 统计每个目录包含的必需文件数量
    dir_counts = {}
    for name in namelist:
        parts = Path(name).parts
        # 检查每个可能的父目录
        for i in range(len(parts)):
            dir_path = str(Path(*parts[:i+1])) if i < len(parts) - 1 else ""
            if not dir_path:
                continue
            # 检查这个文件/目录名是否在必需列表中
            item_name = parts[i]
            if item_name in required:
                parent = str(Path(*parts[:i])) if i > 0 else ""
                dir_counts[parent] = dir_counts.get(parent, 0) + 1
    
    # 找到包含最多必需文件的目录
    if dir_counts:
        data_root = max(dir_counts.items(), key=lambda x: x[1])[0]
        return data_root
    return ""


def main():
    parser = argparse.ArgumentParser(description="ZIP 文件处理脚本")
    parser.add_argument("--zip", required=True, help="ZIP 文件路径")
    parser.add_argument("--json", required=True, help="JSON 标注文件路径")
    parser.add_argument("--out", required=True, help="输出目录")
    parser.add_argument("--output_name", default=None, help="输出目录名（可选，默认使用 ZIP 文件名）")
    parser.add_argument("--rename_json", default="False", help="是否重命名 JSON 为 annotations.json")
    args = parser.parse_args()
    
    zip_path = Path(args.zip)
    json_path = Path(args.json)
    output_root = Path(args.out)
    rename = args.rename_json.lower() == "true"
    
    # 目标目录：优先使用指定的输出名称，否则使用 ZIP 文件名
    output_name = args.output_name if args.output_name else zip_path.stem
    final_dir = output_root / output_name
    
    try:
        # 创建最终目录（如果已存在则清理）
        if final_dir.exists():
            shutil.rmtree(final_dir)
        final_dir.mkdir(parents=True, exist_ok=True)
        
        # 复制 JSON 文件
        target_json = "annotations.json" if rename else json_path.name
        shutil.copy(str(json_path), str(final_dir / target_json))
        print(f"复制 JSON: {target_json}")
        
        # 需要保留的文件和目录
        keep_items = [
            "sample.json",
            "ins.json", 
            "sensor_config_combined_latest.json",
            "combined_scales",
            "camera_cam_3M_front",
            "camera_cam_3M_left",
            "camera_cam_3M_right",
            "camera_cam_3M_rear",
            "camera_cam_8M_wa_front",
            "iv_points_front_left",
            "iv_points_front_mid",
            "iv_points_front_right",
            "iv_points_rear_left",
            "iv_points_rear_right",
            "iv_points_left_mid",
            "iv_points_right_mid"
        ]
        
        # 选择性解压：只提取需要的文件
        print(f"选择性解压: {zip_path.name}")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # 查找数据根目录
            data_root = find_data_root_in_zip(zf)
            print(f"数据根目录: {data_root}")
            
            # 构建需要提取的文件路径前缀
            prefix = data_root + "/" if data_root else ""
            
            # 遍历ZIP中的所有文件
            extracted_count = 0
            for member in zf.namelist():
                # 检查是否在数据根目录下
                if not member.startswith(prefix):
                    continue
                
                # 获取相对于数据根目录的路径
                rel_path = member[len(prefix):]
                if not rel_path:
                    continue
                
                # 检查是否是需要保留的项目
                should_extract = False
                for item in keep_items:
                    if rel_path == item or rel_path.startswith(item + "/"):
                        should_extract = True
                        break
                
                if should_extract:
                    # 提取到最终目录
                    target_path = final_dir / rel_path
                    
                    # 如果是目录，创建它
                    if member.endswith('/'):
                        target_path.mkdir(parents=True, exist_ok=True)
                    else:
                        # 确保父目录存在
                        target_path.parent.mkdir(parents=True, exist_ok=True)
                        # 提取文件
                        with zf.open(member) as source, open(target_path, 'wb') as target:
                            shutil.copyfileobj(source, target)
                        extracted_count += 1
                        
                        # 每提取100个文件打印一次进度
                        if extracted_count % 100 == 0:
                            print(f"已提取: {extracted_count} 个文件")
        
        print(f"提取完成: 共 {extracted_count} 个文件")
        print("OK")
        
    except Exception as e:
        # 如果出错，清理不完整的目标目录
        if final_dir.exists():
            shutil.rmtree(final_dir)
        raise


if __name__ == "__main__":
    main()
