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


def find_data_root(extract_dir):
    """查找数据根目录（包含必需文件的目录）"""
    required = ["camera_cam_3M_front", "combined_scales", "ins.json", "sample.json"]
    for root, dirs, files in os.walk(extract_dir):
        count = sum(1 for name in dirs + files if name in required)
        if count >= 2:
            return Path(root)
    return None


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
    temp_dir = output_root / f"temp_{output_name}"
    
    # 清理临时目录
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # 解压 ZIP
        print(f"解压: {zip_path.name}")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(temp_dir)
        
        # 查找数据根目录
        data_root = find_data_root(temp_dir)
        if not data_root:
            raise Exception("未找到数据根目录")
        
        print(f"数据根目录: {data_root}")
        
        # 创建最终目录
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
        ]
        
        # 复制需要的文件
        for item in keep_items:
            src = data_root / item
            if src.exists():
                dst = final_dir / item
                # 如果目标已存在，先删除
                if dst.exists():
                    if dst.is_dir():
                        shutil.rmtree(dst)
                    else:
                        dst.unlink()
                # 复制
                if src.is_dir():
                    shutil.copytree(str(src), str(dst))
                else:
                    shutil.copy(str(src), str(dst))
                print(f"复制: {item}")
        
        print("OK")
        
    finally:
        # 清理临时目录
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    main()
