import yaml
import argparse
import sys
import os

# 添加项目根目录到系统路径，以便可以导入 src 包
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_loader import CustomJsonLoader
from src.rules_checker import RuleChecker
from src.visualizer import Visualizer
from src.batch_processor import BatchProcessor

def load_config(config_path: str):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def main():
    parser = argparse.ArgumentParser(description="3D标注检查工具")
    parser.add_argument('--config', type=str, default='configs/default.yaml', help='配置文件路径')
    parser.add_argument('--mode', type=str, choices=['visualize', 'batch', 'single'], 
                        default='visualize', help='运行模式')
    parser.add_argument('--frame_id', type=str, help='帧ID（用于single模式）')
    args = parser.parse_args()
    
    config = load_config(args.config)
    
    if args.mode == 'batch':
        processor = BatchProcessor(config)
        processor.process_all(config['batch_processing']['output_report_path'])
    elif args.mode == 'single' and args.frame_id:
        data_loader = CustomJsonLoader(
            config['data']['annotation_path'],
            config['data']['pointcloud_path'],
            config
        )
        objects = data_loader.load_annotation(args.frame_id)
        pointcloud = data_loader.load_pointcloud(args.frame_id)
        visualizer = Visualizer(config)
        visualizer.visualize_frame(pointcloud, objects)
    elif args.mode == 'visualize':
        data_loader = CustomJsonLoader(
            config['data']['annotation_path'],
            config['data']['pointcloud_path'],
            config
        )
        frame_ids = data_loader.get_all_frame_ids()
        if not frame_ids:
            print("未找到任何帧。")
            return
        
        print(f"找到 {len(frame_ids)} 帧。默认显示第一帧: {frame_ids[0]}")
        # 如果指定了frame_id，则使用指定的，否则使用第一个
        target_frame = args.frame_id if args.frame_id else frame_ids[0]
        
        objects = data_loader.load_annotation(target_frame)
        pointcloud = data_loader.load_pointcloud(target_frame)
        visualizer = Visualizer(config)
        visualizer.visualize_frame(pointcloud, objects)
    else:
        # 交互式可视化模式
        print("请使用visualize模式或batch模式。")

if __name__ == '__main__':
    main()