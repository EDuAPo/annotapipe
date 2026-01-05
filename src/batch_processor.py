import json
from pathlib import Path
from typing import Dict, List
from .data_loader import CustomJsonLoader
from .rules_checker import RuleChecker

class BatchProcessor:
    def __init__(self, config: Dict):
        self.config = config
        self.data_loader = CustomJsonLoader(
            config['data']['annotation_path'],
            config['data']['pointcloud_path'],
            config
        )
        self.rule_checker = RuleChecker(config)
        
    def process_all(self, output_report: str):
        """处理所有标注，生成报告"""
        issues_by_frame = {}
        
        # 1. 获取所有数据并构建轨迹
        # 假设 data_loader 已经缓存了数据
        all_data = self.data_loader.get_all_annotations()
        
        # 如果是字典 {frame_id: [objs]}
        # 如果是列表 [obj, obj, ...] (NuScenes style)
        
        tracks = {} # instance_token -> list of (frame_id, obj)
        
        if isinstance(all_data, dict):
            # Frame-based dict
            for frame_id, objs in all_data.items():
                for obj in objs:
                    inst_id = obj.get('instance_token')
                    if inst_id:
                        if inst_id not in tracks: tracks[inst_id] = []
                        tracks[inst_id].append((frame_id, obj))
        elif isinstance(all_data, list):
            # Flat list
            for obj in all_data:
                inst_id = obj.get('instance_token')
                frame_id = obj.get('frame_id', 'unknown') # 假设有frame_id
                if inst_id:
                    if inst_id not in tracks: tracks[inst_id] = []
                    tracks[inst_id].append((frame_id, obj))
        
        # 对每个轨迹按时间排序 (假设frame_id是可排序的字符串或数字，或者依赖obj中的timestamp)
        # 这里简单按frame_id排序
        for inst_id in tracks:
            tracks[inst_id].sort(key=lambda x: x[0])
            
        # 2. 逐帧检查
        frame_ids = self.data_loader.get_all_frame_ids()
        # 排序frame_ids以保证报告顺序
        try:
            frame_ids.sort(key=lambda x: int(x))
        except:
            frame_ids.sort()
            
        for frame_id in frame_ids:
            objects = self.data_loader.load_annotation(frame_id)
            
            frame_issues = []
            for obj in objects:
                # 基础检查
                obj_issues = self.rule_checker.check_object(obj)
                
                # 运动一致性检查
                inst_id = obj.get('instance_token')
                if inst_id and inst_id in tracks:
                    track = tracks[inst_id]
                    # 找到当前对象在轨迹中的索引
                    idx = -1
                    for i, (fid, o) in enumerate(track):
                        if o is obj: # 引用比较
                            idx = i
                            break
                    
                    if idx != -1:
                        prev_obj = track[idx-1][1] if idx > 0 else None
                        next_obj = track[idx+1][1] if idx < len(track)-1 else None
                        motion_issues = self.rule_checker.check_motion_alignment(obj, prev_obj, next_obj)
                        obj_issues.extend(motion_issues)
                
                if obj_issues:
                    frame_issues.append({
                        'object_token': obj.get('token', 'unknown'),
                        'class_name': obj.get('attribute_tokens', {}).get('Class', 'unknown'),
                        'issues': obj_issues
                    })
            
            if frame_issues:
                issues_by_frame[frame_id] = frame_issues
        
        # 生成报告
        self.generate_report(issues_by_frame, output_report)
        
    def generate_report(self, issues_by_frame: Dict, output_path: str):
        with open(output_path, 'w') as f:
            if issues_by_frame:
                f.write("标注质量检查报告\n")
                f.write("=================\n\n")
                for frame_id, frame_issues in issues_by_frame.items():
                    f.write(f"帧: {frame_id}\n")
                    for obj_issue in frame_issues:
                        f.write(f"  对象: {obj_issue['object_token']} (类别: {obj_issue.get('class_name', 'unknown')})\n")
                        for issue in obj_issue['issues']:
                            f.write(f"    - {issue}\n")
                    f.write("\n")
            else:
                f.write("未发现任何问题。\n")