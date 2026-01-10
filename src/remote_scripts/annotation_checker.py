#!/usr/bin/env python3
"""
远程标注质量检查脚本
在服务器上检查标注数据的质量

使用方法:
    python3 annotation_checker.py --data_dir /path/to/data --config /path/to/config.yaml --report /path/to/report.txt
"""
import os
import sys
import json
import math
import argparse
import yaml
import numpy as np
from pathlib import Path


def get_euler_angles(q):
    """四元数转欧拉角 (返回弧度)"""
    w, x, y, z = q
    sinr_cosp = 2 * (w * x + y * z)
    cosr_cosp = 1 - 2 * (x * x + y * y)
    roll = math.atan2(sinr_cosp, cosr_cosp)
    sinp = 2 * (w * y - z * x)
    pitch = math.asin(max(-1, min(1, sinp)))
    siny_cosp = 2 * (w * z + x * y)
    cosy_cosp = 1 - 2 * (y * y + z * z)
    yaw = math.atan2(siny_cosp, cosy_cosp)
    return roll, pitch, yaw


def quaternion_to_rotation_matrix(q):
    """四元数转旋转矩阵"""
    w, x, y, z = q
    return np.array([
        [1 - 2*(y*y + z*z), 2*(x*y - w*z), 2*(x*z + w*y)],
        [2*(x*y + w*z), 1 - 2*(x*x + z*z), 2*(y*z - w*x)],
        [2*(x*z - w*y), 2*(y*z + w*x), 1 - 2*(x*x + y*y)]
    ])


def transform_to_world(pos_ego, ins_entry):
    """将自车坐标系下的位置转换到世界坐标系"""
    ego_utm = np.array([
        ins_entry.get('utm_x', 0),
        ins_entry.get('utm_y', 0),
        ins_entry.get('utm_z', 0)
    ])
    q_ego = [
        ins_entry.get('quaternion_w', 1),
        ins_entry.get('quaternion_x', 0),
        ins_entry.get('quaternion_y', 0),
        ins_entry.get('quaternion_z', 0)
    ]
    R_ego = quaternion_to_rotation_matrix(q_ego)
    return R_ego @ np.array(pos_ego) + ego_utm


def check_vehicle_heading(track_data, frame_to_ins, min_frames=3, min_displacement=1.0):
    """
    使用多帧轨迹检查车辆朝向一致性
    
    Args:
        track_data: [(frame_idx, obj), ...] 按帧排序的轨迹数据
        frame_to_ins: {frame_idx: ins_entry} INS数据映射
        min_frames: 最少需要的帧数
        min_displacement: 最小位移阈值(米)，低于此值视为静止
    
    Returns:
        dict: {frame_idx: issue_msg} 有问题的帧
    """
    issues = {}
    
    if len(track_data) < min_frames:
        return issues
    
    use_world = len(frame_to_ins) > 0
    
    # 提取所有位置
    positions = []
    for frame_idx, obj in track_data:
        pos_ego = np.array(obj.get('translation', [0, 0, 0]))
        ins = frame_to_ins.get(frame_idx)
        if use_world and ins:
            pos = transform_to_world(pos_ego, ins)
        else:
            pos = pos_ego
        positions.append(pos[:2])
    
    positions = np.array(positions)
    
    # 计算总位移，判断是否静止
    total_displacement = np.linalg.norm(positions[-1] - positions[0])
    if total_displacement < min_displacement:
        # 静止车辆，跳过检查
        return issues
    
    # 使用滑动窗口计算局部运动方向（前后各N帧）
    window_size = 2  # 前后各2帧
    
    for i, (frame_idx, obj) in enumerate(track_data):
        rotation = obj.get('rotation', [])
        if len(rotation) != 4:
            continue
        
        # 计算局部运动向量（使用前后多帧）
        start_idx = max(0, i - window_size)
        end_idx = min(len(positions) - 1, i + window_size)
        
        if end_idx - start_idx < 2:
            continue
        
        motion_vec = positions[end_idx] - positions[start_idx]
        local_displacement = np.linalg.norm(motion_vec)
        
        # 局部位移太小，可能是短暂停车或低速，跳过
        if local_displacement < 0.3:
            continue
        
        # 计算运动方向
        motion_yaw = math.atan2(motion_vec[1], motion_vec[0])
        
        # 获取对象朝向
        _, _, obj_yaw_ego = get_euler_angles(rotation)
        ins = frame_to_ins.get(frame_idx)
        if use_world and ins:
            azimuth_rad = math.radians(ins.get('azimuth', 0))
            obj_yaw = azimuth_rad + obj_yaw_ego
        else:
            obj_yaw = obj_yaw_ego
        
        # 计算角度差并归一化
        diff = motion_yaw - obj_yaw
        while diff > math.pi:
            diff -= 2 * math.pi
        while diff < -math.pi:
            diff += 2 * math.pi
        diff_abs = abs(diff)
        
        # 判断：正向（<60°）、倒车（~180°±60°）、或异常
        is_forward = diff_abs < 1.05  # ~60度
        is_backward = abs(diff_abs - math.pi) < 1.05  # 倒车
        
        if not is_forward and not is_backward:
            direction = "侧向" if 1.05 <= diff_abs <= 2.09 else "异常"
            issues[frame_idx] = (
                f"朝向与运动方向不一致({direction}): "
                f"差值{math.degrees(diff_abs):.1f}°, "
                f"局部位移{local_displacement:.2f}m"
            )
    
    return issues


def check_object(obj, rules, prev_obj=None, next_obj=None, 
                 curr_ins=None, prev_ins=None, next_ins=None):
    """检查单个对象 - 保留接口兼容性，实际检查在 check_vehicle_heading 中完成"""
    # 此函数保留用于兼容，实际多帧检查在 main 中调用 check_vehicle_heading
    return []


def main():
    parser = argparse.ArgumentParser(description="标注质量检查脚本")
    parser.add_argument("--data_dir", required=True, help="数据目录")
    parser.add_argument("--config", required=True, help="配置文件路径")
    parser.add_argument("--report", required=True, help="报告输出路径")
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    
    # 加载配置
    rules = {}
    if Path(args.config).exists():
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
            rules = config.get('rules', {})
    
    # 加载 INS 数据
    ins_data = None
    ins_file = data_dir / 'ins.json'
    if ins_file.exists():
        try:
            with open(ins_file, 'r') as f:
                ins_data = json.load(f)
            print(f"已加载 INS 数据: {len(ins_data)} 条")
        except Exception as e:
            print(f"加载 INS 数据失败: {e}")
    else:
        print("未找到 ins.json，将不进行自车位姿补偿")
    
    # 查找标注文件
    annotation_file = None
    for name in ['annotations.json', 'annotation.json']:
        p = data_dir / name
        if p.exists():
            annotation_file = p
            break
    
    if not annotation_file:
        for f in data_dir.glob('*.json'):
            if f.name not in ['sample.json', 'ins.json', 'sensor_config_combined_latest.json']:
                annotation_file = f
                break
    
    if not annotation_file:
        print("ERROR: 未找到标注文件")
        sys.exit(1)
    
    # 加载标注
    print(f"加载标注文件: {annotation_file.name}")
    with open(annotation_file, 'r') as f:
        data = json.load(f)
    
    # 解析帧数据
    frames_to_check = []
    if isinstance(data, dict):
        if 'frames' in data:
            for frame in data['frames']:
                frame_id = frame.get('frame_id', frame.get('id', 'unknown'))
                objects = frame.get('objects', [])
                frames_to_check.append((str(frame_id), objects))
        else:
            for frame_id, objects in data.items():
                if isinstance(objects, list):
                    frames_to_check.append((str(frame_id), objects))
    
    # 排序
    try:
        frames_to_check.sort(key=lambda x: int(x[0]))
    except (ValueError, TypeError):
        frames_to_check.sort(key=lambda x: x[0])
    
    total_frames = len(frames_to_check)
    print(f"开始检查 {total_frames} 帧 (仅检查车辆朝向)...")
    
    # 构建 INS 索引
    frame_to_ins = {}
    if ins_data:
        for i in range(min(len(frames_to_check), len(ins_data))):
            frame_to_ins[i] = ins_data[i]
    
    # 构建实例轨迹（仅车辆）
    tracks = {}
    total_objects = 0
    vehicle_count = 0
    
    for i, (frame_id, objects) in enumerate(frames_to_check):
        total_objects += len(objects)
        for obj in objects:
            obj_class = obj.get('attribute_tokens', {}).get('Class', '').lower()
            if 'vehicle' not in obj_class:
                continue
            
            inst_id = obj.get('instance_token')
            if inst_id:
                vehicle_count += 1
                if inst_id not in tracks:
                    tracks[inst_id] = []
                tracks[inst_id].append((i, obj))
    
    for inst_id in tracks:
        tracks[inst_id].sort(key=lambda x: x[0])
    
    print(f"  车辆实例数: {len(tracks)}, 车辆标注数: {vehicle_count}")
    
    # 使用多帧轨迹检查每个车辆实例
    issues_by_frame = {}
    issue_objects = 0
    checked_instances = 0
    skipped_static = 0
    skipped_short = 0
    
    for inst_id, track_data in tracks.items():
        if len(track_data) < 3:
            skipped_short += 1
            continue
        
        checked_instances += 1
        track_issues = check_vehicle_heading(track_data, frame_to_ins)
        
        if not track_issues:
            # 检查是否因静止被跳过
            positions = []
            for frame_idx, obj in track_data:
                pos = np.array(obj.get('translation', [0, 0, 0]))[:2]
                positions.append(pos)
            total_disp = np.linalg.norm(np.array(positions[-1]) - np.array(positions[0]))
            if total_disp < 1.0:
                skipped_static += 1
            continue
        
        # 记录问题
        for frame_idx, issue_msg in track_issues.items():
            issue_objects += 1
            frame_id = frames_to_check[frame_idx][0]
            obj = next(o for fi, o in track_data if fi == frame_idx)
            
            if frame_id not in issues_by_frame:
                issues_by_frame[frame_id] = []
            
            issues_by_frame[frame_id].append({
                'token': obj.get('token', 'unknown'),
                'instance': inst_id[:8] + '...',
                'class': obj.get('attribute_tokens', {}).get('Class', 'unknown'),
                'issues': [issue_msg]
            })
    
    issue_frames = len(issues_by_frame)
    print(f"  检查实例: {checked_instances}, 静止跳过: {skipped_static}, 轨迹过短: {skipped_short}")
    print(f"\n检查完成!")
    print(f"  总帧数: {total_frames}")
    print(f"  总对象数: {total_objects}")
    print(f"  问题帧数: {issue_frames}")
    print(f"  问题对象数: {issue_objects}")
    
    # 写入报告
    with open(args.report, 'w') as f:
        f.write(f"检查报告 - {data_dir.name}\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"检查项目: 车辆朝向与运动方向一致性\n\n")
        f.write(f"统计汇总:\n")
        f.write(f"  总帧数: {total_frames}\n")
        f.write(f"  总对象数: {total_objects}\n")
        f.write(f"  问题帧数: {issue_frames}\n")
        f.write(f"  问题对象数: {issue_objects}\n")
        f.write(f"  通过率: {(total_frames - issue_frames) * 100 / max(total_frames, 1):.1f}%\n")
        if ins_data:
            f.write(f"  自车位姿补偿: 已启用 ({len(ins_data)} 条INS数据)\n")
        else:
            f.write(f"  自车位姿补偿: 未启用\n")
        f.write("\n" + "=" * 50 + "\n\n")
        
        if not issues_by_frame:
            f.write("恭喜! 所有帧检查通过，未发现问题。\n")
        else:
            f.write("问题详情:\n\n")
            for frame_id, issues in sorted(issues_by_frame.items(), 
                                           key=lambda x: int(x[0]) if x[0].isdigit() else x[0]):
                f.write(f"帧: {frame_id}\n")
                for item in issues:
                    f.write(f"  对象: {item['token']} (类别: {item['class']})\n")
                    for issue in item['issues']:
                        f.write(f"    - {issue}\n")
                f.write("\n")
    
    if issue_frames == 0:
        print("RESULT: PASS")
    else:
        print(f"RESULT: FAIL ({issue_frames} frames with issues)")


if __name__ == "__main__":
    main()
