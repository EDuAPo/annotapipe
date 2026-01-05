import math
import numpy as np
from typing import Dict, List, Tuple

class RuleChecker:
    def __init__(self, config: Dict):
        self.config = config
        self.rules = config['rules']
        
    def get_euler_angles(self, q):
        # 假设输入四元数格式为 [w, x, y, z]
        w, x, y, z = q
        # Roll (x-axis rotation)
        sinr_cosp = 2 * (w * x + y * z)
        cosr_cosp = 1 - 2 * (x * x + y * y)
        roll = math.atan2(sinr_cosp, cosr_cosp)

        # Pitch (y-axis rotation)
        sinp = 2 * (w * y - z * x)
        if abs(sinp) >= 1:
            pitch = math.copysign(math.pi / 2, sinp)
        else:
            pitch = math.asin(sinp)

        # Yaw (z-axis rotation)
        siny_cosp = 2 * (w * z + x * y)
        cosy_cosp = 1 - 2 * (y * y + z * z)
        yaw = math.atan2(siny_cosp, cosy_cosp)

        return roll, pitch, yaw

    def check_motion_alignment(self, obj: Dict, prev_obj: Dict, next_obj: Dict) -> List[str]:
        """
        检查车辆朝向与运动轨迹的一致性
        Industrial Method: Motion Consistency Check
        """
        issues = []
        obj_class = obj.get('attribute_tokens', {}).get('Class', 'unknown').lower()
        
        # 仅检查车辆
        if 'vehicle' not in obj_class:
            return issues
            
        # 获取当前位置
        curr_pos = np.array(obj.get('translation', [0,0,0]))
        
        # 计算运动向量
        motion_vec = None
        
        # 优先使用前后帧计算
        if prev_obj and next_obj:
            prev_pos = np.array(prev_obj.get('translation', [0,0,0]))
            next_pos = np.array(next_obj.get('translation', [0,0,0]))
            motion_vec = next_pos - prev_pos
        elif next_obj:
            next_pos = np.array(next_obj.get('translation', [0,0,0]))
            motion_vec = next_pos - curr_pos
        elif prev_obj:
            prev_pos = np.array(prev_obj.get('translation', [0,0,0]))
            motion_vec = curr_pos - prev_pos
            
        if motion_vec is None:
            return issues
            
        # 计算速度模长 (假设帧间隔恒定，这里只看位移大小)
        # 如果位移太小，认为是静止或噪声，不进行朝向检查
        dist = np.linalg.norm(motion_vec[:2]) # 只看XY平面
        if dist < 0.5: # 阈值可调，例如0.5米
            return issues
            
        # 计算运动方向 (Yaw)
        motion_yaw = math.atan2(motion_vec[1], motion_vec[0])
        
        # 获取标注朝向 (Yaw)
        rotation = obj.get('rotation', [])
        if len(rotation) == 4:
            _, _, obj_yaw = self.get_euler_angles(rotation)
            
            # 计算角度差 (考虑周期性)
            diff = abs(motion_yaw - obj_yaw)
            while diff > math.pi:
                diff -= 2 * math.pi
            diff = abs(diff)
            
            # 检查一致性
            # 允许误差: 30度 (约0.52弧度)
            # 考虑倒车情况: 差值接近 PI
            is_forward = diff < 0.52
            is_backward = abs(diff - math.pi) < 0.52
            
            if not is_forward and not is_backward:
                issues.append(f"朝向与轨迹不一致: 运动Yaw={motion_yaw:.2f}, 标注Yaw={obj_yaw:.2f}, 差值={math.degrees(diff):.1f}度")
            elif is_backward:
                # 倒车是合法的，但可以标记一下，或者如果大部分时间是倒车可能需要确认
                # issues.append(f"Info: 车辆正在倒车")
                pass
                
        return issues

    def check_object(self, obj: Dict) -> List[str]:
        """检查单个对象，返回问题列表"""
        issues = []
        obj_token = obj.get('token', 'unknown')
        size = obj.get('size', [])
        center = obj.get('translation', [])
        rotation = obj.get('rotation', [])
        num_pts = obj.get('num_lidar_pts', 0)
        obj_class = obj.get('attribute_tokens', {}).get('Class', 'unknown').lower()
        
        # 检查点云数量
        if num_pts < self.rules['min_lidar_points']:
            issues.append(f"点云数量过少: {num_pts}")
        
        # 检查尺寸
        if len(size) == 3:
            l, w, h = size
            # 根据类别选择规则
            if 'vehicle' in obj_class:
                rule = self.rules['vehicle']
            elif 'pedestrian' in obj_class:
                rule = self.rules['pedestrian']
            elif 'cone' in obj_class:
                rule = self.rules['cone']
            elif 'sign' in obj_class:
                rule = self.rules['sign']
            else:
                rule = None
                
            if rule:
                if not (rule['length_range'][0] <= l <= rule['length_range'][1]):
                    issues.append(f"长度异常: {l}")
                if not (rule['width_range'][0] <= w <= rule['width_range'][1]):
                    issues.append(f"宽度异常: {w}")
                if not (rule['height_range'][0] <= h <= rule['height_range'][1]):
                    issues.append(f"高度异常: {h}")
        
        # 检查高度位置
        # if len(center) == 3:
        #     z = center[2]
        #     # 简单检查：物体应该在地面附近，这里假设地面z=0，允许上下浮动2米
        #     if abs(z) > 2.0:  # 根据实际情况调整
        #         issues.append(f"高度异常: {z}")
        
        # 检查四元数归一化
        if len(rotation) == 4:
            norm = math.sqrt(sum([r*r for r in rotation]))
            if abs(norm - 1.0) > 0.01:
                issues.append(f"四元数未归一化: {norm}")
            
            # 检查车辆的Roll和Pitch
            if 'vehicle' in obj_class:
                roll, pitch, yaw = self.get_euler_angles(rotation)
                # 阈值设为 0.5 弧度 (约28度)，防止车辆翻车或严重倾斜
                if abs(roll) > 0.5:
                    issues.append(f"车辆Roll异常: {roll:.2f}")
                if abs(pitch) > 0.5:
                    issues.append(f"车辆Pitch异常: {pitch:.2f}")
        
        return issues