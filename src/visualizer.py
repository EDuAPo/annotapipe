import open3d as o3d
import numpy as np
from typing import List, Dict

class Visualizer:
    def __init__(self, config: Dict):
        self.config = config
        self.window_width = config['visualization']['window_width']
        self.window_height = config['visualization']['window_height']
        self.background_color = config['visualization']['background_color']
        self.point_size = config['visualization']['point_size']
        
        # 颜色映射
        self.color_map = {
            'vehicle.car': [1, 0, 0],
            'human.pedestrian.adult': [0, 1, 0],
            'traffic_cone': [1, 1, 0],
            'traffic_sign': [0, 0, 1],
            'default': [1, 0.5, 0]
        }
    
    def quaternion_to_rotation_matrix(self, q):
        # 假设输入四元数格式为 [w, x, y, z]
        w, x, y, z = q
        R = np.array([
            [1 - 2*y*y - 2*z*z,     2*x*y - 2*z*w,     2*x*z + 2*y*w],
            [2*x*y + 2*z*w,     1 - 2*x*x - 2*z*z,     2*y*z - 2*x*w],
            [2*x*z - 2*y*w,         2*y*z + 2*x*w,     1 - 2*x*x - 2*y*y]
        ])
        return R
    
    def create_3d_box(self, center, size, rotation_q):
        l, w, h = size
        x_corners = [l/2, l/2, -l/2, -l/2, l/2, l/2, -l/2, -l/2]
        y_corners = [w/2, -w/2, -w/2, w/2, w/2, -w/2, -w/2, w/2]
        z_corners = [-h/2, -h/2, -h/2, -h/2, h/2, h/2, h/2, h/2]
        corners = np.array([x_corners, y_corners, z_corners])
        
        R = self.quaternion_to_rotation_matrix(rotation_q)
        rotated_corners = R @ corners
        rotated_corners[0, :] += center[0]
        rotated_corners[1, :] += center[1]
        rotated_corners[2, :] += center[2]
        
        return rotated_corners.T
    
    def get_box_color(self, obj_class: str) -> List[float]:
        for key, color in self.color_map.items():
            if key in obj_class:
                return color
        return self.color_map['default']
    
    def draw_boxes(self, objects: List[Dict]) -> List[o3d.geometry.LineSet]:
        boxes = []
        for obj in objects:
            center = obj['translation']
            size = obj['size']
            rotation = obj['rotation']
            obj_class = obj['attribute_tokens'].get('Class', 'default')
            
            corners = self.create_3d_box(center, size, rotation)
            lines = [[0,1],[1,2],[2,3],[3,0],
                     [4,5],[5,6],[6,7],[7,4],
                     [0,4],[1,5],[2,6],[3,7]]
            line_set = o3d.geometry.LineSet()
            line_set.points = o3d.utility.Vector3dVector(corners)
            line_set.lines = o3d.utility.Vector2iVector(lines)
            color = self.get_box_color(obj_class)
            line_set.paint_uniform_color(color)
            boxes.append(line_set)

            # 仅对车辆绘制朝向
            if 'vehicle' in obj_class.lower():
                # 绘制朝向 (假设x轴为正方向)
                l, w, h = size
                
                # 绘制一个箭头来表示朝向
                # 箭头长度：从中心延伸到前方盒子外
                arrow_len = l / 2.0 + 0.5  # 延伸出盒子0.5米
                if arrow_len < 1.0: arrow_len = 1.0 # 最小长度
                
                # 定义箭头在局部坐标系下的点
                # 0: 中心点
                # 1: 箭头尖端
                # 2: 箭头左翼
                # 3: 箭头右翼
                pts_local = np.array([
                    [0, 0, 0],              # Center
                    [arrow_len, 0, 0],      # Tip
                    [arrow_len - 0.3, 0.2, 0], # Left wing
                    [arrow_len - 0.3, -0.2, 0] # Right wing
                ])
                
                # 旋转并平移到世界坐标系
                R = self.quaternion_to_rotation_matrix(rotation)
                pts_world = (R @ pts_local.T).T + np.array(center)
                
                # 连接线段: Center->Tip, Tip->Left, Tip->Right
                dir_lines = [[0, 1], [1, 2], [1, 3]]
                
                dir_line_set = o3d.geometry.LineSet()
                dir_line_set.points = o3d.utility.Vector3dVector(pts_world)
                dir_line_set.lines = o3d.utility.Vector2iVector(dir_lines)
                # 使用醒目的颜色 (例如青色) 表示朝向
                dir_line_set.paint_uniform_color([0, 1, 1]) 
                boxes.append(dir_line_set)

        return boxes
    
    def visualize_frame(self, pointcloud: np.ndarray, objects: List[Dict]):
        self.print_frame_info(objects)
        
        geometries = []
        
        # 点云
        pcd = o3d.geometry.PointCloud()
        pcd.points = o3d.utility.Vector3dVector(pointcloud)
        pcd.paint_uniform_color([0.5, 0.5, 0.5])
        geometries.append(pcd)
        
        # 3D框
        boxes = self.draw_boxes(objects)
        geometries.extend(boxes)
        
        # 坐标轴
        axis = o3d.geometry.TriangleMesh.create_coordinate_frame(size=2.0, origin=[0, 0, 0])
        geometries.append(axis)
        
        # 地面网格 (辅助检查高度)
        grid = self.create_ground_grid()
        geometries.append(grid)
        
        # 可视化
        vis = o3d.visualization.Visualizer()
        vis.create_window(window_name="3D标注可视化", width=self.window_width, height=self.window_height)
        
        for geometry in geometries:
            vis.add_geometry(geometry)
            
        # 设置视角为第一人称 (从后上方看向前方)
        ctr = vis.get_view_control()
        # 默认视角参数，可以根据实际效果微调
        # 假设车辆朝向X轴正方向
        # 摄像机位置: 车辆后方 (-10, 0, 5)
        # 观察点: 车辆前方 (20, 0, 0)
        # 上方向: Z轴 (0, 0, 1)
        ctr.set_front([-1.0, 0.0, 0.5]) # 摄像机朝向向量 (反向) - 实际上Open3D的set_front是设置视线方向的反方向(即摄像机位置相对于焦点的方向)
        # 更简单的方法是使用 set_lookat, set_up, set_zoom
        # 但Open3D的ViewControl API比较复杂，通常 set_front, set_lookat, set_up, set_zoom 组合使用
        
        # 尝试设置一个合理的初始视角
        ctr.set_lookat([10.0, 0.0, 0.0])  # 看向前方
        ctr.set_up([0.0, 0.0, 1.0])       # Z轴向上
        ctr.set_front([-1.0, 0.0, 0.5])   # 摄像机在后上方
        ctr.set_zoom(0.3)                 # 缩放比例
        
        vis.run()
        vis.destroy_window()

    def print_frame_info(self, objects: List[Dict]):
        print("-" * 50)
        print(f"当前帧包含 {len(objects)} 个对象:")
        print(f"{'Index':<6} | {'Class':<25} | {'State':<10} | {'Instance ID':<20} | {'Size (L,W,H)':<20}")
        print("-" * 90)
        for i, obj in enumerate(objects):
            attrs = obj.get('attribute_tokens', {})
            cls = attrs.get('Class', 'unknown')
            state = attrs.get('State', 'N/A')
            inst_id = obj.get('instance_token', 'N/A')
            size = obj.get('size', [0,0,0])
            size_str = f"{size[0]:.2f}, {size[1]:.2f}, {size[2]:.2f}"
            
            # 尝试获取速度信息 (如果有)
            velo = obj.get('velocity', [])
            velo_str = f"{velo[0]:.2f}, {velo[1]:.2f}" if len(velo) >= 2 else "N/A"
            
            print(f"{i:<6} | {cls:<25} | {state:<10} | {inst_id:<20} | {size_str:<20} | Vel: {velo_str}")
        print("-" * 50)

    def create_ground_grid(self):
        # 获取地面高度配置
        coord_sys = self.config.get('coordinate_system', {})
        target_frame = coord_sys.get('frame', 'vehicle')
        sensor_height = coord_sys.get('sensor_height', 0.0)
        
        z_height = 0.0
        if target_frame == 'lidar':
            z_height = -sensor_height
            
        # 创建一个简单的网格
        lines = []
        points = []
        
        step = 2.0
        count = 20
        min_v = -count * step
        max_v = count * step
        
        # x lines
        for i in range(-count, count + 1):
            x = i * step
            points.append([x, min_v, z_height])
            points.append([x, max_v, z_height])
            lines.append([len(points)-2, len(points)-1])
            
        # y lines
        for i in range(-count, count + 1):
            y = i * step
            points.append([min_v, y, z_height])
            points.append([max_v, y, z_height])
            lines.append([len(points)-2, len(points)-1])
            
        line_set = o3d.geometry.LineSet()
        line_set.points = o3d.utility.Vector3dVector(points)
        line_set.lines = o3d.utility.Vector2iVector(lines)
        line_set.paint_uniform_color([0.2, 0.2, 0.2])
        return line_set