# 3D Annotation Checker (3D标注检查工具)

这是一个用于检查3D点云标注质量的工具，支持可视化检查和基于规则的批量自动检查。

## 功能特性

*   **多维度规则检查**：涵盖点云数量、物体尺寸、姿态角、运动一致性等。
*   **3D可视化**：基于Open3D的交互式可视化，支持点云、3D框、朝向箭头、地面网格叠加显示。
*   **自动坐标系对齐**：支持读取传感器外参文件，自动将点云对齐到车体坐标系。
*   **批量报告生成**：自动扫描所有数据帧，生成详细的错误报告。

## 检查规则与阈值

工具依据 `configs/user_config.yaml` 和内置逻辑执行以下检查：

### 1. 基础属性检查

| 检查项 | 描述 | 默认阈值/范围 |
| :--- | :--- | :--- |
| **最小点云数** | 检查框内包含的激光雷达点数 | $\ge 3$ 个点 |
| **四元数归一化** | 检查旋转四元数的模长是否接近1 | 误差 $\le 0.01$ |

### 2. 尺寸合理性检查 (Size Constraints)

根据物体类别检查长(L)、宽(W)、高(H)是否在物理合理范围内：

*   **Vehicle (车辆)**:
    *   长: 2.0m ~ 12.0m
    *   宽: 1.0m ~ 3.0m
    *   高: 1.0m ~ 4.0m
*   **Pedestrian (行人)**:
    *   长: 0.3m ~ 1.2m
    *   宽: 0.3m ~ 1.2m
    *   高: 1.0m ~ 2.5m
*   **Cone (锥桶)**:
    *   长/宽: 0.1m ~ 0.5m
    *   高: 0.3m ~ 0.8m
*   **Sign (标志牌)**:
    *   长/宽: 0.05m ~ 2.0m
    *   高: 0.05m ~ 3.0m

### 3. 车辆姿态与运动检查

*   **姿态角 (Roll/Pitch)**:
    *   检查车辆是否存在异常的翻滚或俯仰。
    *   **阈值**: 绝对值 $\le 0.5$ 弧度 (约 28.6°)。
*   **朝向一致性 (Motion Consistency)**:
    *   **原理**: 比较车辆的标注朝向 (Yaw) 与其实际运动轨迹方向 (Motion Vector)。
    *   **触发条件**: 车辆位移 $\ge 0.5$ 米 (忽略静止物体)。
    *   **阈值**: 角度差异 $\le 30^\circ$ (0.52 弧度)。
    *   *注: 自动识别并忽略倒车情况 (差异接近 180°)。*

## 使用说明

### 1. 环境准备

需要安装以下Python库：
```bash
pip install numpy open3d pyyaml
```

### 2. 配置文件

修改 `configs/user_config.yaml` 以指定数据路径和参数：

```yaml
data:
  annotation_path: "/path/to/annotations.json"
  pointcloud_path: "/path/to/pointcloud_dir/"

coordinate_system:
  frame: "vehicle"       # 目标坐标系
  sensor_height: 1.84    # 无外参时的默认雷达高度

rules:
  # 在此调整各类别的尺寸阈值
```

### 3. 运行脚本

#### 模式一：可视化检查 (Visualize)

加载数据并打开3D窗口，显示点云、检测框和朝向箭头（青色）。

```bash
python3 src/main.py --config configs/user_config.yaml --mode visualize
```
*   默认显示第一帧。
*   **交互操作**: 鼠标左键旋转，右键平移，滚轮缩放。

#### 模式二：批量自动检查 (Batch)

扫描所有帧并生成文本报告。

```bash
python3 src/main.py --config configs/user_config.yaml --mode batch
```

### 4. 查看报告

运行批量模式后，检查结果将保存在 `reports/check_report.txt`。

**报告示例**:
```text
帧: 131
  对象: token_131_125-108 (类别: human.pedestrian.adult)
    - 点云数量过少: 0

帧: 150
  对象: token_150_161-62 (类别: human.pedestrian.adult)
    - 长度异常: 1.259047703565082
```
