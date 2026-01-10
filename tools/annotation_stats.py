import json
import argparse
import os
import sys
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import math


def load_json(file_path: str) -> Dict[str, Any]:
    """
    加载JSON文件
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        sys.exit(1)


def detect_annotation_type(data: Dict[str, Any]) -> str:
    """
    检测标注类型
    """
    has_box = False
    has_2d = False
    has_line = False
    for sample_id, annotations in data.items():
        for ann in annotations:
            if 'translation' in ann and 'size' in ann and 'rotation' in ann:
                has_box = True
            if 'bbox' in ann and 'translation' not in ann and 'polyine' not in ann:
                has_2d = True
            if 'polyine' in ann:
                has_line = True
    types = []
    if has_box:
        types.append("3D拉框")
    if has_2d:
        types.append("2D矩形框")
    if has_line:
        types.append("3D线段")
    if len(types) > 1:
        return "混合格式 (" + ", ".join(types) + ")"
    elif types:
        return types[0] + "标注"
    else:
        return "未知格式"


def calculate_distance(p1: Dict[str, float], p2: Dict[str, float]) -> float:
    """
    计算两点之间的欧几里得距离
    """
    return math.sqrt((p1['x'] - p2['x'])**2 + (p1['y'] - p2['y'])**2 + (p1['z'] - p2['z'])**2)


def calculate_polyline_length(polyline: List[Dict[str, float]]) -> float:
    """
    计算折线段的总长度
    """
    if len(polyline) < 2:
        return 0.0
    length = 0.0
    for i in range(len(polyline) - 1):
        length += calculate_distance(polyline[i], polyline[i+1])
    return length


def collect_stats(data: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    """
    收集统计数据
    """
    ann_type = detect_annotation_type(data)
    stats = {
        'sample_count': len(data),
        'total_annotations': 0,
        'box_count': 0,
        '2d_box_count': 0,
        'line_count': 0,
        'total_line_points': 0,
        'box_categories': defaultdict(lambda: {'count': 0, 'sizes': []}),
        '2d_categories': defaultdict(lambda: {'count': 0}),
        'line_categories': defaultdict(lambda: {'count': 0, 'lengths': [], 'point_counts': []}),
    }
    for sample_id, annotations in data.items():
        stats['total_annotations'] += len(annotations)
        for ann in annotations:
            cls = ann.get('attribute_tokens', {}).get('Class', 'unknown')
            if 'translation' in ann and 'size' in ann and 'rotation' in ann:
                stats['box_count'] += 1
                stats['box_categories'][cls]['count'] += 1
                size = ann['size']
                stats['box_categories'][cls]['sizes'].append(size)
            if 'bbox' in ann and 'translation' not in ann and 'polyine' not in ann:
                stats['2d_box_count'] += 1
                stats['2d_categories'][cls]['count'] += 1
            if 'polyine' in ann:
                stats['line_count'] += 1
                polyline = ann['polyine']
                length = calculate_polyline_length(polyline)
                point_count = len(polyline)
                stats['total_line_points'] += point_count
                stats['line_categories'][cls]['count'] += 1
                stats['line_categories'][cls]['lengths'].append(length)
                stats['line_categories'][cls]['point_counts'].append(point_count)
    return stats, ann_type


def compute_averages(stats: Dict[str, Any]):
    """
    计算平均值
    """
    for cls, data in stats['box_categories'].items():
        if data['sizes']:
            avg_size = [sum(dim)/len(data['sizes']) for dim in zip(*data['sizes'])]
            data['avg_size'] = avg_size
    for cls, data in stats['line_categories'].items():
        if data['lengths']:
            data['avg_length'] = sum(data['lengths']) / len(data['lengths'])
        if data['point_counts']:
            data['avg_points'] = sum(data['point_counts']) / len(data['point_counts'])


def print_stats(file_path: str, stats: Dict[str, Any], ann_type: str):
    """
    打印统计结果到控制台
    """
    print("=== JSON标注统计报告 ===")
    print(f"文件: {os.path.basename(file_path)}")
    print(f"类型: {ann_type}")
    print(f"样本数量: {stats['sample_count']}")
    print(f"总标注数量: {stats['total_annotations']}")
    print()

    if stats['box_count'] > 0:
        print("=== 3D拉框统计 ===")
        print(f"总数: {stats['box_count']}")
        print("按类别统计:")
        for cls, data in stats['box_categories'].items():
            count = data['count']
            if 'avg_size' in data:
                avg = data['avg_size']
                print(f"  {cls}: {count} (平均尺寸: {avg[0]:.1f}x{avg[1]:.1f}x{avg[2]:.1f}m)")
            else:
                print(f"  {cls}: {count}")
        print()

    if stats['2d_box_count'] > 0:
        print("=== 2D矩形框统计 ===")
        print(f"总数: {stats['2d_box_count']}")
        print("按类别统计:")
        for cls, data in stats['2d_categories'].items():
            count = data['count']
            print(f"  {cls}: {count}")
        print()

    if stats['line_count'] > 0:
        print("=== 3D线段统计 ===")
        print(f"总数: {stats['line_count']}")
        print("按类别统计:")
        for cls, data in stats['line_categories'].items():
            count = data['count']
            avg_len = data.get('avg_length', 0)
            avg_pts = data.get('avg_points', 0)
            print(f"  {cls}: {count} (平均长度: {avg_len:.1f}m, 平均点数: {avg_pts:.1f})")
        print(f"总线段点数: {stats['total_line_points']}")
        print()

    print("=== 总结 ===")
    print(f"- 3D拉框: {stats['box_count']}个")
    print(f"- 2D矩形框: {stats['2d_box_count']}个")
    print(f"- 3D线段: {stats['line_count']}个")


def save_json(stats: Dict[str, Any], output_path: str):
    """
    保存统计结果为JSON
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)


def save_csv(stats: Dict[str, Any], output_path: str):
    """
    保存统计结果为CSV
    """
    import csv
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['类别', '类型', '数量', '平均尺寸/长度'])
        for cls, data in stats['box_categories'].items():
            count = data['count']
            avg_size = data.get('avg_size', [0,0,0])
            writer.writerow([cls, '3D拉框', count, f"{avg_size[0]:.1f}x{avg_size[1]:.1f}x{avg_size[2]:.1f}m"])
        for cls, data in stats['2d_categories'].items():
            count = data['count']
            writer.writerow([cls, '2D矩形框', count, ''])
        for cls, data in stats['line_categories'].items():
            count = data['count']
            avg_len = data.get('avg_length', 0)
            writer.writerow([cls, '3D线段', count, f"{avg_len:.1f}m"])


def main():
    parser = argparse.ArgumentParser(description="统计自动驾驶感知标注JSON文件")
    parser.add_argument('file_path', help='JSON文件路径')
    parser.add_argument('--output', choices=['console', 'json', 'csv'], default='console', help='输出格式')
    parser.add_argument('--output_path', help='输出文件路径（用于json/csv）')
    args = parser.parse_args()

    if not os.path.exists(args.file_path):
        print(f"文件不存在: {args.file_path}")
        sys.exit(1)

    data = load_json(args.file_path)
    stats, ann_type = collect_stats(data)
    compute_averages(stats)

    if args.output == 'console':
        print_stats(args.file_path, stats, ann_type)
    elif args.output == 'json':
        output_path = args.output_path or args.file_path.replace('.json', '_stats.json')
        save_json(stats, output_path)
        print(f"统计结果已保存到: {output_path}")
    elif args.output == 'csv':
        output_path = args.output_path or args.file_path.replace('.json', '_stats.csv')
        save_csv(stats, output_path)
        print(f"统计结果已保存到: {output_path}")


if __name__ == '__main__':
    main()