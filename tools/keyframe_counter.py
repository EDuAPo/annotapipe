import os
import json
import argparse

def count_keyframes(json_file_path):
    """
    从JSON文件中读取关键帧个数。
    假设JSON格式为：键为字符串'0', '1', '2', ..., 'n'，值是关键帧数据。
    关键帧个数为键的数量。
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # 统计所有键的数量
        keyframe_count = len(data.keys())
        return keyframe_count
    except Exception as e:
        print(f"Error reading {json_file_path}: {e}")
        return None

def count_keyframes_in_directory(directory):
    """
    递归统计目录下的所有JSON文件的关键帧总数。
    """
    total_keyframes = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                count = count_keyframes(file_path)
                if count is not None:
                    total_keyframes += count
    return total_keyframes

def main(path):
    """
    统计路径下的关键帧数量。
    如果是文件，直接统计该JSON文件。
    如果是目录，递归统计所有JSON文件的关键帧总数。
    """
    if os.path.isfile(path):
        if not path.endswith('.json'):
            print(f"文件 {path} 不是JSON文件。")
            return
        count = count_keyframes(path)
        if count is not None:
            print(f"文件 {os.path.basename(path)}: {count}")
        else:
            print(f"无法读取文件 {path}")
    elif os.path.isdir(path):
        total_keyframes = count_keyframes_in_directory(path)
        print(f"目录 {path} 总关键帧数: {total_keyframes}")
    else:
        print(f"路径 {path} 不存在或无效。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count keyframes in JSON file or directory.")
    parser.add_argument("path", help="Path to JSON file or directory to scan.")
    args = parser.parse_args()
    main(args.path)