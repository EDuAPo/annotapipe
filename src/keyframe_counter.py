import os
import json
import argparse

def count_keyframes(json_file_path):
    """
    从JSON文件中读取关键帧个数。
    假设JSON格式为：键为字符串'1', '2', ..., 'n'，值是关键帧数据。
    关键帧ID从'1'开始，排除'0'。
    关键帧个数为键的数量减去'0'（如果存在）。
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # 排除'0'，关键帧从'1'开始
        keyframe_count = len([k for k in data.keys() if k != '0'])
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

def main(root_directory):
    """
    统计根目录下每个子目录的关键帧数量，并计算总和。
    """
    if not os.path.isdir(root_directory):
        print(f"Directory {root_directory} does not exist.")
        return

    subdirs = [d for d in os.listdir(root_directory) if os.path.isdir(os.path.join(root_directory, d))]
    total_all = 0
    for subdir in sorted(subdirs):
        subdir_path = os.path.join(root_directory, subdir)
        count = count_keyframes_in_directory(subdir_path)
        print(f"{subdir}: {count}")
        total_all += count

    print(f"\n总和: {total_all}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count total keyframes in subdirectories.")
    parser.add_argument("directory", help="Root directory to scan.")
    args = parser.parse_args()
    main(args.directory)