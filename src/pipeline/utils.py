"""
工具函数
"""
import re


def normalize_zip_name(stem: str) -> str:
    """
    规范化ZIP文件名，移除常见的后缀模式
    
    例如:
    - 20251227_173931-174100_rere_0 -> 20251227_173931-174100
    - 20251227_173931-174100_rere_1 -> 20251227_173931-174100
    - 20251227_173931-174100 -> 20251227_173931-174100 (不变)
    
    Args:
        stem: 原始文件名（不含扩展名）
    
    Returns:
        规范化后的文件名
    """
    # 移除 _rere_数字 等后缀
    normalized = re.sub(r'_rere_\d+$', '', stem)
    # 可以根据需要添加更多模式
    # normalized = re.sub(r'_v\d+$', '', normalized)  # 移除 _v1, _v2 等
    return normalized
