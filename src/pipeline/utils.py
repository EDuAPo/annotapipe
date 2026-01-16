"""
工具函数
"""
import re
from typing import List


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


def get_zip_name_candidates(stem: str) -> List[str]:
    """
    生成多个候选ZIP文件名（按优先级排序），用于自适应文件名匹配
    
    支持多种规范化策略的fallback机制，确保能够匹配DataWeave中的ZIP文件
    
    例如：1202_111045_111345_1_rere_1 →
    1. 1202_111045_111345_1.zip (移除_rere_1)
    2. 1202_111045_111345.zip (移除_1_rere_1)
    3. 1202_111045_111345_1_rere_1.zip (原始名称，兜底)
    
    Args:
        stem: 原始文件名（不含扩展名）
    
    Returns:
        候选文件名列表（按优先级排序）
    """
    candidates = []
    seen = set()  # 用于去重
    
    # 策略1: 只移除 _rere_数字 后缀
    candidate1 = re.sub(r'_rere_\d+$', '', stem)
    if candidate1 not in seen:
        candidates.append(f"{candidate1}.zip")
        seen.add(candidate1)
    
    # 策略2: 移除 _单/双位数字_rere_数字 后缀（如 _1_rere_1）
    # 限制为1-2位数字，避免匹配时间戳部分
    candidate2 = re.sub(r'_\d{1,2}_rere_\d+$', '', stem)
    if candidate2 not in seen and candidate2 != stem:
        candidates.append(f"{candidate2}.zip")
        seen.add(candidate2)
    
    # 策略3: 原始名称作为兜底
    if stem not in seen:
        candidates.append(f"{stem}.zip")
    
    return candidates
