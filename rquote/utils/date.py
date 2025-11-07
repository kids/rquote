# -*- coding: utf-8 -*-
"""
日期工具模块
"""
import time
import re
from typing import Optional


def check_date_format(date_str: str) -> str:
    """
    检查并标准化日期格式
    
    Args:
        date_str: 日期字符串
    
    Returns:
        标准化后的日期字符串（格式：YYYY-MM-DD）
    
    Raises:
        ValueError: 日期格式无法识别
    """
    if not date_str:
        return ''
    
    # 允许格式: YYYY-MM-DD
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    
    # 尝试转换其他格式
    formats = ["%Y/%m/%d", "%Y%m%d", "%Y.%m.%d", "%Y_%m_%d", "%Y-%m-%d"]
    for fmt in formats:
        try:
            t_struct = time.strptime(date_str, fmt)
            return time.strftime("%Y-%m-%d", t_struct)
        except ValueError:
            continue
    
    raise ValueError(f"date format not recognized: {date_str}")

