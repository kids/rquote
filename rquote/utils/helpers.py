# -*- coding: utf-8 -*-
"""
辅助工具函数
"""
import json
from .web import hget


def load_js_var_json(url: str):
    """
    加载JavaScript变量中的JSON数据
    
    Args:
        url: 请求URL
    
    Returns:
        JSON数据
    """
    a = hget(url)
    if a:
        a = json.loads(a.text.split('(')[1].split(')')[0])
    return a

