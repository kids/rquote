# -*- coding: utf-8 -*-
"""
辅助工具函数
"""
import json
from .web import hget


def load_js_var_json(url: str):
    a = hget(url)
    if a:
        a = json.loads(a.text.split('(')[1].split(')')[0])
    return a


def to_float(val, default=None):
    """安全转换为 float，失败返回 default。"""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default
