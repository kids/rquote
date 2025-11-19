# -*- coding: utf-8 -*-
"""
缓存模块
"""
from .base import Cache
from .memory import MemoryCache

# 尝试导入持久化缓存（可选依赖）
try:
    from .persistent import PersistentCache
    __all__ = ['Cache', 'MemoryCache', 'PersistentCache']
except ImportError:
    __all__ = ['Cache', 'MemoryCache']

