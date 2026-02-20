# -*- coding: utf-8 -*-
"""
缓存模块
"""
from .base import Cache
from .memory import MemoryCache

# 尝试导入持久化缓存与工厂（可选依赖）
try:
    from .persistent import PersistentCache, MarketPersistentCache, create_persistent_cache
    from . import storage
    __all__ = [
        'Cache',
        'MemoryCache',
        'PersistentCache',
        'MarketPersistentCache',
        'create_persistent_cache',
        'storage',
    ]
except ImportError:
    __all__ = ['Cache', 'MemoryCache']

