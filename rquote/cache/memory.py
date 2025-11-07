# -*- coding: utf-8 -*-
"""
内存缓存实现
"""
import time
from typing import Optional, Any, Dict
from .base import Cache


class MemoryCache(Cache):
    """内存缓存实现"""
    
    def __init__(self, ttl: int = 3600):
        """
        初始化内存缓存
        
        Args:
            ttl: 默认过期时间（秒）
        """
        self.ttl = ttl
        self._cache: Dict[str, tuple] = {}  # {key: (value, expire_time)}
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        if key not in self._cache:
            return None
        
        value, expire_time = self._cache[key]
        if time.time() > expire_time:
            del self._cache[key]
            return None
        
        return value
    
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存"""
        ttl = ttl or self.ttl
        expire_time = time.time() + ttl
        self._cache[key] = (value, expire_time)
    
    def delete(self, key: str) -> None:
        """删除缓存"""
        self._cache.pop(key, None)
    
    def clear(self) -> None:
        """清空缓存"""
        self._cache.clear()
    
    def size(self) -> int:
        """获取缓存大小"""
        return len(self._cache)


class DictCache(Cache):
    """字典缓存适配器（用于向后兼容旧的dd参数）"""
    
    def __init__(self, cache_dict: dict):
        """
        初始化字典缓存
        
        Args:
            cache_dict: 字典对象
        """
        self._dict = cache_dict
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        return self._dict.get(key)
    
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存"""
        self._dict[key] = value
    
    def delete(self, key: str) -> None:
        """删除缓存"""
        self._dict.pop(key, None)

