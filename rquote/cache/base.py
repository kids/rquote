# -*- coding: utf-8 -*-
"""
缓存基类
"""
from abc import ABC, abstractmethod
from typing import Optional, Any


class Cache(ABC):
    """缓存基类"""
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        pass
    
    @abstractmethod
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存"""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> None:
        """删除缓存"""
        pass

