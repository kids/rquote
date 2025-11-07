# -*- coding: utf-8 -*-
"""
市场基类
"""
from abc import ABC, abstractmethod
from typing import Tuple, Optional
import pandas as pd
from ..cache import Cache
from ..data_sources.base import DataSource


class Market(ABC):
    """市场基类"""
    
    def __init__(self, data_source: DataSource, cache: Optional[Cache] = None):
        """
        初始化市场
        
        Args:
            data_source: 数据源
            cache: 缓存对象
        """
        self.data_source = data_source
        self.cache = cache
    
    @abstractmethod
    def get_price(self, symbol: str, sdate: str = '', edate: str = '', 
                  freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """获取价格数据"""
        pass
    
    @abstractmethod
    def normalize_symbol(self, symbol: str) -> str:
        """标准化股票代码"""
        pass
    
    def _get_cached(self, key: str) -> Optional[Tuple[str, str, pd.DataFrame]]:
        """从缓存获取数据"""
        if self.cache:
            cached = self.cache.get(key)
            if cached:
                return cached
        return None
    
    def _put_cache(self, key: str, value: Tuple[str, str, pd.DataFrame]) -> None:
        """存入缓存"""
        if self.cache:
            self.cache.put(key, value)

