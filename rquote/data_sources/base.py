# -*- coding: utf-8 -*-
"""
数据源基类
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List


class DataSource(ABC):
    """数据源基类"""
    
    @abstractmethod
    def fetch_kline(self, symbol: str, **kwargs) -> Dict[str, Any]:
        """获取K线数据"""
        pass
    
    @abstractmethod
    def fetch_tick(self, symbols: List[str]) -> Dict[str, Any]:
        """获取实时行情"""
        pass

