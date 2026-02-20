# -*- coding: utf-8 -*-
"""
市场基类
"""
from abc import ABC, abstractmethod
from typing import Tuple, Optional
import pandas as pd
from ..cache import Cache
from ..data_sources.base import DataSource

# 尝试导入持久化缓存（可选依赖）
try:
    from ..cache.persistent import PersistentCache
except ImportError:
    PersistentCache = None


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
    
    def get_price(self, symbol: str, sdate: str = '', edate: str = '', 
                  freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """
        获取价格数据（模板方法，统一处理缓存逻辑）
        
        子类可以重写此方法以处理特殊情况，但建议调用 super().get_price() 来使用缓存功能
        或者实现 _fetch_price_data 方法，让基类自动处理缓存
        """
        symbol = self.normalize_symbol(symbol)
        cache_key = f"{symbol}:{sdate}:{edate}:{freq}:{days}:{fq}"
        
        # 如果是持久化缓存且是日级别数据，使用智能扩展逻辑
        if PersistentCache and isinstance(self.cache, PersistentCache) and freq == 'day':
            return self._get_price_with_persistent_cache(
                symbol, sdate, edate, freq, days, fq,
                lambda s, sd, ed, f, d, fq_param: self._fetch_price_data(s, sd, ed, f, d, fq_param)
            )
        
        # 普通缓存逻辑
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        # 从数据源获取
        result = self._fetch_price_data(symbol, sdate, edate, freq, days, fq)
        self._put_cache(cache_key, result)
        return result
    
    def _fetch_price_data(self, symbol: str, sdate: str = '', edate: str = '', 
                          freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """
        从数据源获取价格数据（子类需要实现）
        
        这个方法只负责获取数据，不处理缓存。缓存逻辑由 get_price 统一处理。
        
        Args:
            symbol: 股票代码（已标准化）
            sdate: 开始日期
            edate: 结束日期
            freq: 频率
            days: 天数
            fq: 复权方式
        
        Returns:
            (symbol, name, DataFrame)
        """
        # 默认实现：子类应该重写此方法
        return (symbol, '', pd.DataFrame())
    
    @abstractmethod
    def normalize_symbol(self, symbol: str) -> str:
        """标准化股票代码"""
        pass
    
    def _get_cached(self, key: str, sdate: str = '', edate: str = '') -> Optional[Tuple[str, str, pd.DataFrame]]:
        """从缓存获取数据"""
        if self.cache:
            # 如果是 PersistentCache，使用 base_key + 日期参数的方式
            if PersistentCache and isinstance(self.cache, PersistentCache):
                # 从完整 key 中提取 base_key
                parts = key.split(':')
                if len(parts) == 3:
                    # 已经是 base_key 格式：symbol:freq:fq
                    base_key = key
                    cached = self.cache.get(base_key, sdate=sdate, edate=edate)
                elif len(parts) >= 6:
                    # 完整 key 格式：symbol:sdate:edate:freq:days:fq
                    symbol = parts[0]
                    freq = parts[3]
                    fq = parts[5]
                    base_key = f"{symbol}:{freq}:{fq}"
                    cached = self.cache.get(base_key, sdate=sdate, edate=edate)
                else:
                    cached = self.cache.get(key)
            else:
                cached = self.cache.get(key)
            if cached:
                return cached
        return None
    
    def _put_cache(self, key: str, value: Tuple[str, str, pd.DataFrame]) -> None:
        """存入缓存"""
        if self.cache:
            # 如果是 PersistentCache，使用 base_key 存储
            if PersistentCache and isinstance(self.cache, PersistentCache):
                # 从完整 key 中提取 base_key
                parts = key.split(':')
                if len(parts) == 3:
                    # 已经是 base_key 格式：symbol:freq:fq
                    base_key = key
                    self.cache.put(base_key, value)
                elif len(parts) >= 6:
                    # 完整 key 格式：symbol:sdate:edate:freq:days:fq
                    symbol = parts[0]
                    freq = parts[3]
                    fq = parts[5]
                    base_key = f"{symbol}:{freq}:{fq}"
                    self.cache.put(base_key, value)
                else:
                    self.cache.put(key, value)
            else:
                self.cache.put(key, value)
    
    def _get_price_with_persistent_cache(self, symbol: str, sdate: str, edate: str,
                                          freq: str, days: int, fq: str,
                                          fetch_func) -> Tuple[str, str, pd.DataFrame]:
        """
        使用持久化缓存的自动扩展逻辑。
        扩展判定与融合写回由 cache 模块完成，market 仅负责提供 fetch_func。
        """
        if self.cache and hasattr(self.cache, "get_price_auto_merge"):
            return self.cache.get_price_auto_merge(
                symbol=symbol,
                sdate=sdate,
                edate=edate,
                freq=freq,
                days=days,
                fq=fq,
                fetch_func=fetch_func,
            )

        cache_key = f"{symbol}:{sdate}:{edate}:{freq}:{days}:{fq}"
        cached = self._get_cached(cache_key, sdate=sdate, edate=edate)
        if cached:
            return cached
        result = fetch_func(symbol, sdate, edate, freq, days, fq)
        self._put_cache(cache_key, result)
        return result

