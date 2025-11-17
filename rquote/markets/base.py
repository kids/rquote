# -*- coding: utf-8 -*-
"""
市场基类
"""
from abc import ABC, abstractmethod
from typing import Tuple, Optional
import pandas as pd
from datetime import datetime, timedelta
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
    
    def _get_price_with_persistent_cache(self, symbol: str, sdate: str, edate: str,
                                          freq: str, days: int, fq: str,
                                          fetch_func) -> Tuple[str, str, pd.DataFrame]:
        """
        使用持久化缓存的智能扩展逻辑
        
        当请求的 edate 不在缓存中时，从缓存的最新日期向前扩展到 edate
        当请求的 sdate 不在缓存中时，从缓存的最早日期向后扩展到 sdate
        """
        cache_key = f"{symbol}:{sdate}:{edate}:{freq}:{days}:{fq}"
        
        # 尝试从缓存获取
        cached = self._get_cached(cache_key)
        if cached:
            _, name, cached_df = cached
            
            # 检查是否需要扩展
            if cached_df.empty or not isinstance(cached_df.index, pd.DatetimeIndex):
                # 缓存为空或索引不是日期，直接获取新数据
                result = fetch_func(symbol, sdate, edate, freq, days, fq)
                self._put_cache(cache_key, result)
                return result
            
            cached_earliest = cached_df.index.min()
            cached_latest = cached_df.index.max()
            request_sdate = pd.to_datetime(sdate) if sdate else None
            request_edate = pd.to_datetime(edate) if edate else None
            
            need_extend_forward = False  # 需要向前扩展（更新日期）
            need_extend_backward = False  # 需要向后扩展（更早日期）
            extend_sdate = sdate
            extend_edate = edate
            
            # 检查是否需要向前扩展
            if request_edate and request_edate > cached_latest:
                need_extend_forward = True
                # 从缓存的最新日期+1天开始，扩展到请求的 edate
                extend_sdate = (cached_latest + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                extend_edate = edate
            
            # 检查是否需要向后扩展
            if request_sdate and request_sdate < cached_earliest:
                need_extend_backward = True
                # 从请求的 sdate 开始，扩展到缓存的最早日期-1天
                extend_sdate = sdate
                extend_edate = (cached_earliest - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 如果需要扩展，获取缺失的数据
            if need_extend_forward or need_extend_backward:
                # 获取扩展的数据
                extended_result = fetch_func(symbol, extend_sdate, extend_edate, freq, days, fq)
                _, _, extended_df = extended_result
                
                if not extended_df.empty:
                    # 合并数据
                    merged_df = pd.concat([cached_df, extended_df])
                    merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
                    merged_df = merged_df.sort_index()
                    
                    # 过滤到请求的日期范围
                    if request_sdate or request_edate:
                        if request_sdate and request_edate:
                            mask = (merged_df.index >= request_sdate) & (merged_df.index <= request_edate)
                        elif request_sdate:
                            mask = merged_df.index >= request_sdate
                        else:
                            mask = merged_df.index <= request_edate
                        merged_df = merged_df[mask]
                    
                    result = (symbol, name, merged_df)
                    # 更新缓存（使用原始 key，PersistentCache 会智能合并）
                    self._put_cache(cache_key, result)
                    return result
            
            # 不需要扩展，直接返回缓存的数据
            # 过滤到请求的日期范围
            if request_sdate or request_edate:
                if request_sdate and request_edate:
                    mask = (cached_df.index >= request_sdate) & (cached_df.index <= request_edate)
                elif request_sdate:
                    mask = cached_df.index >= request_sdate
                else:
                    mask = cached_df.index <= request_edate
                filtered_df = cached_df[mask]
                return (symbol, name, filtered_df)
            
            return (symbol, name, cached_df)
        
        # 缓存未命中，直接获取
        if fetch_func:
            result = fetch_func(symbol, sdate, edate, freq, days, fq)
            self._put_cache(cache_key, result)
            return result
        else:
            # 如果没有提供 fetch_func，返回空数据
            return (symbol, '', pd.DataFrame())

