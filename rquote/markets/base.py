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

# 导入日志
try:
    from ..utils.logging import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

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
        使用持久化缓存的智能扩展逻辑（不论 backend）。
        - 数据不足（需更早数据）：不传 sdate，用当前数据集中最早的日期做 edate 请求，拼合更新，循环直到数据充足。
        - 所有数据都在请求日期之前：用当前日期做 edate 请求，从缓存最新+1 往前拉直到与已有数据连上，拼合更新。
        """
        # 认为「数据充足」时，请求 edate 之前至少需要的行数
        MIN_ROWS_BEFORE_EDATE = 60
        MAX_EXTEND_ITERATIONS = 15

        cache_key = f"{symbol}:{sdate}:{edate}:{freq}:{days}:{fq}"
        logger.info(f"[PRICE GET] symbol={symbol}, sdate={sdate}, edate={edate}, freq={freq}, cache_key={cache_key}")

        cached = self._get_cached(cache_key, sdate=sdate, edate=edate)
        if cached:
            _, name, cached_df = cached
            logger.info(f"[PRICE CACHE HIT] symbol={symbol}, 缓存数据行数={len(cached_df)}, 日期范围={cached_df.index.min() if not cached_df.empty else 'N/A'} 到 {cached_df.index.max() if not cached_df.empty else 'N/A'}")

            if cached_df.empty or not isinstance(cached_df.index, pd.DatetimeIndex):
                logger.info(f"[PRICE FETCH] 缓存数据无效，从网络获取 symbol={symbol}, sdate={sdate}, edate={edate}")
                result = fetch_func(symbol, sdate, edate, freq, days, fq)
                self._put_cache(cache_key, result)
                return result

            cached_earliest = cached_df.index.min()
            cached_latest = cached_df.index.max()
            request_sdate = pd.to_datetime(sdate) if sdate else None
            request_edate = pd.to_datetime(edate) if edate else None
            data_before_edate = cached_df[cached_df.index <= request_edate] if request_edate else pd.DataFrame()

            need_forward = request_edate is not None and request_edate > cached_latest
            need_backward = (
                (request_edate is not None and request_edate < cached_earliest)
                or (request_edate is not None and len(data_before_edate) <= MIN_ROWS_BEFORE_EDATE)
            )

            if need_forward:
                # 所有数据都在请求日期之前：用当前日期做 edate，从缓存最新+1 请求直到与已有数据连上
                today_str = pd.Timestamp.now().strftime("%Y-%m-%d")
                current_merged_df = cached_df.copy()
                for _ in range(MAX_EXTEND_ITERATIONS):
                    extend_sdate = (current_merged_df.index.max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                    extend_edate = today_str
                    logger.info(f"[PRICE EXTEND FORWARD] symbol={symbol}, extend_sdate={extend_sdate}, extend_edate={extend_edate}")
                    extended_result = fetch_func(symbol, extend_sdate, extend_edate, freq, days, fq)
                    _, _, extended_df = extended_result
                    if extended_df.empty:
                        logger.info(f"[PRICE EXTEND FORWARD] 获取数据为空，停止向前扩展")
                        break
                    if not isinstance(current_merged_df.index, pd.DatetimeIndex):
                        try:
                            current_merged_df.index = pd.to_datetime(current_merged_df.index)
                        except (ValueError, TypeError):
                            pass
                    if not isinstance(extended_df.index, pd.DatetimeIndex):
                        try:
                            extended_df.index = pd.to_datetime(extended_df.index)
                        except (ValueError, TypeError):
                            pass
                    current_merged_df = pd.concat([current_merged_df, extended_df])
                    current_merged_df = current_merged_df[~current_merged_df.index.duplicated(keep="last")]
                    current_merged_df = current_merged_df.sort_index()
                    if current_merged_df.index.max() >= request_edate:
                        logger.info(f"[PRICE EXTEND FORWARD] 已覆盖请求日期, 最新={current_merged_df.index.max()}")
                        break
                if request_sdate is not None or request_edate is not None:
                    if request_sdate is not None and request_edate is not None:
                        mask = (current_merged_df.index >= request_sdate) & (current_merged_df.index <= request_edate)
                    elif request_sdate is not None:
                        mask = current_merged_df.index >= request_sdate
                    else:
                        mask = current_merged_df.index <= request_edate
                    current_merged_df = current_merged_df[mask]
                result = (symbol, name, current_merged_df)
                self._put_cache(cache_key, result)
                return result

            if need_backward:
                # 数据不足：不传 sdate，用当前数据集中最早的日期做 edate 请求，拼合更新直到数据充足
                current_merged_df = cached_df.copy()
                for _ in range(MAX_EXTEND_ITERATIONS):
                    earliest = current_merged_df.index.min()
                    extend_edate = (earliest - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                    extend_sdate = ""
                    logger.info(f"[PRICE EXTEND BACKWARD] symbol={symbol}, extend_edate={extend_edate} (当前最早={earliest}), 不传 sdate")
                    extended_result = fetch_func(symbol, extend_sdate, extend_edate, freq, days, fq)
                    _, _, extended_df = extended_result
                    if extended_df.empty:
                        logger.info(f"[PRICE EXTEND BACKWARD] 获取数据为空，停止向后扩展")
                        break
                    if not isinstance(current_merged_df.index, pd.DatetimeIndex):
                        try:
                            current_merged_df.index = pd.to_datetime(current_merged_df.index)
                        except (ValueError, TypeError):
                            pass
                    if not isinstance(extended_df.index, pd.DatetimeIndex):
                        try:
                            extended_df.index = pd.to_datetime(extended_df.index)
                        except (ValueError, TypeError):
                            pass
                    current_merged_df = pd.concat([current_merged_df, extended_df])
                    current_merged_df = current_merged_df[~current_merged_df.index.duplicated(keep="last")]
                    current_merged_df = current_merged_df.sort_index()
                    if request_edate is not None:
                        data_before = current_merged_df[current_merged_df.index <= request_edate]
                        if len(data_before) > MIN_ROWS_BEFORE_EDATE and current_merged_df.index.min() <= request_edate:
                            logger.info(f"[PRICE EXTEND BACKWARD] 已满足数据充足, edate 前行数={len(data_before)}")
                            break
                if request_sdate is not None or request_edate is not None:
                    if request_sdate is not None and request_edate is not None:
                        mask = (current_merged_df.index >= request_sdate) & (current_merged_df.index <= request_edate)
                    elif request_sdate is not None:
                        mask = current_merged_df.index >= request_sdate
                    else:
                        mask = current_merged_df.index <= request_edate
                    current_merged_df = current_merged_df[mask]
                result = (symbol, name, current_merged_df)
                self._put_cache(cache_key, result)
                return result

            logger.info(f"[PRICE RETURN] 直接返回缓存数据, symbol={symbol}, 数据行数={len(cached_df)}")
            return (symbol, name, cached_df)

        if fetch_func:
            logger.info(f"[PRICE FETCH] 缓存未命中，从网络获取 symbol={symbol}, sdate={sdate}, edate={edate}")
            result = fetch_func(symbol, sdate, edate, freq, days, fq)
            _, _, df = result
            logger.info(f"[PRICE FETCH] 网络获取完成, 数据行数={len(df)}, 准备存储到缓存")
            self._put_cache(cache_key, result)
            return result
        return (symbol, '', pd.DataFrame())

