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
        使用持久化缓存的智能扩展逻辑
        
        当请求的 edate 不在缓存中时，从缓存的最新日期向前扩展到 edate
        当请求的 sdate 不在缓存中时，从缓存的最早日期向后扩展到 sdate
        """
        cache_key = f"{symbol}:{sdate}:{edate}:{freq}:{days}:{fq}"
        
        logger.info(f"[PRICE GET] symbol={symbol}, sdate={sdate}, edate={edate}, freq={freq}, cache_key={cache_key}")
        
        # 尝试从缓存获取（传入日期参数，PersistentCache 会使用 base_key + 日期参数）
        cached = self._get_cached(cache_key, sdate=sdate, edate=edate)
        if cached:
            _, name, cached_df = cached
            logger.info(f"[PRICE CACHE HIT] symbol={symbol}, 缓存数据行数={len(cached_df)}, 日期范围={cached_df.index.min() if not cached_df.empty else 'N/A'} 到 {cached_df.index.max() if not cached_df.empty else 'N/A'}")
            
            # 检查是否需要扩展
            if cached_df.empty or not isinstance(cached_df.index, pd.DatetimeIndex):
                # 缓存为空或索引不是日期，直接获取新数据
                logger.info(f"[PRICE FETCH] 缓存数据无效，从网络获取 symbol={symbol}, sdate={sdate}, edate={edate}")
                result = fetch_func(symbol, sdate, edate, freq, days, fq)
                self._put_cache(cache_key, result)
                return result
            
            cached_earliest = cached_df.index.min()
            cached_latest = cached_df.index.max()
            request_sdate = pd.to_datetime(sdate) if sdate else None
            request_edate = pd.to_datetime(edate) if edate else None
            
            need_extend_forward = False  # 需要向前扩展（更新日期）
            need_extend_backward = False  # 需要向后扩展（更早日期）
            need_extend_for_length = False  # 需要扩展以满足长度要求（>=60行）
            extend_sdate = sdate
            extend_edate = edate
            
            # 逻辑1: 检查是否需要向前扩展（请求的 edate 晚于缓存的最新日期）
            if request_edate and request_edate > cached_latest:
                need_extend_forward = True
                # 从缓存的最新日期+1天开始，扩展到请求的 edate
                extend_sdate = (cached_latest + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                extend_edate = edate
            
            # 逻辑2: 如果从cache取的数据在edate前的长度小于等于60，则进行网络请求取数合并进cache
            elif request_edate:
                # 计算edate之前的数据行数
                data_before_edate = cached_df[cached_df.index <= request_edate]
                if len(data_before_edate) <= 60:
                    need_extend_for_length = True
                    # 从更早的日期开始获取，确保edate前有足够的数据（>=60行）
                    # 往前推约4个月（120天），确保有足够的交易日
                    target_sdate = request_edate - pd.Timedelta(days=120)
                    extend_sdate = target_sdate.strftime('%Y-%m-%d')
                    extend_edate = edate
                    logger.info(f"[PRICE EXTEND LENGTH] symbol={symbol}, edate前数据行数={len(data_before_edate)} <= 60, 从更早日期获取, extend_sdate={extend_sdate}")
            
            # 逻辑3: 如果cache中有数据，但新的edate小于cache中数据最小值
            elif request_edate and request_edate < cached_earliest:
                need_extend_backward = True
                # 从缓存最早日期开始往前获取，直到覆盖edate且edate前的长度大于60
                # 先尝试从edate往前推足够的天数（约4个月）
                target_sdate = request_edate - pd.Timedelta(days=120)
                extend_sdate = target_sdate.strftime('%Y-%m-%d')
                extend_edate = (cached_earliest - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"[PRICE EXTEND EARLY] symbol={symbol}, edate={request_edate} 早于缓存最早日期={cached_earliest}, 从更早日期获取, extend_sdate={extend_sdate}, extend_edate={extend_edate}")
            
            # 检查是否需要向后扩展（请求的 sdate 早于缓存的最早日期，且不是情况3）
            if request_sdate and request_sdate < cached_earliest and not need_extend_backward:
                need_extend_backward = True
                # 从请求的 sdate 开始，扩展到缓存的最早日期-1天
                extend_sdate = sdate
                extend_edate = (cached_earliest - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 如果需要扩展，获取缺失的数据
            if need_extend_forward or need_extend_backward or need_extend_for_length:
                logger.info(f"[PRICE EXTEND] 需要扩展数据, symbol={symbol}, extend_sdate={extend_sdate}, extend_edate={extend_edate}, need_forward={need_extend_forward}, need_backward={need_extend_backward}, need_length={need_extend_for_length}")
                
                # 对于逻辑2和逻辑3，可能需要循环获取直到满足条件
                max_iterations = 5  # 最多循环5次，避免无限循环
                iteration = 0
                current_merged_df = cached_df.copy()
                
                while iteration < max_iterations:
                    iteration += 1
                    # 获取扩展的数据
                    extended_result = fetch_func(symbol, extend_sdate, extend_edate, freq, days, fq)
                    _, _, extended_df = extended_result
                    logger.info(f"[PRICE FETCH] 从网络获取扩展数据 (迭代{iteration}), 数据行数={len(extended_df)}")
                    
                    if not extended_df.empty:
                        # 确保两个 DataFrame 的索引都是 DatetimeIndex
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
                        
                        # 合并数据
                        current_merged_df = pd.concat([current_merged_df, extended_df])
                        current_merged_df = current_merged_df[~current_merged_df.index.duplicated(keep='last')]
                        current_merged_df = current_merged_df.sort_index()
                        
                        # 检查是否满足条件（逻辑2和逻辑3需要检查长度）
                        if need_extend_for_length or need_extend_backward:
                            if request_edate:
                                data_before_edate = current_merged_df[current_merged_df.index <= request_edate]
                                if len(data_before_edate) > 60:
                                    # 满足条件，退出循环
                                    logger.info(f"[PRICE EXTEND] 已满足长度要求, edate前数据行数={len(data_before_edate)}")
                                    break
                                else:
                                    # 还需要继续获取更早的数据
                                    current_earliest = current_merged_df.index.min()
                                    if current_earliest <= pd.to_datetime(extend_sdate):
                                        # 已经获取到最早的数据，无法再往前获取
                                        logger.warning(f"[PRICE EXTEND] 已获取到最早数据，但edate前数据行数={len(data_before_edate)}仍不足60行")
                                        break
                                    # 继续往前推
                                    extend_sdate_dt = pd.to_datetime(extend_sdate) - pd.Timedelta(days=120)
                                    extend_sdate = extend_sdate_dt.strftime('%Y-%m-%d')
                                    logger.info(f"[PRICE EXTEND] 继续获取更早数据, 新extend_sdate={extend_sdate}")
                                    continue
                        
                        # 对于逻辑1（向前扩展），不需要循环，直接退出
                        if need_extend_forward and not need_extend_for_length and not need_extend_backward:
                            break
                    else:
                        # 获取失败，退出循环
                        logger.warning(f"[PRICE EXTEND] 获取数据为空，退出循环")
                        break
                
                # 过滤到请求的日期范围
                if request_sdate or request_edate:
                    if request_sdate and request_edate:
                        mask = (current_merged_df.index >= request_sdate) & (current_merged_df.index <= request_edate)
                    elif request_sdate:
                        mask = current_merged_df.index >= request_sdate
                    else:
                        mask = current_merged_df.index <= request_edate
                    current_merged_df = current_merged_df[mask]
                
                result = (symbol, name, current_merged_df)
                # 更新缓存（使用原始 key，PersistentCache 会智能合并）
                self._put_cache(cache_key, result)
                return result
            
            # 不需要扩展，直接返回缓存的数据
            # 注意：PersistentCache.get() 已经根据请求的日期范围进行了过滤，
            # 返回的数据已经是过滤后的，不需要再次过滤
            logger.info(f"[PRICE RETURN] 直接返回缓存数据, symbol={symbol}, 数据行数={len(cached_df)}")
            return (symbol, name, cached_df)
        
        # 缓存未命中，直接获取
        if fetch_func:
            logger.info(f"[PRICE FETCH] 缓存未命中，从网络获取 symbol={symbol}, sdate={sdate}, edate={edate}")
            result = fetch_func(symbol, sdate, edate, freq, days, fq)
            _, _, df = result
            logger.info(f"[PRICE FETCH] 网络获取完成, 数据行数={len(df)}, 准备存储到缓存")
            self._put_cache(cache_key, result)
            return result
        else:
            # 如果没有提供 fetch_func，返回空数据
            return (symbol, '', pd.DataFrame())

