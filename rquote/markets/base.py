# -*- coding: utf-8 -*-
"""
市场基类
"""
from abc import ABC, abstractmethod
from typing import Tuple, Optional, List, Dict
from ..cache import Cache
from ..data_sources.base import DataSource

try:
    from ..cache.persistent import PersistentCache
except ImportError:
    PersistentCache = None


class Market(ABC):
    """市场基类"""

    def __init__(self, data_source: DataSource, cache: Optional[Cache] = None):
        self.data_source = data_source
        self.cache = cache

    def get_price(self, symbol: str, sdate: str = '', edate: str = '',
                  freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, List[Dict]]:
        symbol = self.normalize_symbol(symbol)
        cache_key = f"{symbol}:{sdate}:{edate}:{freq}:{days}:{fq}"

        if PersistentCache and isinstance(self.cache, PersistentCache) and freq == 'day':
            return self._get_price_with_persistent_cache(
                symbol, sdate, edate, freq, days, fq,
                lambda s, sd, ed, f, d, fq_param: self._fetch_price_data(s, sd, ed, f, d, fq_param)
            )

        cached = self._get_cached(cache_key)
        if cached:
            return cached

        result = self._fetch_price_data(symbol, sdate, edate, freq, days, fq)
        self._put_cache(cache_key, result)
        return result

    def _fetch_price_data(self, symbol: str, sdate: str = '', edate: str = '',
                          freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, List[Dict]]:
        return (symbol, '', [])

    @abstractmethod
    def normalize_symbol(self, symbol: str) -> str:
        pass

    def _get_cached(self, key: str, sdate: str = '', edate: str = '') -> Optional[Tuple[str, str, List[Dict]]]:
        if self.cache:
            if PersistentCache and isinstance(self.cache, PersistentCache):
                parts = key.split(':')
                if len(parts) == 3:
                    base_key = key
                    cached = self.cache.get(base_key, sdate=sdate, edate=edate)
                elif len(parts) >= 6:
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

    def _put_cache(self, key: str, value: Tuple[str, str, List[Dict]]) -> None:
        if self.cache:
            if PersistentCache and isinstance(self.cache, PersistentCache):
                parts = key.split(':')
                if len(parts) == 3:
                    base_key = key
                    self.cache.put(base_key, value)
                elif len(parts) >= 6:
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
                                          fetch_func) -> Tuple[str, str, List[Dict]]:
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
