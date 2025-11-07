# -*- coding: utf-8 -*-
"""
市场工厂
"""
from typing import Optional
from .base import Market
from .cn_stock import CNStockMarket
from .hk_stock import HKStockMarket
from .us_stock import USStockMarket
from .future import FutureMarket
from ..data_sources import TencentDataSource, SinaDataSource
from ..cache import Cache, MemoryCache
from ..config import default_config


class MarketFactory:
    """市场工厂类"""
    
    @staticmethod
    def create_from_symbol(symbol: str, cache: Optional[Cache] = None) -> Market:
        """
        根据股票代码创建对应的市场实例
        
        Args:
            symbol: 股票代码
            cache: 缓存对象，如果为None且配置启用缓存，则创建MemoryCache
        
        Returns:
            Market实例
        """
        # 处理缓存
        if cache is None and default_config.cache_enabled:
            cache = MemoryCache(ttl=default_config.cache_ttl)
        
        # 标准化代码
        if symbol[0] in ['0', '1', '3', '5', '6']:
            symbol = 'sh' + symbol if symbol[0] in ['5', '6'] else 'sz' + symbol
        
        # 根据前缀选择市场
        if symbol[:2] == 'BK':
            # 板块，使用A股市场处理
            return CNStockMarket(TencentDataSource(), cache)
        elif symbol[:2] == 'fu':
            return FutureMarket(SinaDataSource(), cache)
        elif symbol[:2] == 'pt':
            # PT代码，使用A股市场处理
            return CNStockMarket(TencentDataSource(), cache)
        elif symbol[:2] in ['sh', 'sz']:
            return CNStockMarket(TencentDataSource(), cache)
        elif symbol[:2] == 'hk':
            return HKStockMarket(TencentDataSource(), cache)
        elif symbol[:2] == 'us':
            return USStockMarket(TencentDataSource(), cache)
        else:
            raise ValueError(f'Unsupported symbol format: {symbol}')
    
    @staticmethod
    def create(market_type: str, cache: Optional[Cache] = None) -> Market:
        """
        根据市场类型创建市场实例
        
        Args:
            market_type: 市场类型 ('cn_stock', 'hk_stock', 'us_stock', 'future')
            cache: 缓存对象
        
        Returns:
            Market实例
        """
        if cache is None and default_config.cache_enabled:
            cache = MemoryCache(ttl=default_config.cache_ttl)
        
        if market_type == 'cn_stock':
            return CNStockMarket(TencentDataSource(), cache)
        elif market_type == 'hk_stock':
            return HKStockMarket(TencentDataSource(), cache)
        elif market_type == 'us_stock':
            return USStockMarket(TencentDataSource(), cache)
        elif market_type == 'future':
            return FutureMarket(SinaDataSource(), cache)
        else:
            raise ValueError(f'Unsupported market type: {market_type}')

