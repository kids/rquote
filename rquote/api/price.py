# -*- coding: utf-8 -*-
"""
价格相关API
"""
from typing import Tuple
import pandas as pd
from ..markets import MarketFactory
from ..cache import Cache
from ..cache.memory import DictCache as DictCacheAdapter
from ..utils.date import check_date_format
from ..exceptions import SymbolError


def get_price(i: str, sdate: str = '', edate: str = '', freq: str = 'day', 
              days: int = 320, fq: str = 'qfq', dd=None) -> Tuple[str, str, pd.DataFrame]:
    '''
    获取价格数据
    
    Args:
        i: 股票代码
        sdate: 开始日期
        edate: 结束日期
        dd: data dictionary或Cache对象，任何有get/put方法的本地缓存
        days: 获取天数，覆盖sdate
        fq: 复权方式，qfq为前复权
    
    Returns:
        (symbol, name, DataFrame)
    '''
    # 处理缓存
    cache = None
    if dd is not None:
        if isinstance(dd, dict):
            cache = DictCacheAdapter(dd)
        elif isinstance(dd, Cache):
            cache = dd
        elif hasattr(dd, 'get'):
            cache = DictCacheAdapter(dd)
    
    # 检查日期格式
    try:
        sdate = check_date_format(sdate) if sdate else ''
        edate = check_date_format(edate) if edate else ''
    except ValueError as e:
        raise SymbolError(f"Invalid date format: {e}")
    
    # 使用市场工厂创建对应的市场实例
    market = MarketFactory.create_from_symbol(i, cache=cache)
    
    # 调用市场的get_price方法
    return market.get_price(i, sdate=sdate, edate=edate, freq=freq, days=days, fq=fq)


def get_price_longer(i: str, l: int = 2, dd=None) -> Tuple[str, str, pd.DataFrame]:
    """
    获取更长时间的历史数据
    
    Args:
        i: 股票代码
        l: 年数
        dd: 缓存对象
    
    Returns:
        (symbol, name, DataFrame)
    """
    _, name, a = get_price(i, dd=dd)
    d1 = a.index.format()[0]
    for y in range(1, l):
        d0 = str(int(d1[:4]) - 1) + d1[4:]
        a = pd.concat((get_price(i, d0, d1, dd=dd)[2], a), 0).drop_duplicates()
        d1 = d0
    return i, name, a

