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


def _normalize_dataframe_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    统一处理 DataFrame 索引，转换为 DatetimeIndex
    
    Args:
        df: 输入的 DataFrame
    
    Returns:
        处理后的 DataFrame，索引为 DatetimeIndex
    """
    if df.empty:
        return df
    
    # 如果已经是 DatetimeIndex，直接返回
    if isinstance(df.index, pd.DatetimeIndex):
        return df
    
    # 尝试转换为 DatetimeIndex
    try:
        df.index = pd.to_datetime(df.index)
    except (ValueError, TypeError) as e:
        # 如果转换失败，保持原样（可能是其他类型的索引）
        pass
    
    return df


def get_price(i: str, sdate: str = '', edate: str = '', freq: str = 'day', 
              days: int = 320, fq: str = 'qfq', dd=None) -> Tuple[str, str, pd.DataFrame]:
    '''
    获取价格数据
    
    Args:
        i: 股票代码
        sdate: 开始日期
        edate: 结束日期
        freq: 频率，默认'day' (日线)，可选：'week', 'month', 'min'
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
    symbol, name, df = market.get_price(i, sdate=sdate, edate=edate, freq=freq, days=days, fq=fq)
    
    # 统一后处理：转换索引为 DatetimeIndex
    df = _normalize_dataframe_index(df)
    
    return symbol, name, df


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
    # 使用 DatetimeIndex 的格式化方法（get_price 已统一转换为 DatetimeIndex）
    if isinstance(a.index, pd.DatetimeIndex) and len(a.index) > 0:
        d1 = a.index[0].strftime('%Y%m%d')
    else:
        # 降级处理：如果索引不是 DatetimeIndex（理论上不应该发生），尝试格式化
        try:
            d1 = str(a.index[0])[:8] if len(str(a.index[0])) >= 8 else str(a.index[0])
        except:
            d1 = a.index.format()[0] if hasattr(a.index, 'format') else str(a.index[0])
    
    for y in range(1, l):
        d0 = str(int(d1[:4]) - 1) + d1[4:]
        a = pd.concat((get_price(i, d0, d1, dd=dd)[2], a), 0).drop_duplicates()
        d1 = d0
    return i, name, a


if __name__ == '__main__':
    i = 'usTSLA.N'
    a = get_price(i, freq='min')
    print(a)