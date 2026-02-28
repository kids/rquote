# -*- coding: utf-8 -*-
"""
价格相关API
"""
from typing import Tuple, Union, List, Dict
from ..markets import MarketFactory
from ..cache import Cache
from ..cache.memory import DictCache as DictCacheAdapter
from ..utils.date import check_date_format, DateRangeUtils
from ..exceptions import SymbolError


def _records_to_dataframe(records: List[Dict]):
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas is required for as_dataframe=True: pip install rquote[dataframe]")
    if not records:
        return pd.DataFrame()
    date_key = DateRangeUtils.detect_date_key(records)
    df = pd.DataFrame(records)
    if date_key in df.columns:
        df = df.set_index(date_key)
        try:
            df.index = pd.to_datetime(df.index)
        except (ValueError, TypeError):
            pass
    return df


def get_price(i: str, sdate: str = '', edate: str = '', freq: str = 'day',
              days: int = 320, fq: str = 'qfq', dd=None, as_dataframe: bool = False):
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
        as_dataframe: 为 True 时将结果转为 pd.DataFrame（需要安装 pandas）

    Returns:
        as_dataframe=False: (symbol, name, list[dict])
        as_dataframe=True:  (symbol, name, pd.DataFrame)
    '''
    cache = None
    if dd is not None:
        if isinstance(dd, dict):
            cache = DictCacheAdapter(dd)
        elif isinstance(dd, Cache):
            cache = dd
        elif hasattr(dd, 'get'):
            cache = DictCacheAdapter(dd)

    try:
        sdate = check_date_format(sdate) if sdate else ''
        edate = check_date_format(edate) if edate else ''
    except ValueError as e:
        raise SymbolError(f"Invalid date format: {e}")

    market = MarketFactory.create_from_symbol(i, cache=cache)
    symbol, name, records = market.get_price(i, sdate=sdate, edate=edate, freq=freq, days=days, fq=fq)

    if as_dataframe:
        return symbol, name, _records_to_dataframe(records)
    return symbol, name, records


def get_price_longer(i: str, l: int = 2, edate: str = '', freq: str = 'day',
                     fq: str = 'qfq', dd=None, as_dataframe: bool = False):
    """
    获取更长时间的历史数据

    Args:
        i: 股票代码
        l: 年数
        edate: 结束日期
        freq: 频率
        fq: 复权方式
        dd: 缓存对象
        as_dataframe: 为 True 时将结果转为 pd.DataFrame

    Returns:
        as_dataframe=False: (symbol, name, list[dict])
        as_dataframe=True:  (symbol, name, pd.DataFrame)
    """
    _, name, records = get_price(i, edate=edate, freq=freq, fq=fq, dd=dd)

    for y in range(1, l):
        date_key = DateRangeUtils.detect_date_key(records)
        earliest_str, _ = DateRangeUtils.get_date_range(records, date_key)
        if not earliest_str:
            break
        # Convert to YYYYMMDD for edate param
        d1 = earliest_str.replace('-', '')
        older = get_price(i, edate=d1, freq=freq, fq=fq, dd=dd)[2]
        records = DateRangeUtils.merge_records(older, records, date_key)

    if as_dataframe:
        return i, name, _records_to_dataframe(records)
    return i, name, records


if __name__ == '__main__':
    i = 'usTSLA.N'
    a = get_price(i, freq='min')
    print(a)
