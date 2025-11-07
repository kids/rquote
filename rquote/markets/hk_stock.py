# -*- coding: utf-8 -*-
"""
港股市场
"""
import pandas as pd
from typing import Tuple
from .base import Market
from ..parsers import KlineParser
from ..exceptions import DataSourceError, ParseError
from ..utils import logger


class HKStockMarket(Market):
    """港股市场"""
    
    def normalize_symbol(self, symbol: str) -> str:
        """标准化港股代码"""
        if not symbol.startswith('hk'):
            return 'hk' + symbol
        return symbol
    
    def get_price(self, symbol: str, sdate: str = '', edate: str = '', 
                  freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """获取港股价格数据"""
        symbol = self.normalize_symbol(symbol)
        
        cache_key = f"{symbol}:{sdate}:{edate}:{freq}:{days}:{fq}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        try:
            raw_data = self.data_source.fetch_kline(
                symbol, freq=freq, sdate=sdate, edate=edate, days=days, fq=fq
            )
            
            parser = KlineParser()
            name, df = parser.parse_tencent_kline(raw_data, symbol)
            
            result = (symbol, name, df)
            self._put_cache(cache_key, result)
            return result
        except (DataSourceError, ParseError) as e:
            logger.warning(f'Failed to fetch {symbol}: {e}')
            raise

