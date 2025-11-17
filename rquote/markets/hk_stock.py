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
    
    def _fetch_price_data(self, symbol: str, sdate: str = '', edate: str = '', 
                          freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """从数据源获取港股价格数据"""
        try:
            raw_data = self.data_source.fetch_kline(
                symbol, freq=freq, sdate=sdate, edate=edate, days=days, fq=fq
            )
            
            parser = KlineParser()
            name, df = parser.parse_tencent_kline(raw_data, symbol, fq=fq)
            
            return (symbol, name, df)
        except (DataSourceError, ParseError) as e:
            logger.warning(f'Failed to fetch {symbol}: {e}')
            raise

