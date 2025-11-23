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
            symbol = 'hk' + symbol
        
        # 如果hk后面只有4位数字，则添加一个0
        if symbol.startswith('hk') and len(symbol) == 6:
            # hk + 4位数字 = 6位，需要补0变成 hk + 0 + 4位数字
            return 'hk0' + symbol[2:]
        
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

