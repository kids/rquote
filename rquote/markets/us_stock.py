# -*- coding: utf-8 -*-
"""
美股市场
"""
import json
import pandas as pd
from typing import Tuple
from .base import Market
from ..parsers import KlineParser
from ..exceptions import DataSourceError, ParseError
from ..utils import hget, logger


class USStockMarket(Market):
    """美股市场"""
    
    def normalize_symbol(self, symbol: str) -> str:
        """标准化美股代码"""
        if not symbol.startswith('us'):
            return 'us' + symbol
        return symbol
    
    def get_price(self, symbol: str, sdate: str = '', edate: str = '', 
                  freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """获取美股价格数据"""
        symbol = self.normalize_symbol(symbol)
        
        # 特殊处理分钟数据（不使用缓存）
        if freq in ('min', '1min', 'minute'):
            return self._get_minute_data(symbol)
        
        # 使用基类的缓存逻辑
        return super().get_price(symbol, sdate, edate, freq, days, fq)
    
    def _fetch_price_data(self, symbol: str, sdate: str = '', edate: str = '', 
                          freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """从数据源获取美股价格数据"""
        try:
            raw_data = self.data_source.fetch_kline(
                symbol, freq=freq, sdate=sdate, edate=edate, days=days, fq=fq
            )
            
            parser = KlineParser()
            name, df = parser.parse_tencent_kline(raw_data, symbol)
            
            return (symbol, name, df)
        except (DataSourceError, ParseError) as e:
            logger.warning(f'Failed to fetch {symbol}: {e}')
            raise
    
    def _get_minute_data(self, symbol: str) -> Tuple[str, str, pd.DataFrame]:
        """获取分钟数据"""
        url = f'https://web.ifzq.gtimg.cn/appstock/app/UsMinute/query?_var=min_data_{symbol.replace(".", "")}&code={symbol}'
        response = hget(url)
        if not response:
            raise DataSourceError(f'Failed to fetch minute data for {symbol}')
        
        data = json.loads(response.text.split('=')[1])['data'][symbol]
        name = data['qt'][symbol][1]
        df = pd.DataFrame([i.split() for i in data['data']['data']],
                          columns=['minute', 'price', 'volume']).set_index('minute')
        for col in ['price', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        result = (symbol, name, df)
        self._put_cache(f"{symbol}:min", result)
        return result

