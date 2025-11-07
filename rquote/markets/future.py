# -*- coding: utf-8 -*-
"""
期货市场
"""
import json
import pandas as pd
from typing import Tuple
from .base import Market
from ..parsers import KlineParser
from ..exceptions import DataSourceError, ParseError
from ..utils import hget, logger


class FutureMarket(Market):
    """期货市场"""
    
    def normalize_symbol(self, symbol: str) -> str:
        """标准化期货代码"""
        if not symbol.startswith('fu'):
            return 'fu' + symbol
        return symbol
    
    def get_price(self, symbol: str, sdate: str = '', edate: str = '', 
                  freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """获取期货价格数据"""
        symbol = self.normalize_symbol(symbol)
        
        # 特殊处理BTC
        if symbol[2:5].lower() == 'btc':
            return self._get_btc_price(symbol)
        
        cache_key = f"{symbol}:{sdate}:{edate}:{freq}:{days}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        future_code = symbol[2:]  # 去掉'fu'前缀
        
        try:
            raw_data = self.data_source.fetch_kline(future_code, freq=freq)
            parser = KlineParser()
            df = parser.parse_sina_future_kline(raw_data, freq=freq)
            
            result = (symbol, future_code, df)
            self._put_cache(cache_key, result)
            return result
        except (DataSourceError, ParseError) as e:
            logger.warning(f'Failed to fetch {symbol} using new architecture, falling back: {e}')
            return self._get_price_fallback(symbol, future_code, freq)
    
    def _get_btc_price(self, symbol: str) -> Tuple[str, str, pd.DataFrame]:
        """获取比特币价格"""
        url = 'https://quotes.sina.cn/fx/api/openapi.php/BtcService.getDayKLine?symbol=btcbtcusd'
        response = hget(url)
        if not response:
            raise DataSourceError("Failed to fetch BTC data")
        
        data = json.loads(response.text)['result']['data'].split('|')
        df = pd.DataFrame([i.split(',') for i in data], 
                          columns=['date', 'open', 'high', 'low', 'close', 'vol', 'amount'])
        for col in ['open', 'high', 'low', 'close', 'vol', 'amount']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.set_index('date').astype(float)
        
        result = (symbol, 'BTC', df)
        self._put_cache(symbol, result)
        return result
    
    def _get_price_fallback(self, symbol: str, future_code: str, freq: str) -> Tuple[str, str, pd.DataFrame]:
        """降级方法"""
        from ..utils.helpers import load_js_var_json
        
        if freq in ('min', '1min', 'minute'):
            url = f'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20t1nf_{future_code}=/InnerFuturesNewService.getMinLine?symbol={future_code}'
            df = pd.DataFrame(load_js_var_json(url))
            df.columns = ['dtime', 'close', 'avg', 'vol', 'hold', 'last_close', 'cur_date']
            for col in ['close', 'avg', 'vol', 'hold']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.set_index('dtime')
            result = (future_code, future_code, df)
        else:
            url = f'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20t1nf_{future_code}=/InnerFuturesNewService.getDailyKLine?symbol={future_code}'
            df = pd.DataFrame(load_js_var_json(url))
            df.columns = ['date', 'open', 'high', 'low', 'close', 'vol', 'p', 's']
            for col in ['open', 'high', 'low', 'close', 'vol', 'p', 's']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.set_index('date').astype(float)
            result = (symbol, future_code, df)
        
        self._put_cache(f"{symbol}:{freq}", result)
        return result

