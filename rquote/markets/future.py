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
        
        # 特殊处理BTC（不使用缓存）
        if symbol[2:5].lower() == 'btc':
            if freq in ('min', '1min', 'minute'):
                return self._get_btc_minute_price(symbol)
            else:
                return self._get_btc_price(symbol)
        
        # 使用基类的缓存逻辑
        return super().get_price(symbol, sdate, edate, freq, days, fq)
    
    def _fetch_price_data(self, symbol: str, sdate: str = '', edate: str = '', 
                          freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """从数据源获取期货价格数据"""
        future_code = symbol[2:]  # 去掉'fu'前缀
        
        try:
            raw_data = self.data_source.fetch_kline(future_code, freq=freq)
            parser = KlineParser()
            df = parser.parse_sina_future_kline(raw_data, freq=freq)
            
            return (symbol, future_code, df)
        except (DataSourceError, ParseError) as e:
            logger.warning(f'Failed to fetch {symbol} using new architecture, falling back: {e}')
            return self._get_price_fallback(symbol, future_code, freq)
    
    def _get_btc_price(self, symbol: str) -> Tuple[str, str, pd.DataFrame]:
        """获取比特币日线价格"""
        url = 'https://quotes.sina.cn/fx/api/openapi.php/BtcService.getDayKLine?symbol=btcbtcusd'
        response = hget(url)
        if not response:
            raise DataSourceError("Failed to fetch BTC data")
        
        data = json.loads(response.text)['result']['data'].split('|')
        df = pd.DataFrame([i.split(',') for i in data], 
                          columns=['date', 'open', 'high', 'low', 'close', 'vol', 'amount'])
        for col in ['open', 'high', 'low', 'close', 'vol', 'amount']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.set_index('date')
        
        result = (symbol, 'BTC', df)
        self._put_cache(symbol, result)
        return result
    
    def _get_btc_minute_price(self, symbol: str, datalen: int = 1440) -> Tuple[str, str, pd.DataFrame]:
        """
        获取比特币分钟级价格
        
        Args:
            symbol: 股票代码（如 'fuBTC'）
            datalen: 数据长度，默认1440（24小时，每分钟1条）
        
        Returns:
            (symbol, name, DataFrame)
        """
        cache_key = f"{symbol}:min:{datalen}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        url = f'https://quotes.sina.cn/fx/api/openapi.php/BtcService.getMinKline?symbol=btcbtcusd&scale=1&datalen={datalen}&callback=var%20_btcbtcusd'
        response = hget(url)
        if not response:
            raise DataSourceError("Failed to fetch BTC minute data")
        
        # 解析 JavaScript callback 格式: var _btcbtcusd({...})
        text = response.text
        
        # 移除开头的注释和脚本标签
        if '*/' in text:
            text = text.split('*/', 1)[1]
        text = text.strip()
        
        # 查找 JSON 部分（从第一个 { 开始）
        json_start = text.find('{')
        if json_start == -1:
            raise DataSourceError("Invalid BTC minute data format: no JSON found")
        
        # 提取 JSON 部分，需要找到匹配的最后一个 }
        # 格式: var _btcbtcusd({...}) 或 var _btcbtcusd({...});
        json_str = text[json_start:]
        # 移除末尾可能的 ); 或 )
        json_str = json_str.rstrip(');').rstrip(')')
        
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise DataSourceError(f"Failed to parse BTC minute data JSON: {e}")
        
        # 检查返回状态
        if data.get('result', {}).get('status', {}).get('code') != 0:
            raise DataSourceError(f"BTC API error: {data.get('result', {}).get('status', {}).get('msg', 'Unknown error')}")
        
        # 提取数据
        kline_data = data.get('result', {}).get('data', [])
        if not kline_data:
            raise DataSourceError("No BTC minute data returned")
        
        # 转换为 DataFrame
        # 数据格式: {"d":"2025-11-16 15:35:00","o":"95835.37","h":"95919.90","l":"95835.37","c":"95919.89","v":"6","a":"551441.4297"}
        records = []
        for item in kline_data:
            records.append({
                'date': item.get('d', ''),
                'open': item.get('o', '0'),
                'high': item.get('h', '0'),
                'low': item.get('l', '0'),
                'close': item.get('c', '0'),
                'vol': item.get('v', '0'),
                'amount': item.get('a', '0')
            })
        
        df = pd.DataFrame(records)
        if df.empty:
            raise DataSourceError("Empty BTC minute data")
        
        # 转换数据类型
        for col in ['open', 'high', 'low', 'close', 'vol', 'amount']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 设置索引
        df = df.set_index('date')
        
        result = (symbol, 'BTC', df)
        self._put_cache(cache_key, result)
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
            df = df.set_index('date')
            result = (symbol, future_code, df)
        
        self._put_cache(f"{symbol}:{freq}", result)
        return result

