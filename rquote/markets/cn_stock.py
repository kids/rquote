# -*- coding: utf-8 -*-
"""
A股市场
"""
import json
import base64
import pandas as pd
from typing import Tuple
from .base import Market
from ..parsers import KlineParser
from ..exceptions import DataSourceError, ParseError
from ..utils import hget, logger


class CNStockMarket(Market):
    """A股市场"""
    
    def normalize_symbol(self, symbol: str) -> str:
        """标准化A股代码"""
        if symbol[0] in ['0', '1', '3', '5', '6']:
            prefix = 'sh' if symbol[0] in ['5', '6'] else 'sz'
            return prefix + symbol
        return symbol
    
    def get_price(self, symbol: str, sdate: str = '', edate: str = '', 
                  freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """获取A股价格数据"""
        symbol = self.normalize_symbol(symbol)
        
        # 特殊处理BK（板块）代码（不使用缓存）
        if symbol[:2] == 'BK':
            return self._get_bk_price(symbol)
        
        # 特殊处理PT代码（不使用缓存）
        if symbol[:2] == 'pt':
            return self._get_pt_price(symbol, sdate, edate, freq, days, fq)
        
        # 使用基类的缓存逻辑
        return super().get_price(symbol, sdate, edate, freq, days, fq)
    
    def _fetch_price_data(self, symbol: str, sdate: str = '', edate: str = '', 
                          freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """从数据源获取A股价格数据"""
        try:
            raw_data = self.data_source.fetch_kline(
                symbol, freq=freq, sdate=sdate, edate=edate, days=days, fq=fq
            )
            
            # 使用解析器解析
            parser = KlineParser()
            name, df = parser.parse_tencent_kline(raw_data, symbol)
            
            return (symbol, name, df)
        except (DataSourceError, ParseError) as e:
            logger.warning(f'Failed to fetch {symbol} using new architecture: {e}')
            # 降级到旧方法
            return self._get_price_fallback(symbol, sdate, edate, freq, days, fq)
    
    def _get_bk_price(self, symbol: str) -> Tuple[str, str, pd.DataFrame]:
        """获取板块价格（BK开头）"""
        try:
            url = base64.b64decode('aHR0cDovL3B1c2gyaGlzLmVhc3' +
                                   'Rtb25leS5jb20vYXBpL3F0L3N0b2NrL2tsaW5lL2dldD9jYj1qUX' +
                                   'VlcnkxMTI0MDIyNTY2NDQ1ODczNzY2OTcyXzE2MTc4NjQ1NjgxMz' +
                                   'Emc2VjaWQ9OTAu').decode() + symbol + \
                              '&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5' + \
                              '&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58' + \
                              '&klt=101&fqt=0&beg=19900101&end=20990101&_=1'
            response = hget(url)
            if not response:
                logger.warning(f'{symbol} hget failed')
                return symbol, 'None', pd.DataFrame([])
            
            data = json.loads(response.text.split('jQuery1124022566445873766972_1617864568131(')[1][:-2])
            if not data.get('data'):
                logger.warning(f'{symbol} data empty')
                return symbol, 'None', pd.DataFrame([])
            
            name = data['data']['name']
            df = pd.DataFrame([i.split(',') for i in data['data']['klines']], 
                             columns=['date', 'open', 'close', 'high', 'low', 'vol', 'money', 'p'])
            df = df.set_index(['date'])
            # 转换数值列
            for col in ['open', 'close', 'high', 'low', 'vol', 'money', 'p']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            result = (symbol, name, df)
            self._put_cache(symbol, result)
            return result
        except Exception as e:
            logger.warning(f'error fetching {symbol}, err: {e}')
            return symbol, 'None', pd.DataFrame([])
    
    def _get_pt_price(self, symbol: str, sdate: str, edate: str, 
                     freq: str, days: int, fq: str) -> Tuple[str, str, pd.DataFrame]:
        """获取PT代码价格"""
        # 先检查缓存（使用base_key格式，日期通过参数传递）
        base_key = f"{symbol}:{freq}:{fq}"
        cached = self._get_cached(base_key, sdate=sdate, edate=edate)
        if cached:
            logger.info(f"[PT CACHE HIT] symbol={symbol}, 从缓存返回数据")
            return cached
        
        try:
            url = f'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get?_var=kline_dayqfq&param={symbol},{freq},{sdate},{edate},{days},{fq}'
            response = hget(url)
            if not response:
                logger.warning(f'{symbol} hget failed')
                return symbol, 'None', pd.DataFrame([])
            
            response_text = response.text
            json_start = response_text.find('{')
            if json_start == -1:
                logger.warning(f'{symbol} invalid response format')
                return symbol, 'None', pd.DataFrame([])
            
            data = json.loads(response_text[json_start:])
            if data.get('code') != 0:
                logger.warning(f'{symbol} API returned error: {data.get("msg", "Unknown error")}')
                return symbol, 'None', pd.DataFrame([])
            
            # 使用解析器
            try:
                parser = KlineParser()
                name, df = parser.parse_tencent_kline(data, symbol)
                result = (symbol, name, df)
                self._put_cache(base_key, result)
                return result
            except Exception as e:
                logger.warning(f'Failed to parse {symbol}, using fallback: {e}')
                # 降级处理
                symbol_data = data.get('data', {}).get(symbol, {})
                if not symbol_data:
                    return symbol, 'None', pd.DataFrame([])
                
                tk = None
                for tkt in ['day', 'qfqday', 'hfqday', 'week', 'qfqweek', 'hfqweek',
                            'month', 'qfqmonth', 'hfqmonth']:
                    if tkt in symbol_data:
                        tk = tkt
                        break
                
                if not tk:
                    return symbol, 'None', pd.DataFrame([])
                
                name = ''
                if 'qt' in symbol_data and symbol in symbol_data['qt']:
                    name = symbol_data['qt'][symbol][1] if len(symbol_data['qt'][symbol]) > 1 else ''
                
                kline_data = symbol_data[tk]
                df = pd.DataFrame([j[:6] for j in kline_data],
                                 columns=['date', 'open', 'close', 'high', 'low', 'vol']).set_index('date')
                for col in ['open', 'high', 'low', 'close', 'vol']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                result = (symbol, name, df)
                self._put_cache(base_key, result)
                return result
        except Exception as e:
            logger.warning(f'error fetching {symbol}, err: {e}')
            return symbol, 'None', pd.DataFrame([])
    
    def _get_price_fallback(self, symbol: str, sdate: str, edate: str, 
                           freq: str, days: int, fq: str) -> Tuple[str, str, pd.DataFrame]:
        """降级方法（旧实现）"""
        from ..utils import hget
        import json
        
        url = f'https://web.ifzq.gtimg.cn/appstock/app/newfqkline/get?param={symbol},{freq},{sdate},{edate},{days},{fq}'
        response = hget(url)
        if not response:
            raise DataSourceError(f'Failed to fetch data for {symbol}')
        
        data = json.loads(response.text)['data'][symbol]
        name = ''
        for tkt in ['day', 'qfqday', 'hfqday', 'week', 'qfqweek', 'hfqweek',
                    'month', 'qfqmonth', 'hfqmonth']:
            if tkt in data:
                tk = tkt
                break
        
        df = pd.DataFrame([j[:6] for j in data[tk]],
                         columns=['date', 'open', 'close', 'high', 'low', 'vol']).set_index('date')
        for col in ['open', 'high', 'low', 'close', 'vol']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        if 'qt' in data:
            name = data['qt'][symbol][1]
        
        result = (symbol, name, df)
        cache_key = f"{symbol}:{sdate}:{edate}:{freq}:{days}:{fq}"
        self._put_cache(cache_key, result)
        return result

