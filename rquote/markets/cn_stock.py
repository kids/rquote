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
        
        # PT代码也使用基类的缓存逻辑（包含长度检查和扩展逻辑）
        # 使用基类的缓存逻辑，所有市场都会应用这两个逻辑：
        # 1. 如果从cache取的数据在edate前的长度小于等于60，则进行网络请求取数合并进cache
        # 2. 如果cache中有数据，但新的edate小于cache中数据最小值，从更早日期开始取并合并
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
        cached_df = None
        need_fetch = False  # 标记是否需要从网络获取数据
        if cached:
            _, name, cached_df = cached
            # 检查缓存数据是否满足请求的 edate
            if not cached_df.empty and isinstance(cached_df.index, pd.DatetimeIndex):
                cached_earliest = cached_df.index.min()
                cached_latest = cached_df.index.max()
                request_edate = pd.to_datetime(edate) if edate else None
                request_sdate = pd.to_datetime(sdate) if sdate else None
                
                # 逻辑1: 如果请求的 edate 晚于缓存的最新日期，需要从网络获取新数据
                if request_edate and request_edate > cached_latest:
                    logger.info(f"[PT CACHE INCOMPLETE] symbol={symbol}, 缓存最新日期={cached_latest}, 请求日期={request_edate}, 需要扩展数据")
                    need_fetch = True
                # 逻辑2: 如果从cache取的数据在edate前的长度小于等于60，则进行网络请求取数合并进cache
                elif request_edate:
                    # 计算edate之前的数据行数
                    data_before_edate = cached_df[cached_df.index <= request_edate]
                    if len(data_before_edate) <= 60:
                        logger.info(f"[PT CACHE INSUFFICIENT] symbol={symbol}, edate前数据行数={len(data_before_edate)} <= 60, 需要获取更多历史数据")
                        need_fetch = True
                # 逻辑3: 如果cache中有数据，但新的edate小于cache中数据最小值，需要从更早的日期开始取
                elif request_edate and request_edate < cached_earliest:
                    logger.info(f"[PT CACHE EARLY] symbol={symbol}, 请求edate={request_edate} 早于缓存最早日期={cached_earliest}, 需要从更早日期获取")
                    need_fetch = True
                else:
                    logger.info(f"[PT CACHE HIT] symbol={symbol}, 从缓存返回数据, 日期范围={cached_df.index.min()} 到 {cached_df.index.max()}")
                    return cached
            else:
                logger.info(f"[PT CACHE HIT] symbol={symbol}, 从缓存返回数据")
                return cached
        
        try:
            # 确定需要获取的日期范围
            extend_sdate = sdate
            extend_edate = edate
            need_multiple_fetch = False  # 是否需要多次获取以满足长度要求
            
            if cached and cached_df is not None and not cached_df.empty and isinstance(cached_df.index, pd.DatetimeIndex):
                cached_earliest = cached_df.index.min()
                cached_latest = cached_df.index.max()
                request_edate = pd.to_datetime(edate) if edate else None
                request_sdate = pd.to_datetime(sdate) if sdate else None
                
                # 情况1: 请求的 edate 晚于缓存的最新日期，从缓存的最新日期+1天开始获取
                if request_edate and request_edate > cached_latest:
                    extend_sdate = (cached_latest + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    logger.info(f"[PT FETCH] 从缓存最新日期后开始获取, extend_sdate={extend_sdate}, edate={edate}")
                # 情况2: edate前的数据长度<=60，需要获取更多历史数据
                elif request_edate:
                    data_before_edate = cached_df[cached_df.index <= request_edate]
                    if len(data_before_edate) <= 60:
                        # 计算需要获取多少天的数据才能达到60+行
                        # 假设每个交易日都有数据，需要大约60个交易日（约3个月）
                        # 从edate往前推，确保获取足够的数据
                        target_sdate = request_edate - pd.Timedelta(days=120)  # 往前推约4个月，确保有足够交易日
                        extend_sdate = target_sdate.strftime('%Y-%m-%d')
                        extend_edate = edate
                        logger.info(f"[PT FETCH] edate前数据不足60行，从更早日期获取, extend_sdate={extend_sdate}, extend_edate={extend_edate}")
                        need_multiple_fetch = True  # 可能需要多次获取
                # 情况3: 请求的edate早于缓存的最早日期，从缓存最早日期开始往前获取
                elif request_edate and request_edate < cached_earliest:
                    # 从缓存最早日期开始往前获取，直到覆盖edate且edate前的长度大于60
                    # 先尝试从edate往前推足够的天数
                    target_sdate = request_edate - pd.Timedelta(days=120)  # 往前推约4个月
                    extend_sdate = target_sdate.strftime('%Y-%m-%d')
                    extend_edate = (cached_earliest - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    logger.info(f"[PT FETCH] edate早于缓存最早日期，从更早日期获取, extend_sdate={extend_sdate}, extend_edate={extend_edate}")
                    need_multiple_fetch = True  # 可能需要多次获取
            
            url = f'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get?_var=kline_dayqfq&param={symbol},{freq},{extend_sdate},{extend_edate},{days},{fq}'
            response = hget(url)
            if not response:
                logger.warning(f'{symbol} hget failed')
                # 如果网络请求失败，但有缓存数据，返回缓存数据
                if cached:
                    logger.info(f"[PT FALLBACK] 网络请求失败，返回缓存数据")
                    return cached
                return symbol, 'None', pd.DataFrame([])
            
            response_text = response.text
            json_start = response_text.find('{')
            if json_start == -1:
                logger.warning(f'{symbol} invalid response format')
                # 如果解析失败，但有缓存数据，返回缓存数据
                if cached:
                    logger.info(f"[PT FALLBACK] 解析失败，返回缓存数据")
                    return cached
                return symbol, 'None', pd.DataFrame([])
            
            data = json.loads(response_text[json_start:])
            if data.get('code') != 0:
                logger.warning(f'{symbol} API returned error: {data.get("msg", "Unknown error")}')
                # 如果API返回错误，但有缓存数据，返回缓存数据
                if cached:
                    logger.info(f"[PT FALLBACK] API错误，返回缓存数据")
                    return cached
                return symbol, 'None', pd.DataFrame([])
            
            # 使用解析器
            try:
                parser = KlineParser()
                name, df = parser.parse_tencent_kline(data, symbol)
                
                # 如果有缓存数据，合并新旧数据
                if cached and cached_df is not None and not cached_df.empty and isinstance(cached_df.index, pd.DatetimeIndex):
                    # 确保两个 DataFrame 的索引都是 DatetimeIndex
                    if not isinstance(df.index, pd.DatetimeIndex):
                        try:
                            df.index = pd.to_datetime(df.index)
                        except (ValueError, TypeError):
                            pass
                    
                    # 合并数据
                    merged_df = pd.concat([cached_df, df])
                    merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
                    merged_df = merged_df.sort_index()
                    
                    # 过滤到请求的日期范围
                    if edate:
                        request_edate = pd.to_datetime(edate)
                        merged_df = merged_df[merged_df.index <= request_edate]
                    
                    result = (symbol, name, merged_df)
                    logger.info(f"[PT MERGE] 合并缓存和新数据, 缓存行数={len(cached_df)}, 新数据行数={len(df)}, 合并后行数={len(merged_df)}")
                else:
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
                
                # 如果有缓存数据，合并新旧数据
                if cached and cached_df is not None and not cached_df.empty and isinstance(cached_df.index, pd.DatetimeIndex):
                    # 确保两个 DataFrame 的索引都是 DatetimeIndex
                    if not isinstance(df.index, pd.DatetimeIndex):
                        try:
                            df.index = pd.to_datetime(df.index)
                        except (ValueError, TypeError):
                            pass
                    
                    # 合并数据
                    merged_df = pd.concat([cached_df, df])
                    merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
                    merged_df = merged_df.sort_index()
                    
                    # 过滤到请求的日期范围
                    if edate:
                        request_edate = pd.to_datetime(edate)
                        merged_df = merged_df[merged_df.index <= request_edate]
                    
                    result = (symbol, name, merged_df)
                    logger.info(f"[PT MERGE FALLBACK] 合并缓存和新数据, 缓存行数={len(cached_df)}, 新数据行数={len(df)}, 合并后行数={len(merged_df)}")
                else:
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

