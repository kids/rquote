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
        parser = KlineParser()

        # 如果已经带有 .OQ / .N / .AM（ETF）后缀，直接按当前 symbol 请求一次即可
        if symbol.endswith(('.OQ', '.N', '.AM')):
            try:
                raw_data = self.data_source.fetch_kline(
                    symbol, freq=freq, sdate=sdate, edate=edate, days=days, fq=fq
                )
                name, df = parser.parse_tencent_kline(raw_data, symbol, fq=fq)
                return (symbol, name, df)
            except (DataSourceError, ParseError) as e:
                logger.warning(f'Failed to fetch {symbol}: {e}')
                raise

        # 否则：美股但未带交易所后缀时，同时尝试 .OQ / .N 组合
        candidates = []
        for suffix in ('.OQ', '.N'):
            full_symbol = f"{symbol}{suffix}"
            try:
                raw_data = self.data_source.fetch_kline(
                    full_symbol, freq=freq, sdate=sdate, edate=edate, days=days, fq=fq
                )
                name, df = parser.parse_tencent_kline(raw_data, full_symbol, fq=fq)
                # 行数太少（<3）通常是异常数据，直接忽略这个候选
                if len(df) < 3:
                    logger.warning(f'Skip {full_symbol} because df rows < 3 (len={len(df)})')
                    continue
                candidates.append((full_symbol, name, df))
            except (DataSourceError, ParseError) as e:
                logger.warning(f'Failed to fetch {full_symbol}: {e}')
                continue

        if not candidates:
            # 所有后缀组合都失败，按原 symbol 抛出错误
            raise DataSourceError(f'Failed to fetch US symbol {symbol} with .OQ/.N suffixes')

        # 按规则选择最优：
        # 1）先比行数，更多者优先
        # 2）如果行数相同，选第一条日期更晚的（DataFrame 按日期升序）
        def _score(df: pd.DataFrame):
            if df.empty:
                return (0, '')
            first_idx = str(df.index[0])
            return (len(df), first_idx)

        best_symbol, best_name, best_df = max(
            candidates,
            key=lambda item: _score(item[2]),
        )
        return (best_symbol, best_name, best_df)
    
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

