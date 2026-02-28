# -*- coding: utf-8 -*-
"""
港股市场
"""
from typing import Tuple, List, Dict
from .base import Market
from ..parsers import KlineParser
from ..exceptions import DataSourceError, ParseError
from ..utils import logger


class HKStockMarket(Market):
    """港股市场"""

    def normalize_symbol(self, symbol: str) -> str:
        if not symbol.startswith('hk'):
            symbol = 'hk' + symbol
        if symbol.startswith('hk') and len(symbol) == 6:
            return 'hk0' + symbol[2:]
        return symbol

    def _fetch_price_data(self, symbol: str, sdate: str = '', edate: str = '',
                          freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, List[Dict]]:
        try:
            raw_data = self.data_source.fetch_kline(
                symbol, freq=freq, sdate=sdate, edate=edate, days=days, fq=fq
            )
            parser = KlineParser()
            name, records = parser.parse_tencent_kline(raw_data, symbol, fq=fq)
            return (symbol, name, records)
        except (DataSourceError, ParseError) as e:
            logger.warning(f'Failed to fetch {symbol}: {e}')
            raise
