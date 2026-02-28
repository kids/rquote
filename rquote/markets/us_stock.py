# -*- coding: utf-8 -*-
"""
美股市场
"""
import json
from typing import Tuple, List, Dict
from .base import Market
from ..parsers import KlineParser
from ..exceptions import DataSourceError, ParseError
from ..utils import hget, logger
from ..utils.helpers import to_float


class USStockMarket(Market):
    """美股市场"""

    def normalize_symbol(self, symbol: str) -> str:
        if not symbol.startswith('us'):
            return 'us' + symbol
        return symbol

    def get_price(self, symbol: str, sdate: str = '', edate: str = '',
                  freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, List[Dict]]:
        symbol = self.normalize_symbol(symbol)
        if freq in ('min', '1min', 'minute'):
            return self._get_minute_data(symbol)
        return super().get_price(symbol, sdate, edate, freq, days, fq)

    def _fetch_price_data(self, symbol: str, sdate: str = '', edate: str = '',
                          freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, List[Dict]]:
        parser = KlineParser()

        if symbol.endswith(('.OQ', '.N', '.AM')):
            try:
                raw_data = self.data_source.fetch_kline(
                    symbol, freq=freq, sdate=sdate, edate=edate, days=days, fq=fq
                )
                name, records = parser.parse_tencent_kline(raw_data, symbol, fq=fq)
                return (symbol, name, records)
            except (DataSourceError, ParseError) as e:
                logger.warning(f'Failed to fetch {symbol}: {e}')
                raise

        candidates = []
        for suffix in ('.OQ', '.N'):
            full_symbol = f"{symbol}{suffix}"
            try:
                raw_data = self.data_source.fetch_kline(
                    full_symbol, freq=freq, sdate=sdate, edate=edate, days=days, fq=fq
                )
                name, records = parser.parse_tencent_kline(raw_data, full_symbol, fq=fq)
                if len(records) < 3:
                    logger.warning(f'Skip {full_symbol} because records < 3 (len={len(records)})')
                    continue
                candidates.append((full_symbol, name, records))
            except (DataSourceError, ParseError) as e:
                logger.warning(f'Failed to fetch {full_symbol}: {e}')
                continue

        if not candidates:
            raise DataSourceError(f'Failed to fetch US symbol {symbol} with .OQ/.N suffixes')

        def _score(records: List[Dict]):
            if not records:
                return (0, '')
            first_date = str(records[0].get('date', ''))
            return (len(records), first_date)

        best_symbol, best_name, best_records = max(
            candidates,
            key=lambda item: _score(item[2]),
        )
        return (best_symbol, best_name, best_records)

    def _get_minute_data(self, symbol: str) -> Tuple[str, str, List[Dict]]:
        url = f'https://web.ifzq.gtimg.cn/appstock/app/UsMinute/query?_var=min_data_{symbol.replace(".", "")}&code={symbol}'
        response = hget(url)
        if not response:
            raise DataSourceError(f'Failed to fetch minute data for {symbol}')

        data = json.loads(response.text.split('=')[1])['data'][symbol]
        name = data['qt'][symbol][1]
        records = []
        for item in data['data']['data']:
            parts = item.split()
            records.append({
                'minute': parts[0] if len(parts) > 0 else '',
                'price': to_float(parts[1]) if len(parts) > 1 else None,
                'volume': to_float(parts[2]) if len(parts) > 2 else None,
            })

        result = (symbol, name, records)
        self._put_cache(f"{symbol}:min", result)
        return result
