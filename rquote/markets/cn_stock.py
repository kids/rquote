# -*- coding: utf-8 -*-
"""
A股市场
"""
import json
import base64
from typing import Tuple, List, Dict
from .base import Market
from ..parsers import KlineParser
from ..exceptions import DataSourceError, ParseError
from ..utils import hget, logger
from ..utils.helpers import to_float


class CNStockMarket(Market):
    """A股市场"""

    def normalize_symbol(self, symbol: str) -> str:
        if symbol[0] in ['0', '1', '3', '5', '6']:
            prefix = 'sh' if symbol[0] in ['5', '6'] else 'sz'
            return prefix + symbol
        return symbol

    def get_price(self, symbol: str, sdate: str = '', edate: str = '',
                  freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, List[Dict]]:
        symbol = self.normalize_symbol(symbol)
        if symbol[:2] == 'BK':
            return self._get_bk_price(symbol)
        return super().get_price(symbol, sdate, edate, freq, days, fq)

    def _fetch_price_data(self, symbol: str, sdate: str = '', edate: str = '',
                          freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, List[Dict]]:
        try:
            raw_data = self.data_source.fetch_kline(
                symbol, freq=freq, sdate=sdate, edate=edate, days=days, fq=fq
            )
            parser = KlineParser()
            name, records = parser.parse_tencent_kline(raw_data, symbol)
            return (symbol, name, records)
        except (DataSourceError, ParseError) as e:
            logger.warning(f'Failed to fetch {symbol} using new architecture: {e}')
            return self._get_price_fallback(symbol, sdate, edate, freq, days, fq)

    def _get_bk_price(self, symbol: str) -> Tuple[str, str, List[Dict]]:
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
                return symbol, 'None', []

            data = json.loads(response.text.split('jQuery1124022566445873766972_1617864568131(')[1][:-2])
            if not data.get('data'):
                logger.warning(f'{symbol} data empty')
                return symbol, 'None', []

            name = data['data']['name']
            records = []
            for item in data['data']['klines']:
                parts = item.split(',')
                records.append({
                    'date': parts[0],
                    'open': to_float(parts[1]),
                    'close': to_float(parts[2]),
                    'high': to_float(parts[3]),
                    'low': to_float(parts[4]),
                    'vol': to_float(parts[5]),
                    'money': to_float(parts[6]) if len(parts) > 6 else None,
                    'p': to_float(parts[7]) if len(parts) > 7 else None,
                })

            result = (symbol, name, records)
            self._put_cache(symbol, result)
            return result
        except Exception as e:
            logger.warning(f'error fetching {symbol}, err: {e}')
            return symbol, 'None', []

    def _get_price_fallback(self, symbol: str, sdate: str, edate: str,
                            freq: str, days: int, fq: str) -> Tuple[str, str, List[Dict]]:
        from ..utils import hget
        import json

        url = f'https://web.ifzq.gtimg.cn/appstock/app/newfqkline/get?param={symbol},{freq},{sdate},{edate},{days},{fq}'
        response = hget(url)
        if not response:
            raise DataSourceError(f'Failed to fetch data for {symbol}')

        data = json.loads(response.text)['data'][symbol]
        name = ''
        tk = None
        for tkt in ['day', 'qfqday', 'hfqday', 'week', 'qfqweek', 'hfqweek',
                    'month', 'qfqmonth', 'hfqmonth']:
            if tkt in data:
                tk = tkt
                break

        records = []
        if tk:
            for j in data[tk]:
                row = j[:6]
                records.append({
                    'date': row[0],
                    'open': to_float(row[1]),
                    'close': to_float(row[2]),
                    'high': to_float(row[3]),
                    'low': to_float(row[4]),
                    'vol': to_float(row[5]),
                })
        if 'qt' in data:
            name = data['qt'][symbol][1]

        result = (symbol, name, records)
        cache_key = f"{symbol}:{sdate}:{edate}:{freq}:{days}:{fq}"
        self._put_cache(cache_key, result)
        return result
