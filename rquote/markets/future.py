# -*- coding: utf-8 -*-
"""
期货市场
"""
import json
from typing import Tuple, List, Dict
from .base import Market
from ..parsers import KlineParser
from ..exceptions import DataSourceError, ParseError
from ..utils import hget, logger
from ..utils.helpers import to_float


class FutureMarket(Market):
    """期货市场"""

    def normalize_symbol(self, symbol: str) -> str:
        if not symbol.startswith('fu'):
            return 'fu' + symbol
        return symbol

    def get_price(self, symbol: str, sdate: str = '', edate: str = '',
                  freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, List[Dict]]:
        symbol = self.normalize_symbol(symbol)
        if symbol[2:5].lower() == 'btc':
            if freq in ('min', '1min', 'minute'):
                return self._get_btc_minute_price(symbol)
            else:
                return self._get_btc_price(symbol)
        return super().get_price(symbol, sdate, edate, freq, days, fq)

    def _fetch_price_data(self, symbol: str, sdate: str = '', edate: str = '',
                          freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, List[Dict]]:
        future_code = symbol[2:]
        try:
            raw_data = self.data_source.fetch_kline(future_code, freq=freq)
            parser = KlineParser()
            records = parser.parse_sina_future_kline(raw_data, freq=freq)
            return (symbol, future_code, records)
        except (DataSourceError, ParseError) as e:
            logger.warning(f'Failed to fetch {symbol} using new architecture, falling back: {e}')
            return self._get_price_fallback(symbol, future_code, freq)

    def _get_btc_price(self, symbol: str) -> Tuple[str, str, List[Dict]]:
        url = 'https://quotes.sina.cn/fx/api/openapi.php/BtcService.getDayKLine?symbol=btcbtcusd'
        response = hget(url)
        if not response:
            raise DataSourceError("Failed to fetch BTC data")

        data = json.loads(response.text)['result']['data'].split('|')
        records = []
        cols = ['date', 'open', 'high', 'low', 'close', 'vol', 'amount']
        for item in data:
            parts = item.split(',')
            rec = dict(zip(cols, parts))
            for col in ['open', 'high', 'low', 'close', 'vol', 'amount']:
                if col in rec:
                    rec[col] = to_float(rec[col])
            records.append(rec)

        result = (symbol, 'BTC', records)
        self._put_cache(symbol, result)
        return result

    def _get_btc_minute_price(self, symbol: str, datalen: int = 1440) -> Tuple[str, str, List[Dict]]:
        cache_key = f"{symbol}:min:{datalen}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        url = f'https://quotes.sina.cn/fx/api/openapi.php/BtcService.getMinKline?symbol=btcbtcusd&scale=1&datalen={datalen}&callback=var%20_btcbtcusd'
        response = hget(url)
        if not response:
            raise DataSourceError("Failed to fetch BTC minute data")

        text = response.text
        if '*/' in text:
            text = text.split('*/', 1)[1]
        text = text.strip()
        json_start = text.find('{')
        if json_start == -1:
            raise DataSourceError("Invalid BTC minute data format: no JSON found")

        json_str = text[json_start:]
        json_str = json_str.rstrip(');').rstrip(')')
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise DataSourceError(f"Failed to parse BTC minute data JSON: {e}")

        if data.get('result', {}).get('status', {}).get('code') != 0:
            raise DataSourceError(f"BTC API error: {data.get('result', {}).get('status', {}).get('msg', 'Unknown error')}")

        kline_data = data.get('result', {}).get('data', [])
        if not kline_data:
            raise DataSourceError("No BTC minute data returned")

        records = []
        for item in kline_data:
            records.append({
                'dtime': item.get('d', ''),
                'open': to_float(item.get('o', '0')),
                'high': to_float(item.get('h', '0')),
                'low': to_float(item.get('l', '0')),
                'close': to_float(item.get('c', '0')),
                'vol': to_float(item.get('v', '0')),
                'amount': to_float(item.get('a', '0')),
            })

        if not records:
            raise DataSourceError("Empty BTC minute data")

        result = (symbol, 'BTC', records)
        self._put_cache(cache_key, result)
        return result

    def _get_price_fallback(self, symbol: str, future_code: str, freq: str) -> Tuple[str, str, List[Dict]]:
        from ..utils.helpers import load_js_var_json

        if freq in ('min', '1min', 'minute'):
            url = f'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20t1nf_{future_code}=/InnerFuturesNewService.getMinLine?symbol={future_code}'
            raw = load_js_var_json(url)
            records = []
            if raw:
                cols = ['dtime', 'close', 'avg', 'vol', 'hold', 'last_close', 'cur_date']
                for item in raw:
                    if isinstance(item, (list, tuple)):
                        rec = dict(zip(cols, item))
                    else:
                        rec = dict(item)
                    for col in ['close', 'avg', 'vol', 'hold']:
                        if col in rec:
                            rec[col] = to_float(rec[col])
                    records.append(rec)
            result = (future_code, future_code, records)
        else:
            url = f'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20t1nf_{future_code}=/InnerFuturesNewService.getDailyKLine?symbol={future_code}'
            raw = load_js_var_json(url)
            records = []
            if raw:
                cols = ['date', 'open', 'high', 'low', 'close', 'vol', 'p', 's']
                for item in raw:
                    if isinstance(item, (list, tuple)):
                        rec = dict(zip(cols, item))
                    else:
                        rec = dict(item)
                    for col in ['open', 'high', 'low', 'close', 'vol', 'p', 's']:
                        if col in rec:
                            rec[col] = to_float(rec[col])
                    records.append(rec)
            result = (symbol, future_code, records)

        self._put_cache(f"{symbol}:{freq}", result)
        return result
