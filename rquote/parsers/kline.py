# -*- coding: utf-8 -*-
"""
K线数据解析器
"""
from typing import Dict, Any, Tuple, List
from ..exceptions import ParseError
from ..utils.helpers import to_float


class KlineParser:
    """K线数据解析器"""

    @staticmethod
    def parse_tencent_kline(data: Dict[str, Any], symbol: str, fq: str = 'qfq') -> Tuple[str, List[Dict]]:
        """
        解析腾讯K线数据

        Returns:
            (name, list[dict])  每条记录含 'date' 键
        """
        try:
            symbol_data = data.get('data', {}).get(symbol, {})
            if not symbol_data:
                raise ParseError(f"No data for symbol {symbol}")

            if fq == 'qfq':
                time_keys = ['qfqday', 'day', 'hfqday', 'qfqweek', 'week', 'hfqweek',
                             'qfqmonth', 'month', 'hfqmonth']
            elif fq == 'hfq':
                time_keys = ['hfqday', 'day', 'qfqday', 'hfqweek', 'week', 'qfqweek',
                             'hfqmonth', 'month', 'qfqmonth']
            else:
                time_keys = ['day', 'qfqday', 'hfqday', 'week', 'qfqweek', 'hfqweek',
                             'month', 'qfqmonth', 'hfqmonth']
            tk = None
            for tkt in time_keys:
                if tkt in symbol_data:
                    tk = tkt
                    break

            if not tk:
                raise ParseError(f"No time key found for {symbol}")

            name = ''
            if 'qt' in symbol_data and symbol in symbol_data['qt']:
                qt_data = symbol_data['qt'][symbol]
                if len(qt_data) > 1:
                    name = qt_data[1]

            kline_data = symbol_data[tk]
            records = []
            for j in kline_data:
                row = j[:6]
                records.append({
                    'date': row[0],
                    'open': to_float(row[1]),
                    'close': to_float(row[2]),
                    'high': to_float(row[3]),
                    'low': to_float(row[4]),
                    'vol': to_float(row[5]),
                })

            return name, records
        except ParseError:
            raise
        except Exception as e:
            raise ParseError(f"Failed to parse Tencent kline data: {e}")

    @staticmethod
    def parse_sina_future_kline(data: Dict[str, Any], freq: str = 'day') -> List[Dict]:
        """
        解析新浪期货K线数据

        Returns:
            list[dict]  分钟线含 'dtime' 键，日线含 'date' 键
        """
        try:
            raw_data = data.get('data', [])
            if not raw_data:
                raise ParseError("Empty data from Sina")

            numeric_cols_min = ['close', 'avg', 'vol', 'hold']
            numeric_cols_day = ['open', 'high', 'low', 'close', 'vol', 'p', 's']

            if freq in ('min', '1min', 'minute'):
                records = []
                # 新浪新格式: d -> dtime, c -> close 等
                _SINA_MIN_MAP = {'d': 'dtime', 'c': 'close', 'v': 'vol'}
                for item in raw_data:
                    rec = dict(item)
                    for old_k, new_k in _SINA_MIN_MAP.items():
                        if old_k in rec and new_k not in rec:
                            rec[new_k] = rec.pop(old_k, '' if new_k == 'dtime' else 0)
                    for col in numeric_cols_min:
                        if col in rec:
                            rec[col] = to_float(rec[col])
                    # ensure dtime key
                    if 'dtime' not in rec and 'date' in rec:
                        rec['dtime'] = rec.pop('date')
                    records.append(rec)
            else:
                records = []
                cols = ['date', 'open', 'high', 'low', 'close', 'vol', 'p', 's']
                # 新浪新格式 dict: d,o,h,l,c,v,p,s -> date,open,high,low,close,vol,p,s
                _SINA_DAY_MAP = {'d': 'date', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'vol'}
                for item in raw_data:
                    if isinstance(item, (list, tuple)):
                        rec = dict(zip(cols, item))
                    else:
                        rec = dict(item)
                        for old_k, new_k in _SINA_DAY_MAP.items():
                            if old_k in rec and new_k not in rec:
                                rec[new_k] = rec.pop(old_k, '' if new_k == 'date' else 0)
                    for col in numeric_cols_day:
                        if col in rec:
                            rec[col] = to_float(rec[col])
                    records.append(rec)

            return records
        except ParseError:
            raise
        except Exception as e:
            raise ParseError(f"Failed to parse Sina future kline data: {e}")
