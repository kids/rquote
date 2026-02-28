# -*- coding: utf-8 -*-
"""
日期工具模块
"""
import time
import re
import datetime
from typing import Optional, Tuple


def check_date_format(date_str: str) -> str:
    if not date_str:
        return ''
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    formats = ["%Y/%m/%d", "%Y%m%d", "%Y.%m.%d", "%Y_%m_%d", "%Y-%m-%d"]
    for fmt in formats:
        try:
            t_struct = time.strptime(date_str, fmt)
            return time.strftime("%Y-%m-%d", t_struct)
        except ValueError:
            continue
    raise ValueError(f"date format not recognized: {date_str}")


class DateRangeUtils:
    """纯 Python 日期范围工具，不依赖 pandas。"""

    DATE_KEYS = ('date', 'dtime', 'minute')

    @staticmethod
    def detect_date_key(records: list) -> str:
        if not records:
            return 'date'
        first = records[0]
        for k in DateRangeUtils.DATE_KEYS:
            if k in first:
                return k
        return 'date'

    @staticmethod
    def parse_date(s: str) -> Optional[datetime.date]:
        if not s:
            return None
        s = str(s).strip()
        if re.match(r'^\d{4}-\d{2}-\d{2}', s):
            try:
                return datetime.date.fromisoformat(s[:10])
            except ValueError:
                return None
        if re.match(r'^\d{8}$', s):
            try:
                return datetime.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
            except ValueError:
                return None
        return None

    @staticmethod
    def get_date_range(records: list, date_key: str = 'date') -> Tuple[str, str]:
        if not records:
            return ('', '')
        dates = []
        for r in records:
            v = r.get(date_key, '')
            if v:
                d = DateRangeUtils.parse_date(str(v))
                if d:
                    dates.append(d)
        if not dates:
            return ('', '')
        return (min(dates).strftime('%Y-%m-%d'), max(dates).strftime('%Y-%m-%d'))

    @staticmethod
    def filter_records(
        records: list,
        sdate: str = '',
        edate: str = '',
        date_key: str = 'date',
    ) -> list:
        if not records:
            return []
        start = DateRangeUtils.parse_date(sdate) if sdate else None
        end = DateRangeUtils.parse_date(edate) if edate else None
        if start is None and end is None:
            return list(records)
        result = []
        for r in records:
            v = r.get(date_key, '')
            if not v:
                result.append(r)
                continue
            d = DateRangeUtils.parse_date(str(v))
            if d is None:
                result.append(r)
                continue
            if start is not None and d < start:
                continue
            if end is not None and d > end:
                continue
            result.append(r)
        return result

    @staticmethod
    def merge_records(
        records1: list,
        records2: list,
        date_key: str = 'date',
    ) -> list:
        merged: dict = {}
        for r in records1:
            key = r.get(date_key, '')
            merged[key] = r
        for r in records2:
            key = r.get(date_key, '')
            merged[key] = r

        def sort_key(item):
            d = DateRangeUtils.parse_date(str(item.get(date_key, '')))
            return d or datetime.date.min

        return sorted(merged.values(), key=sort_key)

    @staticmethod
    def today_str() -> str:
        return datetime.date.today().strftime('%Y-%m-%d')

    @staticmethod
    def add_days(date_str: str, days: int) -> str:
        d = DateRangeUtils.parse_date(date_str)
        if d is None:
            return date_str
        return (d + datetime.timedelta(days=days)).strftime('%Y-%m-%d')
