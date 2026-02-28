# -*- coding: utf-8 -*-
"""
持久化缓存实现：基于存储后端协议，通过工厂创建。
"""
from __future__ import annotations

import datetime
from pathlib import Path
from typing import Optional, Any, Tuple, Callable, List, Dict

from .base import Cache
from ..utils.date import DateRangeUtils

try:
    from ..utils.logging import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

from . import storage


def create_persistent_cache(
    backend: str = "sqlite",
    path: Optional[str] = None,
    ttl: Optional[int] = None,
    **kwargs: Any,
) -> "PersistentCache":
    home = Path.home()
    cache_dir = home / ".rquote"
    cache_dir.mkdir(exist_ok=True)

    if path is None:
        if backend == "per_key_json":
            path = str(cache_dir / "cache_json")
        else:
            ext = {"sqlite": "db", "jsonl": "jsonl", "pickle": "pkl"}.get(backend, "db")
            path = str(cache_dir / f"cache.{ext}")

    backend_instance: storage.StorageBackend
    if backend == "sqlite":
        backend_instance = storage.SQLiteBackend(path)
    elif backend == "jsonl":
        market_paths = kwargs.get("market_paths")
        route_fn = kwargs.get("route_fn")
        lazy = kwargs.get("lazy", False)
        if market_paths is None and path:
            p = Path(path)
            if p.suffix:
                base_dir = p.parent
                prefix = p.stem
            else:
                base_dir = p
                prefix = "cache"
            market_paths = {
                "cn": str(base_dir / f"{prefix}_cn.jsonl"),
                "hk": str(base_dir / f"{prefix}_hk.jsonl"),
                "us": str(base_dir / f"{prefix}_us.jsonl"),
                "fu": str(base_dir / f"{prefix}_fu.jsonl"),
            }
        backend_class = storage.JsonlBackendLazy if lazy else storage.JsonlBackend
        backend_instance = storage.MarketJsonlBackend(
            market_paths=market_paths, route_fn=route_fn, backend_class=backend_class
        )
    elif backend == "pickle":
        backend_instance = storage.PickleBackend(path)
    elif backend == "per_key_json":
        backend_instance = storage.PerKeyJsonBackend(path)
    else:
        raise ValueError(f"不支持的 backend: {backend!r}，可选: sqlite, jsonl, pickle, per_key_json")

    return PersistentCache(backend=backend_instance, ttl=ttl)


class PersistentCache(Cache):
    """持久化缓存：委托给 StorageBackend，只做 key 解析、日期过滤与合并逻辑。"""

    def __init__(
        self,
        backend: Optional[storage.StorageBackend] = None,
        db_path: Optional[str] = None,
        use_duckdb: bool = True,
        ttl: Optional[int] = None,
    ):
        self.ttl = ttl
        if backend is not None:
            self._backend = backend
            return
        home = Path.home()
        cache_dir = home / ".rquote"
        cache_dir.mkdir(exist_ok=True)
        path = db_path or str(cache_dir / ("cache.db" if use_duckdb else "cache.pkl"))
        self._backend = storage.SQLiteBackend(path) if use_duckdb else storage.PickleBackend(path)

    def _extract_key_parts(self, key: str) -> Tuple[str, str, str, str, str]:
        parts = key.split(":")
        if len(parts) >= 6:
            return parts[0], parts[1], parts[2], parts[3], parts[5]
        elif len(parts) >= 4:
            return (
                parts[0],
                parts[1] if len(parts) > 1 else "",
                parts[2] if len(parts) > 2 else "",
                parts[3],
                parts[4] if len(parts) > 4 else "qfq",
            )
        else:
            return parts[0] if parts else "", "", "", "day", "qfq"

    def _get_base_key(self, symbol: str, freq: str, fq: str) -> str:
        return f"{symbol}:{freq}:{fq}"

    def get(self, key: str, sdate: Optional[str] = None, edate: Optional[str] = None) -> Optional[Any]:
        parts = key.split(":")
        if len(parts) == 3:
            symbol, freq, fq = parts
            base_key = key
            sdate = sdate or ""
            edate = edate or ""
        else:
            symbol, sdate_from_key, edate_from_key, freq, fq = self._extract_key_parts(key)
            base_key = self._get_base_key(symbol, freq, fq)
            sdate = sdate if sdate is not None else sdate_from_key
            edate = edate if edate is not None else edate_from_key

        logger.info(f"[CACHE GET] key={key}, base_key={base_key}, sdate={sdate}, edate={edate}")
        result = self._get_via_backend(base_key, symbol, sdate, edate, freq, fq)
        if result:
            _, _, records = result
            earliest, latest = DateRangeUtils.get_date_range(records)
            logger.info(f"[CACHE HIT] key={key}, 返回数据行数={len(records)}, 日期范围={earliest} 到 {latest}")
        else:
            logger.info(f"[CACHE MISS] key={key}, 缓存中无数据")
        return result

    def _get_via_backend(
        self,
        base_key: str,
        symbol: str,
        sdate: str,
        edate: str,
        freq: str,
        fq: str,
    ) -> Optional[Tuple[str, str, List[Dict]]]:
        raw = self._backend.get_raw(base_key)
        if not raw:
            return None
        symbol_out, name, records, expire_at = raw
        if self.ttl and expire_at is not None and datetime.datetime.now() > expire_at:
            self.delete(base_key)
            return None
        if not records:
            return None

        date_key = DateRangeUtils.detect_date_key(records)
        cached_earliest_str, cached_latest_str = DateRangeUtils.get_date_range(records, date_key)
        cached_earliest = DateRangeUtils.parse_date(cached_earliest_str)
        cached_latest = DateRangeUtils.parse_date(cached_latest_str)

        request_sdate = DateRangeUtils.parse_date(sdate) if sdate else None
        request_edate = DateRangeUtils.parse_date(edate) if edate else None

        has_overlap = True
        if request_edate is not None and cached_earliest is not None and request_edate < cached_earliest:
            has_overlap = False
        if request_sdate is not None and cached_latest is not None and request_sdate > cached_latest:
            has_overlap = False
        if not has_overlap:
            return None

        filtered = DateRangeUtils.filter_records(records, sdate, edate, date_key)
        if not filtered:
            return None
        return (symbol_out, name, filtered)

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        if not isinstance(value, tuple) or len(value) != 3:
            return
        symbol, name, records = value
        if not isinstance(records, list) or not records:
            return

        date_key = DateRangeUtils.detect_date_key(records)
        earliest_str, latest_str = DateRangeUtils.get_date_range(records, date_key)
        logger.debug(f"[CACHE PUT] key={key}, 数据行数={len(records)}, 日期范围={earliest_str} 到 {latest_str}")

        parts = key.split(":")
        if len(parts) == 3:
            base_key = key
            freq, fq = parts[1], parts[2]
        else:
            _, _, _, freq, fq = self._extract_key_parts(key)
            base_key = self._get_base_key(symbol, freq, fq)

        existing = self._backend.get_raw(base_key)
        if existing:
            _, existing_name, existing_records, _ = existing
            if not name:
                name = existing_name
            records = DateRangeUtils.merge_records(existing_records, records, date_key)
            earliest_str, latest_str = DateRangeUtils.get_date_range(records, date_key)

        expire_at = None
        if ttl or self.ttl:
            expire_at = datetime.datetime.now() + datetime.timedelta(seconds=(ttl or self.ttl))
        self._backend.put(base_key, symbol, name, records, earliest_str, latest_str, freq, fq, expire_at)
        logger.debug(f"[CACHE PUT] 存储完成, base_key={base_key}, 日期范围={earliest_str} 到 {latest_str}")

    def delete(self, key: str) -> None:
        symbol, _, _, freq, fq = self._extract_key_parts(key)
        base_key = self._get_base_key(symbol, freq, fq)
        self._backend.delete(base_key)

    def clear(self) -> None:
        self._backend.clear()

    def close(self) -> None:
        self._backend.close()

    def status_rows(self, symbols: Optional[list[str]] = None) -> list[dict[str, Any]]:
        fn = getattr(self._backend, "status_rows", None)
        if callable(fn):
            return fn(symbols)
        return []

    def print_status_report(self, symbols: Optional[list[str]] = None, printer=print) -> None:
        rows = self.status_rows(symbols=symbols)
        printer("[cache-report] market\tsymbol\tearliest_date\tlatest_date\trows")
        for r in rows:
            printer(
                f"[cache-report] {r.get('market', '')}\t{r.get('symbol', '')}\t"
                f"{r.get('earliest_date', '')}\t{r.get('latest_date', '')}\t{r.get('rows', -1)}"
            )
        printer(f"[cache-report] total_symbols={len(rows)}")

    def get_price_auto_merge(
        self,
        symbol: str,
        sdate: str,
        edate: str,
        freq: str,
        days: int,
        fq: str,
        fetch_func: Callable[[str, str, str, str, int, str], Tuple[str, str, List[Dict]]],
        min_rows_before_edate: int = 300,
        max_extend_iterations: int = 15,
    ) -> Tuple[str, str, List[Dict]]:
        """
        自动扩展并融合缓存数据。
        """
        base_key = self._get_base_key(symbol, freq, fq)
        request_edate = DateRangeUtils.parse_date(edate) if edate else None

        def _get_full_cached() -> Optional[Tuple[str, str, List[Dict]]]:
            return self.get(base_key, sdate="", edate="")

        logger.info(
            f"[PRICE AUTO] symbol={symbol}, base_key={base_key}, sdate={sdate}, "
            f"edate={edate}, freq={freq}, days={days}, fq={fq}"
        )

        full_cached = _get_full_cached()
        if not full_cached:
            logger.info(f"[PRICE AUTO] full cache miss, fetch network: symbol={symbol}")
            fetched = fetch_func(symbol, sdate, edate, freq, days, fq)
            self.put(base_key, fetched)
            final_hit = self.get(base_key, sdate=sdate, edate=edate)
            return final_hit if final_hit else fetched

        _, _, full_records = full_cached
        if not full_records:
            fetched = fetch_func(symbol, sdate, edate, freq, days, fq)
            self.put(base_key, fetched)
            final_hit = self.get(base_key, sdate=sdate, edate=edate)
            return final_hit if final_hit else fetched

        date_key = DateRangeUtils.detect_date_key(full_records)

        # 1) 前向扩展
        _, cache_latest_str = DateRangeUtils.get_date_range(full_records, date_key)
        cache_latest = DateRangeUtils.parse_date(cache_latest_str)
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)

        cache_fresh = cache_latest is not None and cache_latest >= yesterday
        request_is_future = request_edate is not None and request_edate >= today

        is_weekend = today.weekday() >= 5
        is_weekly_market = str(symbol or "").lower().startswith(("sh", "sz", "hk", "us"))
        # 周末休市不扩展的前提：数据已更新到上一交易日
        if today.weekday() == 6:  # Sunday
            prev_trading_day = today - datetime.timedelta(days=2)  # Friday
        else:
            prev_trading_day = today - datetime.timedelta(days=1)
        cache_up_to_prev_td = cache_latest is not None and cache_latest >= prev_trading_day
        skip_forward_non_trading_day = is_weekend and is_weekly_market and cache_up_to_prev_td

        if skip_forward_non_trading_day:
            logger.info(f"[PRICE AUTO FORWARD] skip on weekend (cache up to {prev_trading_day}) for symbol={symbol}, today={today}")

        if (
            request_edate is not None
            and cache_latest is not None
            and request_edate > cache_latest
            and not skip_forward_non_trading_day
            and not (cache_fresh and request_is_future)
        ):
            today_str = today.strftime("%Y-%m-%d")
            for _ in range(max_extend_iterations):
                _, cur_latest_str = DateRangeUtils.get_date_range(full_records, date_key)
                cur_latest = DateRangeUtils.parse_date(cur_latest_str)
                if cur_latest is None:
                    break
                extend_sdate = DateRangeUtils.add_days(cur_latest_str, 1)
                extend_edate = today_str
                logger.info(f"[PRICE AUTO FORWARD] symbol={symbol}, extend_sdate={extend_sdate}, extend_edate={extend_edate}")
                fetched = fetch_func(symbol, extend_sdate, extend_edate, freq, days, fq)
                _, _, fetched_records = fetched
                if not fetched_records:
                    logger.info("[PRICE AUTO FORWARD] fetched empty, stop")
                    break
                # API 可能只返回最近 days 条，导致 extend_sdate 到 fetched_earliest 之间出现 gap，需循环补齐
                gap_sdate = extend_sdate
                gap_fetched_records = fetched_records  # 当前「fetched」段，其 earliest 为 gap 的右边界
                for _ in range(max_extend_iterations):
                    fetched_earliest_str, _ = DateRangeUtils.get_date_range(gap_fetched_records, date_key)
                    gap_sdate_parsed = DateRangeUtils.parse_date(gap_sdate)
                    fetched_earliest_parsed = DateRangeUtils.parse_date(fetched_earliest_str)
                    if (
                        gap_sdate_parsed is None
                        or fetched_earliest_parsed is None
                        or fetched_earliest_parsed <= gap_sdate_parsed
                    ):
                        break
                    gap_edate_str = DateRangeUtils.add_days(fetched_earliest_str, -1)
                    logger.info(
                        f"[PRICE AUTO FORWARD] gap detected, fill {gap_sdate}..{gap_edate_str} "
                        f"(fetched earliest={fetched_earliest_str})"
                    )
                    gap_fetched = fetch_func(symbol, gap_sdate, gap_edate_str, freq, days, fq)
                    _, _, gap_records = gap_fetched
                    if not gap_records:
                        break
                    self.put(base_key, gap_fetched)
                    refreshed = _get_full_cached()
                    if not refreshed:
                        break
                    _, _, full_records = refreshed
                    gap_earliest_str, _ = DateRangeUtils.get_date_range(gap_records, date_key)
                    gap_earliest_parsed = DateRangeUtils.parse_date(gap_earliest_str)
                    if gap_earliest_parsed is None or gap_earliest_parsed <= gap_sdate_parsed:
                        break
                    # 下次仍从 gap_sdate 起填，右边界为本次 gap_records 的 earliest - 1
                    gap_fetched_records = gap_records
                self.put(base_key, fetched)
                refreshed = _get_full_cached()
                if not refreshed:
                    break
                _, _, full_records = refreshed
                _, new_latest_str = DateRangeUtils.get_date_range(full_records, date_key)
                new_latest = DateRangeUtils.parse_date(new_latest_str)
                if new_latest is None or new_latest <= cur_latest:
                    logger.info("[PRICE AUTO FORWARD] latest not advanced, stop")
                    break
                if new_latest >= request_edate:
                    logger.info(f"[PRICE AUTO FORWARD] reached request edate={request_edate}")
                    break

        # 2) 后向扩展
        _, cur_latest_str = DateRangeUtils.get_date_range(full_records, date_key)
        cur_latest = DateRangeUtils.parse_date(cur_latest_str)
        if request_edate is not None and cur_latest is not None and request_edate <= cur_latest:
            data_before = DateRangeUtils.filter_records(full_records, edate=edate, date_key=date_key)
            cur_earliest_str, _ = DateRangeUtils.get_date_range(full_records, date_key)
            cur_earliest = DateRangeUtils.parse_date(cur_earliest_str)
            need_backward = (
                cur_earliest is None
                or request_edate < cur_earliest
                or len(data_before) <= min_rows_before_edate
            )
            if need_backward:
                for _ in range(max_extend_iterations):
                    earliest_str, _ = DateRangeUtils.get_date_range(full_records, date_key)
                    current_earliest = DateRangeUtils.parse_date(earliest_str)
                    if current_earliest is None:
                        break
                    extend_edate = DateRangeUtils.add_days(earliest_str, -1)
                    logger.info(f"[PRICE AUTO BACKWARD] symbol={symbol}, extend_edate={extend_edate}, current_earliest={current_earliest}")
                    fetched = fetch_func(symbol, "", extend_edate, freq, days, fq)
                    _, _, fetched_records = fetched
                    if not fetched_records:
                        logger.info("[PRICE AUTO BACKWARD] fetched empty, stop")
                        break
                    self.put(base_key, fetched)
                    refreshed = _get_full_cached()
                    if not refreshed:
                        break
                    _, _, full_records = refreshed
                    new_earliest_str, _ = DateRangeUtils.get_date_range(full_records, date_key)
                    new_earliest = DateRangeUtils.parse_date(new_earliest_str)
                    if new_earliest is None or new_earliest >= current_earliest:
                        logger.info("[PRICE AUTO BACKWARD] earliest not moved, stop")
                        break
                    data_before = DateRangeUtils.filter_records(full_records, edate=edate, date_key=date_key)
                    if len(data_before) > min_rows_before_edate and new_earliest <= request_edate:
                        logger.info(f"[PRICE AUTO BACKWARD] sufficient rows before edate={request_edate}, rows={len(data_before)}")
                        break

        final_hit = self.get(base_key, sdate=sdate, edate=edate)
        if final_hit:
            return final_hit

        logger.info(f"[PRICE AUTO] final miss after extension, fallback fetch symbol={symbol}")
        fetched = fetch_func(symbol, sdate, edate, freq, days, fq)
        self.put(base_key, fetched)
        return fetched
