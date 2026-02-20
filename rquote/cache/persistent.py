# -*- coding: utf-8 -*-
"""
持久化缓存实现：基于存储后端协议，通过工厂创建。

使用方式：
  - 推荐：cache = create_persistent_cache(backend="sqlite", path=..., ttl=...)
  - 或：   cache = PersistentCache(backend=my_backend, ttl=...)
  - 兼容旧：cache = PersistentCache(db_path=..., use_duckdb=True, ttl=...)  # use_duckdb=True 即 sqlite
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional, Any, Tuple, Callable
import pandas as pd
from .base import Cache

try:
    from ..utils.logging import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# 延迟导入，避免循环依赖；工厂里再取 backends
from . import storage


def create_persistent_cache(
    backend: str = "sqlite",
    path: Optional[str] = None,
    ttl: Optional[int] = None,
    **kwargs: Any,
) -> "PersistentCache":
    """
    工厂：按名称创建持久化缓存。

    Args:
        backend: 存储后端名称，支持 "sqlite" | "jsonl" | "pickle"
        path: 存储路径（文件或目录），默认 ~/.rquote/cache.{db|jsonl|pkl}
        ttl: 默认过期时间（秒），None 表示不过期
        **kwargs: 传给具体后端构造函数的额外参数

    Returns:
        PersistentCache 实例
    """
    home = Path.home()
    cache_dir = home / ".rquote"
    cache_dir.mkdir(exist_ok=True)

    if path is None:
        ext = {"sqlite": "db", "jsonl": "jsonl", "pickle": "pkl"}.get(backend, "db")
        path = str(cache_dir / f"cache.{ext}")

    backend_instance: storage.StorageBackend
    if backend == "sqlite":
        backend_instance = storage.SQLiteBackend(path)
    elif backend == "jsonl":
        market_paths = kwargs.get("market_paths")
        route_fn = kwargs.get("route_fn")
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
        backend_instance = storage.MarketJsonlBackend(market_paths=market_paths, route_fn=route_fn)
    elif backend == "pickle":
        backend_instance = storage.PickleBackend(path)
    else:
        raise ValueError(f"不支持的 backend: {backend!r}，可选: sqlite, jsonl, pickle")

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
        """
        初始化持久化缓存。

        Args:
            backend: 存储后端实例（与 db_path/use_duckdb 二选一）
            db_path: 兼容旧 API，数据库/文件路径
            use_duckdb: 兼容旧 API，True 使用 sqlite，False 使用 pickle
            ttl: 默认过期时间（秒），None 表示不过期
        """
        self.ttl = ttl
        if backend is not None:
            self._backend = backend
            return
        # 兼容旧：PersistentCache(db_path=..., use_duckdb=...)，use_duckdb=True 即 sqlite
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

    def _parse_date(self, date_str: str) -> Optional[pd.Timestamp]:
        if not date_str:
            return None
        try:
            return pd.to_datetime(date_str)
        except Exception:
            return None

    def _get_dataframe_date_range(self, df: pd.DataFrame) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        if df.empty:
            return None, None
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                index = pd.to_datetime(df.index)
                if len(index) > 0:
                    return index.min(), index.max()
            except (ValueError, TypeError):
                pass
            return None, None
        return df.index.min(), df.index.max()

    def _filter_dataframe_by_date(
        self,
        df: pd.DataFrame,
        sdate: Optional[str] = None,
        edate: Optional[str] = None,
    ) -> pd.DataFrame:
        if df.empty or not isinstance(df.index, pd.DatetimeIndex):
            return df
        start_date = self._parse_date(sdate) if sdate else None
        end_date = self._parse_date(edate) if edate else None
        if start_date is not None and end_date is not None:
            mask = (df.index >= start_date) & (df.index <= end_date)
            return df[mask]
        if start_date is not None:
            return df[df.index >= start_date]
        if end_date is not None:
            return df[df.index <= end_date]
        return df

    def _merge_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        if df1.empty:
            return df2
        if df2.empty:
            return df1
        combined = pd.concat([df1, df2])
        combined = combined[~combined.index.duplicated(keep="last")]
        return combined.sort_index()

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
            _, _, df = result
            logger.info(f"[CACHE HIT] key={key}, 返回数据行数={len(df)}, 日期范围={df.index.min()} 到 {df.index.max()}")
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
    ) -> Optional[Tuple[str, str, pd.DataFrame]]:
        raw = self._backend.get_raw(base_key)
        if not raw:
            return None
        symbol_out, name, df, expire_at = raw
        if self.ttl and expire_at is not None and pd.Timestamp.now() > expire_at:
            self.delete(base_key)
            return None
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except (ValueError, TypeError):
                return None
        if df.empty:
            return None
        cached_earliest = df.index.min()
        cached_latest = df.index.max()
        request_sdate = self._parse_date(sdate) if sdate else None
        request_edate = self._parse_date(edate) if edate else None
        has_overlap = True
        if request_edate is not None and request_edate < cached_earliest:
            has_overlap = False
        if request_sdate is not None and request_sdate > cached_latest:
            has_overlap = False
        if not has_overlap:
            return None
        filtered_df = self._filter_dataframe_by_date(df, sdate, edate)
        if filtered_df.empty:
            return None
        return (symbol_out, name, filtered_df)

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        if not isinstance(value, tuple) or len(value) != 3:
            return
        symbol, name, df = value
        if not isinstance(df, pd.DataFrame) or df.empty:
            return
        logger.info(f"[CACHE PUT] key={key}, 数据行数={len(df)}, 日期范围={df.index.min()} 到 {df.index.max()}")
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except (ValueError, TypeError):
                pass
        parts = key.split(":")
        if len(parts) == 3:
            base_key = key
            freq, fq = parts[1], parts[2]
        else:
            _, _, _, freq, fq = self._extract_key_parts(key)
            base_key = self._get_base_key(symbol, freq, fq)
        existing = self._backend.get_raw(base_key)
        if existing:
            _, existing_name, existing_df, _ = existing
            if not name:
                name = existing_name
            df = self._merge_dataframes(existing_df, df)
            if not isinstance(df.index, pd.DatetimeIndex):
                try:
                    df.index = pd.to_datetime(df.index)
                except (ValueError, TypeError):
                    pass
        earliest_date, latest_date = self._get_dataframe_date_range(df)
        earliest_str = earliest_date.strftime("%Y-%m-%d") if earliest_date else None
        latest_str = latest_date.strftime("%Y-%m-%d") if latest_date else None
        expire_at = None
        if ttl or self.ttl:
            expire_at = pd.Timestamp.now() + pd.Timedelta(seconds=(ttl or self.ttl))
        self._backend.put(base_key, symbol, name, df, earliest_str, latest_str, freq, fq, expire_at)
        logger.info(f"[CACHE PUT] 存储完成, base_key={base_key}, 日期范围={earliest_str} 到 {latest_str}")

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
        fetch_func: Callable[[str, str, str, str, int, str], Tuple[str, str, pd.DataFrame]],
        min_rows_before_edate: int = 300,
        max_extend_iterations: int = 15,
    ) -> Tuple[str, str, pd.DataFrame]:
        """
        自动扩展并融合缓存数据：
        - 请求日期晚于缓存最新：向前扩展并融合
        - 请求日期过早或 edate 前数据不足：向后扩展并融合
        - 融合统一通过 put() 完成（去重/排序/覆盖）
        """
        base_key = self._get_base_key(symbol, freq, fq)
        request_edate = self._parse_date(edate) if edate else None

        def _get_full_cached() -> Optional[Tuple[str, str, pd.DataFrame]]:
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

        _, _, full_df = full_cached
        if full_df.empty or not isinstance(full_df.index, pd.DatetimeIndex):
            fetched = fetch_func(symbol, sdate, edate, freq, days, fq)
            self.put(base_key, fetched)
            final_hit = self.get(base_key, sdate=sdate, edate=edate)
            return final_hit if final_hit else fetched

        # 1) 前向扩展：缓存最新仍早于请求 edate
        if request_edate is not None and request_edate > full_df.index.max():
            today_str = pd.Timestamp.now().strftime("%Y-%m-%d")
            for _ in range(max_extend_iterations):
                current_latest = full_df.index.max()
                extend_sdate = (current_latest + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                extend_edate = today_str
                logger.info(
                    f"[PRICE AUTO FORWARD] symbol={symbol}, extend_sdate={extend_sdate}, "
                    f"extend_edate={extend_edate}"
                )
                fetched = fetch_func(symbol, extend_sdate, extend_edate, freq, days, fq)
                _, _, fetched_df = fetched
                if fetched_df is None or fetched_df.empty:
                    logger.info("[PRICE AUTO FORWARD] fetched empty, stop")
                    break
                self.put(base_key, fetched)
                refreshed = _get_full_cached()
                if not refreshed:
                    break
                _, _, full_df = refreshed
                if full_df.index.max() <= current_latest:
                    logger.info("[PRICE AUTO FORWARD] latest not advanced, stop")
                    break
                if full_df.index.max() >= request_edate:
                    logger.info(f"[PRICE AUTO FORWARD] reached request edate={request_edate}")
                    break

        # 2) 后向扩展：请求 edate 早于缓存最早，或 edate 前样本不足
        if request_edate is not None:
            data_before = full_df[full_df.index <= request_edate]
            need_backward = (request_edate < full_df.index.min()) or (len(data_before) <= min_rows_before_edate)
            if need_backward:
                for _ in range(max_extend_iterations):
                    current_earliest = full_df.index.min()
                    extend_edate = (current_earliest - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                    logger.info(
                        f"[PRICE AUTO BACKWARD] symbol={symbol}, extend_edate={extend_edate}, "
                        f"current_earliest={current_earliest}"
                    )
                    fetched = fetch_func(symbol, "", extend_edate, freq, days, fq)
                    _, _, fetched_df = fetched
                    if fetched_df is None or fetched_df.empty:
                        logger.info("[PRICE AUTO BACKWARD] fetched empty, stop")
                        break
                    self.put(base_key, fetched)
                    refreshed = _get_full_cached()
                    if not refreshed:
                        break
                    _, _, full_df = refreshed
                    if full_df.index.min() >= current_earliest:
                        logger.info("[PRICE AUTO BACKWARD] earliest not moved, stop")
                        break
                    data_before = full_df[full_df.index <= request_edate]
                    if len(data_before) > min_rows_before_edate and full_df.index.min() <= request_edate:
                        logger.info(
                            f"[PRICE AUTO BACKWARD] sufficient rows before edate={request_edate}, "
                            f"rows={len(data_before)}"
                        )
                        break

        final_hit = self.get(base_key, sdate=sdate, edate=edate)
        if final_hit:
            return final_hit

        logger.info(f"[PRICE AUTO] final miss after extension, fallback fetch symbol={symbol}")
        fetched = fetch_func(symbol, sdate, edate, freq, days, fq)
        self.put(base_key, fetched)
        return fetched
