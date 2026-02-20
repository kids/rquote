# -*- coding: utf-8 -*-
"""
持久化存储后端抽象与默认实现。

新增数据库兼容只需：
1. 实现 StorageBackend 协议（get_raw / put / delete / clear / close）
2. 在 create_persistent_cache 的工厂里注册 backend 名称与构造方式
"""
from __future__ import annotations

import base64
import json
import pickle
import sqlite3
from pathlib import Path
from typing import Optional, Tuple, Protocol, Any, Callable
import pandas as pd


# ---------- 协议：未来兼容其他库只要实现这 5 个接口 ----------

class StorageBackend(Protocol):
    """持久化存储后端协议。兼容新数据库只需实现本协议并注册到工厂。"""

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, pd.DataFrame, Optional[pd.Timestamp]]]:
        """
        按 base_key 取出一条原始记录（不做过期、不做日期过滤）。
        返回 (symbol, name, df, expire_at)，不存在则 None。
        """
        ...

    def put(
        self,
        base_key: str,
        symbol: str,
        name: str,
        df: pd.DataFrame,
        earliest_date: Optional[str],
        latest_date: Optional[str],
        freq: str,
        fq: str,
        expire_at: Optional[pd.Timestamp],
    ) -> None:
        """写入或覆盖一条缓存记录。"""
        ...

    def delete(self, base_key: str) -> None:
        """按 base_key 删除一条记录。"""
        ...

    def clear(self) -> None:
        """清空所有记录。"""
        ...

    def close(self) -> None:
        """释放连接/句柄。"""
        ...


# ---------- 默认实现：SQLite ----------

class SQLiteBackend:
    """使用 SQLite 的存储后端（标准库 sqlite3，无额外依赖）。"""

    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path)
        self._create_table()

    def _create_table(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cache_data (
                cache_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                name TEXT,
                data BLOB,
                earliest_date TEXT,
                latest_date TEXT,
                freq TEXT,
                fq TEXT,
                updated_at TEXT,
                expire_at TEXT
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_freq_fq ON cache_data(symbol, freq, fq)
        """)
        self.conn.commit()

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, pd.DataFrame, Optional[pd.Timestamp]]]:
        row = self.conn.execute(
            "SELECT symbol, name, data, expire_at FROM cache_data WHERE cache_key = ?",
            (base_key,),
        ).fetchone()
        if not row:
            return None
        symbol, name, data_blob, expire_at_str = row
        df = pickle.loads(data_blob)
        expire_at = pd.to_datetime(expire_at_str) if expire_at_str else None
        return (symbol, name, df, expire_at)

    def put(
        self,
        base_key: str,
        symbol: str,
        name: str,
        df: pd.DataFrame,
        earliest_date: Optional[str],
        latest_date: Optional[str],
        freq: str,
        fq: str,
        expire_at: Optional[pd.Timestamp],
    ) -> None:
        data_blob = pickle.dumps(df)
        updated_at = pd.Timestamp.now().isoformat()
        expire_at_str = expire_at.isoformat() if expire_at else None
        self.conn.execute("""
            INSERT OR REPLACE INTO cache_data
            (cache_key, symbol, name, data, earliest_date, latest_date, freq, fq, updated_at, expire_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (base_key, symbol, name, data_blob, earliest_date, latest_date, freq, fq, updated_at, expire_at_str))
        self.conn.commit()

    def delete(self, base_key: str) -> None:
        self.conn.execute("DELETE FROM cache_data WHERE cache_key = ?", (base_key,))
        self.conn.commit()

    def clear(self) -> None:
        self.conn.execute("DELETE FROM cache_data")
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


# ---------- 默认实现：JSONL（单文件，每行一条 JSON） ----------

class JsonlBackend:
    """使用单文件 JSONL 的存储后端（每行一个 JSON 对象，data 为 base64 的 pickle）。"""

    def __init__(self, path: str):
        self.path = path
        self._data: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if not Path(self.path).exists():
            self._data = {}
            return
        self._data = {}
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    key = obj.get("cache_key")
                    if key:
                        self._data[key] = obj
                except json.JSONDecodeError:
                    continue

    def _save(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            for base_key, obj in self._data.items():
                obj = dict(obj)
                obj["cache_key"] = base_key
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, pd.DataFrame, Optional[pd.Timestamp]]]:
        if base_key not in self._data:
            return None
        obj = self._data[base_key]
        data_b64 = obj.get("data")
        if not data_b64:
            return None
        df = pickle.loads(base64.b64decode(data_b64))
        symbol = obj.get("symbol", "")
        name = obj.get("name", "")
        expire_at_str = obj.get("expire_at")
        expire_at = pd.to_datetime(expire_at_str) if expire_at_str else None
        return (symbol, name, df, expire_at)

    def put(
        self,
        base_key: str,
        symbol: str,
        name: str,
        df: pd.DataFrame,
        earliest_date: Optional[str],
        latest_date: Optional[str],
        freq: str,
        fq: str,
        expire_at: Optional[pd.Timestamp],
    ) -> None:
        data_b64 = base64.b64encode(pickle.dumps(df)).decode("ascii")
        self._data[base_key] = {
            "cache_key": base_key,
            "symbol": symbol,
            "name": name,
            "data": data_b64,
            "earliest_date": earliest_date,
            "latest_date": latest_date,
            "freq": freq,
            "fq": fq,
            "updated_at": pd.Timestamp.now().isoformat(),
            "expire_at": expire_at.isoformat() if expire_at else None,
        }
        self._save()

    def delete(self, base_key: str) -> None:
        if base_key in self._data:
            del self._data[base_key]
            self._save()

    def clear(self) -> None:
        self._data.clear()
        self._save()

    def close(self) -> None:
        pass  # 无长连接，可选再调用 _save() 一次


# ---------- 默认实现：按市场分片 JSONL ----------

class MarketJsonlBackend:
    """
    按市场分片的 JSONL 存储后端。
    - cn -> A股/基金等默认市场
    - hk -> 港股
    - us -> 美股
    - fu -> 期货
    """

    def __init__(
        self,
        market_paths: Optional[dict[str, str]] = None,
        route_fn: Optional[Callable[[str], str]] = None,
    ):
        home = Path.home()
        cache_dir = home / ".rquote"
        cache_dir.mkdir(exist_ok=True)
        self.market_paths = market_paths or {
            "cn": str(cache_dir / "cache_cn.jsonl"),
            "hk": str(cache_dir / "cache_hk.jsonl"),
            "us": str(cache_dir / "cache_us.jsonl"),
            "fu": str(cache_dir / "cache_fu.jsonl"),
        }
        self._route_fn = route_fn or self._default_route
        self._backends = {m: JsonlBackend(p) for m, p in self.market_paths.items()}
        self._fallback_market = "cn"

    @staticmethod
    def _default_route(symbol: str) -> str:
        s = str(symbol or "").lower()
        if s.startswith("us"):
            return "us"
        if s.startswith("hk"):
            return "hk"
        if s.startswith("fu"):
            return "fu"
        return "cn"

    @staticmethod
    def _symbol_from_key(base_key: str) -> str:
        parts = base_key.split(":")
        return parts[0] if parts else ""

    def _backend_for_symbol(self, symbol: str) -> JsonlBackend:
        market = self._route_fn(symbol)
        if market in self._backends:
            return self._backends[market]
        return self._backends[self._fallback_market]

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, pd.DataFrame, Optional[pd.Timestamp]]]:
        symbol = self._symbol_from_key(base_key)
        backend = self._backend_for_symbol(symbol)
        return backend.get_raw(base_key)

    def put(
        self,
        base_key: str,
        symbol: str,
        name: str,
        df: pd.DataFrame,
        earliest_date: Optional[str],
        latest_date: Optional[str],
        freq: str,
        fq: str,
        expire_at: Optional[pd.Timestamp],
    ) -> None:
        backend = self._backend_for_symbol(symbol)
        backend.put(base_key, symbol, name, df, earliest_date, latest_date, freq, fq, expire_at)

    def delete(self, base_key: str) -> None:
        symbol = self._symbol_from_key(base_key)
        backend = self._backend_for_symbol(symbol)
        backend.delete(base_key)

    def clear(self) -> None:
        for backend in self._backends.values():
            backend.clear()

    def close(self) -> None:
        for backend in self._backends.values():
            backend.close()

    def status_rows(self, symbols: Optional[list[str]] = None) -> list[dict[str, Any]]:
        target_set = set(symbols) if symbols else None
        rows: list[dict[str, Any]] = []
        for market, path in self.market_paths.items():
            p = Path(path)
            if not p.exists():
                continue
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        obj = json.loads(s)
                    except Exception:
                        continue
                    symbol = obj.get("symbol")
                    if not symbol:
                        continue
                    if target_set is not None and symbol not in target_set:
                        continue
                    payload = obj.get("data")
                    nrows = -1
                    if payload:
                        try:
                            df = pickle.loads(base64.b64decode(payload))
                            nrows = len(df)
                        except Exception:
                            nrows = -1
                    rows.append(
                        {
                            "market": market,
                            "symbol": symbol,
                            "earliest_date": obj.get("earliest_date"),
                            "latest_date": obj.get("latest_date"),
                            "rows": nrows,
                        }
                    )
        rows.sort(key=lambda x: x["symbol"])
        return rows

# ---------- 可选实现：Pickle（兼容旧 use_duckdb=False） ----------

class PickleBackend:
    """单文件 pickle 字典存储，兼容旧版 PersistentCache(use_duckdb=False)。"""

    def __init__(self, path: str):
        self.path = path
        if Path(path).exists():
            try:
                with open(path, "rb") as f:
                    self._data = pickle.load(f)
            except Exception:
                self._data = {}
        else:
            self._data = {}

    def _save(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "wb") as f:
            pickle.dump(self._data, f)

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, pd.DataFrame, Optional[pd.Timestamp]]]:
        if base_key not in self._data:
            return None
        entry = self._data[base_key]
        symbol = entry.get("symbol", base_key.split(":")[0] if ":" in base_key else "")
        name = entry.get("name", "")
        df = entry.get("data")
        if df is None or not isinstance(df, pd.DataFrame):
            return None
        expire_at = entry.get("expire_at")
        return (symbol, name, df, expire_at)

    def put(
        self,
        base_key: str,
        symbol: str,
        name: str,
        df: pd.DataFrame,
        earliest_date: Optional[str],
        latest_date: Optional[str],
        freq: str,
        fq: str,
        expire_at: Optional[pd.Timestamp],
    ) -> None:
        self._data[base_key] = {
            "symbol": symbol,
            "name": name,
            "data": df,
            "earliest_date": earliest_date,
            "latest_date": latest_date,
            "freq": freq,
            "fq": fq,
            "updated_at": pd.Timestamp.now(),
            "expire_at": expire_at,
        }
        self._save()

    def delete(self, base_key: str) -> None:
        if base_key in self._data:
            del self._data[base_key]
            self._save()

    def clear(self) -> None:
        self._data.clear()
        self._save()

    def close(self) -> None:
        pass
