# -*- coding: utf-8 -*-
"""
持久化存储后端抽象与默认实现。
"""
from __future__ import annotations

import datetime
import json
import pickle
import sqlite3
import threading
from pathlib import Path
from typing import Optional, Tuple, Protocol, Any, Callable, List, Dict


class StorageBackend(Protocol):
    """持久化存储后端协议。"""

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, List[Dict], Optional[datetime.datetime]]]:
        ...

    def put(
        self,
        base_key: str,
        symbol: str,
        name: str,
        records: List[Dict],
        earliest_date: Optional[str],
        latest_date: Optional[str],
        freq: str,
        fq: str,
        expire_at: Optional[datetime.datetime],
    ) -> None:
        ...

    def delete(self, base_key: str) -> None:
        ...

    def clear(self) -> None:
        ...

    def close(self) -> None:
        ...


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
                data TEXT,
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

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, List[Dict], Optional[datetime.datetime]]]:
        row = self.conn.execute(
            "SELECT symbol, name, data, expire_at FROM cache_data WHERE cache_key = ?",
            (base_key,),
        ).fetchone()
        if not row:
            return None
        symbol, name, data_text, expire_at_str = row
        try:
            records = json.loads(data_text) if data_text else []
        except (json.JSONDecodeError, TypeError):
            records = []
        expire_at = datetime.datetime.fromisoformat(expire_at_str) if expire_at_str else None
        return (symbol, name, records, expire_at)

    def put(
        self,
        base_key: str,
        symbol: str,
        name: str,
        records: List[Dict],
        earliest_date: Optional[str],
        latest_date: Optional[str],
        freq: str,
        fq: str,
        expire_at: Optional[datetime.datetime],
    ) -> None:
        data_text = json.dumps(records, ensure_ascii=False)
        updated_at = datetime.datetime.now().isoformat()
        expire_at_str = expire_at.isoformat() if expire_at else None
        self.conn.execute("""
            INSERT OR REPLACE INTO cache_data
            (cache_key, symbol, name, data, earliest_date, latest_date, freq, fq, updated_at, expire_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (base_key, symbol, name, data_text, earliest_date, latest_date, freq, fq, updated_at, expire_at_str))
        self.conn.commit()

    def delete(self, base_key: str) -> None:
        self.conn.execute("DELETE FROM cache_data WHERE cache_key = ?", (base_key,))
        self.conn.commit()

    def clear(self) -> None:
        self.conn.execute("DELETE FROM cache_data")
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class JsonlBackend:
    """使用单文件 JSONL 的存储后端。records 字段直接存 list[dict]（原生 JSON）。

    写入策略：put() 追加写入（O(1)），_load() 后者覆盖前者（last-write-wins）。
    compact()/close() 时整体重写，消除追加产生的冗余行。
    """

    def __init__(self, path: str):
        self.path = path
        self._data: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._append_count = 0
        self._batch_mode = False
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
        self._append_count = 0

    def _append_entry(self, base_key: str) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        obj = dict(self._data[base_key])
        obj["cache_key"] = base_key
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        self._append_count += 1

    def set_batch_mode(self, on: bool) -> None:
        self._batch_mode = on

    def flush_batch(self) -> None:
        with self._lock:
            if self._batch_mode or self._append_count > 0:
                self._save()
            self._batch_mode = False

    def compact(self) -> None:
        with self._lock:
            if self._append_count > 0:
                self._save()

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, List[Dict], Optional[datetime.datetime]]]:
        with self._lock:
            if base_key not in self._data:
                return None
            obj = self._data[base_key]
            records = obj.get("records", [])
            if not isinstance(records, list):
                records = []
            symbol = obj.get("symbol", "")
            name = obj.get("name", "")
            expire_at_str = obj.get("expire_at")
            expire_at = datetime.datetime.fromisoformat(expire_at_str) if expire_at_str else None
            return (symbol, name, records, expire_at)

    def put(
        self,
        base_key: str,
        symbol: str,
        name: str,
        records: List[Dict],
        earliest_date: Optional[str],
        latest_date: Optional[str],
        freq: str,
        fq: str,
        expire_at: Optional[datetime.datetime],
    ) -> None:
        entry = {
            "cache_key": base_key,
            "symbol": symbol,
            "name": name,
            "records": records,
            "earliest_date": earliest_date,
            "latest_date": latest_date,
            "freq": freq,
            "fq": fq,
            "updated_at": datetime.datetime.now().isoformat(),
            "expire_at": expire_at.isoformat() if expire_at else None,
        }
        with self._lock:
            self._data[base_key] = entry
            if not self._batch_mode:
                self._append_entry(base_key)

    def delete(self, base_key: str) -> None:
        with self._lock:
            if base_key in self._data:
                del self._data[base_key]
                self._save()

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self._save()

    def close(self) -> None:
        with self._lock:
            if self._append_count > 0:
                self._save()


class JsonlBackendLazy:
    """
    只读 JSONL 后端：按 offset 索引，get_raw 时再读那一行。
    put/delete/clear 为 no-op（只读）。
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self._index: dict[str, Tuple[int, int, Optional[str], Optional[str]]] = {}
        self._build_index()

    def _build_index(self) -> None:
        if not self.path.exists():
            return
        self._index = {}
        with open(self.path, "rb") as f:
            while True:
                offset = f.tell()
                line = f.readline()
                if not line:
                    break
                line_len = len(line)
                try:
                    obj = json.loads(line.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                key = obj.get("cache_key")
                if not key:
                    continue
                self._index[key] = (
                    offset,
                    line_len,
                    obj.get("earliest_date"),
                    obj.get("latest_date"),
                )

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, List[Dict], Optional[datetime.datetime]]]:
        if base_key not in self._index:
            return None
        offset, length, _, _ = self._index[base_key]
        try:
            with open(self.path, "rb") as f:
                f.seek(offset)
                raw = f.read(length)
            obj = json.loads(raw.decode("utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return None
        records = obj.get("records", [])
        if not isinstance(records, list):
            records = []
        symbol = obj.get("symbol", "")
        name = obj.get("name", "")
        expire_at_str = obj.get("expire_at")
        expire_at = datetime.datetime.fromisoformat(expire_at_str) if expire_at_str else None
        return (symbol, name, records, expire_at)

    def put(self, base_key, symbol, name, records, earliest_date, latest_date, freq, fq, expire_at):
        pass

    def delete(self, base_key: str) -> None:
        pass

    def clear(self) -> None:
        pass

    def compact(self) -> None:
        pass

    def close(self) -> None:
        pass


class MarketJsonlBackend:
    """按市场分片的 JSONL 存储后端。"""

    def __init__(
        self,
        market_paths: Optional[dict[str, str]] = None,
        route_fn: Optional[Callable[[str], str]] = None,
        backend_class: type = JsonlBackend,
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
        self._backends = {m: backend_class(p) for m, p in self.market_paths.items()}
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

    def _backend_for_symbol(self, symbol: str):
        market = self._route_fn(symbol)
        if market in self._backends:
            return self._backends[market]
        return self._backends[self._fallback_market]

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, List[Dict], Optional[datetime.datetime]]]:
        symbol = self._symbol_from_key(base_key)
        return self._backend_for_symbol(symbol).get_raw(base_key)

    def put(self, base_key, symbol, name, records, earliest_date, latest_date, freq, fq, expire_at) -> None:
        self._backend_for_symbol(symbol).put(base_key, symbol, name, records, earliest_date, latest_date, freq, fq, expire_at)

    def delete(self, base_key: str) -> None:
        symbol = self._symbol_from_key(base_key)
        self._backend_for_symbol(symbol).delete(base_key)

    def clear(self) -> None:
        for backend in self._backends.values():
            backend.clear()

    def set_batch_mode(self, on: bool) -> None:
        for backend in self._backends.values():
            if hasattr(backend, "set_batch_mode"):
                backend.set_batch_mode(on)

    def flush_batch(self) -> None:
        for backend in self._backends.values():
            if hasattr(backend, "flush_batch"):
                backend.flush_batch()

    def compact(self) -> None:
        for backend in self._backends.values():
            backend.compact()

    def close(self) -> None:
        for backend in self._backends.values():
            backend.close()

    def status_rows(self, symbols: Optional[list[str]] = None) -> list[dict[str, Any]]:
        target_set = set(symbols) if symbols else None
        rows: list[dict[str, Any]] = []
        for market, backend in self._backends.items():
            if hasattr(backend, "_index"):
                for base_key, entry in backend._index.items():
                    offset, length, earliest, latest = entry
                    symbol = base_key.split(":")[0] if ":" in base_key else base_key
                    if target_set is not None and symbol not in target_set:
                        continue
                    rows.append({
                        "market": market,
                        "symbol": symbol,
                        "earliest_date": earliest,
                        "latest_date": latest,
                        "rows": -1,
                    })
                continue
            with backend._lock:
                data_snapshot = dict(backend._data)
            for base_key, obj in data_snapshot.items():
                symbol = obj.get("symbol")
                if not symbol:
                    continue
                if target_set is not None and symbol not in target_set:
                    continue
                records = obj.get("records", [])
                nrows = len(records) if isinstance(records, list) else -1
                rows.append({
                    "market": market,
                    "symbol": symbol,
                    "earliest_date": obj.get("earliest_date"),
                    "latest_date": obj.get("latest_date"),
                    "rows": nrows,
                })
        rows.sort(key=lambda x: x["symbol"])
        return rows


class PickleBackend:
    """单文件 pickle 字典存储。"""

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

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, List[Dict], Optional[datetime.datetime]]]:
        if base_key not in self._data:
            return None
        entry = self._data[base_key]
        symbol = entry.get("symbol", base_key.split(":")[0] if ":" in base_key else "")
        name = entry.get("name", "")
        records = entry.get("data", [])
        if not isinstance(records, list):
            records = []
        expire_at = entry.get("expire_at")
        # migrate old datetime -> datetime.datetime
        if expire_at is not None and not isinstance(expire_at, datetime.datetime):
            try:
                expire_at = datetime.datetime.fromisoformat(str(expire_at))
            except Exception:
                expire_at = None
        return (symbol, name, records, expire_at)

    def put(
        self,
        base_key: str,
        symbol: str,
        name: str,
        records: List[Dict],
        earliest_date: Optional[str],
        latest_date: Optional[str],
        freq: str,
        fq: str,
        expire_at: Optional[datetime.datetime],
    ) -> None:
        self._data[base_key] = {
            "symbol": symbol,
            "name": name,
            "data": records,
            "earliest_date": earliest_date,
            "latest_date": latest_date,
            "freq": freq,
            "fq": fq,
            "updated_at": datetime.datetime.now(),
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


def _base_key_to_filename(base_key: str) -> str:
    return base_key.replace(":", "_") + ".json"


class PerKeyJsonBackend:
    """每 key 一个 JSON 文件，不区分市场。"""

    def __init__(self, path: str):
        self.root = Path(path)

    def _path_for(self, base_key: str) -> Path:
        return self.root / _base_key_to_filename(base_key)

    def get_raw(self, base_key: str) -> Optional[Tuple[str, str, List[Dict], Optional[datetime.datetime]]]:
        p = self._path_for(base_key)
        if not p.exists():
            return None
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except (OSError, json.JSONDecodeError):
            return None
        records = obj.get("records", [])
        if not isinstance(records, list):
            records = []
        symbol = obj.get("symbol", "")
        name = obj.get("name", "")
        expire_at_str = obj.get("expire_at")
        expire_at = datetime.datetime.fromisoformat(expire_at_str) if expire_at_str else None
        return (symbol, name, records, expire_at)

    def put(
        self,
        base_key: str,
        symbol: str,
        name: str,
        records: List[Dict],
        earliest_date: Optional[str],
        latest_date: Optional[str],
        freq: str,
        fq: str,
        expire_at: Optional[datetime.datetime],
    ) -> None:
        obj = {
            "cache_key": base_key,
            "symbol": symbol,
            "name": name,
            "records": records,
            "earliest_date": earliest_date,
            "latest_date": latest_date,
            "freq": freq,
            "fq": fq,
            "updated_at": datetime.datetime.now().isoformat(),
            "expire_at": expire_at.isoformat() if expire_at else None,
        }
        self.root.mkdir(parents=True, exist_ok=True)
        p = self._path_for(base_key)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)

    def delete(self, base_key: str) -> None:
        p = self._path_for(base_key)
        if p.exists():
            p.unlink()

    def clear(self) -> None:
        if not self.root.exists():
            return
        for p in self.root.glob("*.json"):
            p.unlink()

    def close(self) -> None:
        pass

    def status_rows(self, symbols: Optional[list[str]] = None) -> list[dict[str, Any]]:
        target_set = set(symbols) if symbols else None
        rows: list[dict[str, Any]] = []
        if not self.root.exists():
            return rows
        for p in self.root.glob("*.json"):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    obj = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue
            symbol = obj.get("symbol")
            if not symbol:
                symbol = obj.get("cache_key", "").split(":")[0]
            if target_set is not None and symbol not in target_set:
                continue
            records = obj.get("records", [])
            nrows = len(records) if isinstance(records, list) else -1
            rows.append({
                "market": "",
                "symbol": symbol,
                "earliest_date": obj.get("earliest_date"),
                "latest_date": obj.get("latest_date"),
                "rows": nrows,
            })
        rows.sort(key=lambda x: x["symbol"])
        return rows
