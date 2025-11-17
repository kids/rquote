# -*- coding: utf-8 -*-
"""
持久化缓存实现
"""
import os
import time
from pathlib import Path
from typing import Optional, Any, Tuple
import pandas as pd
from .base import Cache

# 尝试导入 duckdb（可选依赖）
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    duckdb = None


class PersistentCache(Cache):
    """持久化缓存实现，使用 duckdb 或文件系统存储数据"""
    
    def __init__(self, db_path: Optional[str] = None, use_duckdb: bool = True, ttl: Optional[int] = None):
        """
        初始化持久化缓存
        
        Args:
            db_path: 数据库文件路径，默认为 ~/.rquote/cache.db
            use_duckdb: 是否使用 duckdb（如果可用），否则使用 pickle 文件
            ttl: 默认过期时间（秒），None 表示不过期
        """
        self.use_duckdb = use_duckdb and DUCKDB_AVAILABLE
        self.ttl = ttl
        
        if db_path is None:
            # 默认路径：~/.rquote/cache.db 或 ~/.rquote/cache.pkl
            home = Path.home()
            cache_dir = home / '.rquote'
            cache_dir.mkdir(exist_ok=True)
            if self.use_duckdb:
                db_path = str(cache_dir / 'cache.db')
            else:
                db_path = str(cache_dir / 'cache.pkl')
        
        self.db_path = db_path
        
        if self.use_duckdb:
            self._init_duckdb()
        else:
            self._init_pickle()
    
    def _init_duckdb(self):
        """初始化 duckdb 数据库"""
        self.conn = duckdb.connect(self.db_path)
        # 创建缓存表
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
                updated_at TIMESTAMP,
                expire_at TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_freq_fq 
            ON cache_data(symbol, freq, fq)
        """)
    
    def _init_pickle(self):
        """初始化 pickle 存储"""
        import pickle
        self.pickle = pickle
        if os.path.exists(self.db_path):
            try:
                with open(self.db_path, 'rb') as f:
                    self._cache_data = self.pickle.load(f)
            except:
                self._cache_data = {}
        else:
            self._cache_data = {}
    
    def _save_pickle(self):
        """保存 pickle 数据"""
        import pickle
        with open(self.db_path, 'wb') as f:
            self.pickle.dump(self._cache_data, f)
    
    def _extract_key_parts(self, key: str) -> Tuple[str, str, str, str, str]:
        """
        从完整 key 中提取各部分
        
        Args:
            key: 完整 key，格式如 "symbol:sdate:edate:freq:days:fq"
        
        Returns:
            (symbol, sdate, edate, freq, fq)
        """
        parts = key.split(':')
        if len(parts) >= 6:
            return parts[0], parts[1], parts[2], parts[3], parts[5]
        elif len(parts) >= 4:
            return parts[0], parts[1] if len(parts) > 1 else '', parts[2] if len(parts) > 2 else '', parts[3], parts[4] if len(parts) > 4 else 'qfq'
        else:
            return parts[0] if parts else '', '', '', 'day', 'qfq'
    
    def _get_base_key(self, symbol: str, freq: str, fq: str) -> str:
        """生成基础 key（不包含日期）"""
        return f"{symbol}:{freq}:{fq}"
    
    def _parse_date(self, date_str: str) -> Optional[pd.Timestamp]:
        """解析日期字符串"""
        if not date_str:
            return None
        try:
            return pd.to_datetime(date_str)
        except:
            return None
    
    def _get_dataframe_date_range(self, df: pd.DataFrame) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        """获取 DataFrame 的日期范围"""
        if df.empty or not isinstance(df.index, pd.DatetimeIndex):
            return None, None
        return df.index.min(), df.index.max()
    
    def _filter_dataframe_by_date(self, df: pd.DataFrame, sdate: Optional[str] = None, 
                                   edate: Optional[str] = None) -> pd.DataFrame:
        """根据日期范围过滤 DataFrame"""
        if df.empty:
            return df
        
        if not isinstance(df.index, pd.DatetimeIndex):
            return df
        
        start_date = self._parse_date(sdate) if sdate else None
        end_date = self._parse_date(edate) if edate else None
        
        if start_date is not None and end_date is not None:
            mask = (df.index >= start_date) & (df.index <= end_date)
            return df[mask]
        elif start_date is not None:
            return df[df.index >= start_date]
        elif end_date is not None:
            return df[df.index <= end_date]
        else:
            return df
    
    def _merge_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        """合并两个 DataFrame，去重并排序"""
        if df1.empty:
            return df2
        if df2.empty:
            return df1
        
        # 合并并去重
        combined = pd.concat([df1, df2])
        combined = combined[~combined.index.duplicated(keep='last')]
        combined = combined.sort_index()
        return combined
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存数据
        
        Args:
            key: 缓存 key，格式如 "symbol:sdate:edate:freq:days:fq"
        
        Returns:
            (symbol, name, DataFrame) 或 None
        """
        symbol, sdate, edate, freq, fq = self._extract_key_parts(key)
        base_key = self._get_base_key(symbol, freq, fq)
        
        if self.use_duckdb:
            return self._get_duckdb(base_key, symbol, sdate, edate, freq, fq)
        else:
            return self._get_pickle(base_key, symbol, sdate, edate, freq, fq)
    
    def _get_duckdb(self, base_key: str, symbol: str, sdate: str, edate: str, 
                    freq: str, fq: str) -> Optional[Tuple[str, str, pd.DataFrame]]:
        """从 duckdb 获取数据"""
        result = self.conn.execute("""
            SELECT name, data, earliest_date, latest_date, expire_at
            FROM cache_data
            WHERE cache_key = ?
        """, [base_key]).fetchone()
        
        if not result:
            return None
        
        name, data_blob, earliest_date, latest_date, expire_at = result
        
        # 检查过期
        if self.ttl and expire_at:
            expire_ts = pd.to_datetime(expire_at)
            if pd.Timestamp.now() > expire_ts:
                self.delete(base_key)
                return None
        
        # 反序列化 DataFrame
        import pickle
        df = pickle.loads(data_blob)
        
        # 获取缓存数据的日期范围
        cached_earliest = self._parse_date(earliest_date)
        cached_latest = self._parse_date(latest_date)
        
        # 如果请求的日期范围完全在缓存范围内，直接返回过滤后的数据
        request_sdate = self._parse_date(sdate) if sdate else None
        request_edate = self._parse_date(edate) if edate else None
        
        # 检查是否有重叠
        if request_edate and cached_earliest and request_edate < cached_earliest:
            # 请求的结束日期早于缓存的最早日期，无重叠
            return None
        if request_sdate and cached_latest and request_sdate > cached_latest:
            # 请求的开始日期晚于缓存的最晚日期，无重叠
            return None
        
        # 有重叠，返回缓存中可用的部分数据
        # 计算实际可用的日期范围
        actual_sdate = max(request_sdate, cached_earliest) if request_sdate and cached_earliest else (request_sdate or cached_earliest)
        actual_edate = min(request_edate, cached_latest) if request_edate and cached_latest else (request_edate or cached_latest)
        
        # 过滤数据
        filtered_df = self._filter_dataframe_by_date(
            df, 
            actual_sdate.strftime('%Y-%m-%d') if actual_sdate else None,
            actual_edate.strftime('%Y-%m-%d') if actual_edate else None
        )
        
        if filtered_df.empty:
            return None
        
        return (symbol, name, filtered_df)
    
    def _get_pickle(self, base_key: str, symbol: str, sdate: str, edate: str,
                    freq: str, fq: str) -> Optional[Tuple[str, str, pd.DataFrame]]:
        """从 pickle 文件获取数据"""
        if base_key not in self._cache_data:
            return None
        
        cache_entry = self._cache_data[base_key]
        
        # 检查过期
        if self.ttl and 'expire_at' in cache_entry:
            expire_ts = cache_entry['expire_at']
            if pd.Timestamp.now() > expire_ts:
                del self._cache_data[base_key]
                self._save_pickle()
                return None
        
        df = cache_entry['data']
        name = cache_entry.get('name', '')
        earliest_date = cache_entry.get('earliest_date')
        latest_date = cache_entry.get('latest_date')
        
        # 获取缓存数据的日期范围
        cached_earliest = self._parse_date(earliest_date)
        cached_latest = self._parse_date(latest_date)
        
        # 如果请求的日期范围完全在缓存范围内，直接返回过滤后的数据
        request_sdate = self._parse_date(sdate) if sdate else None
        request_edate = self._parse_date(edate) if edate else None
        
        # 检查是否有重叠
        if request_edate and cached_earliest and request_edate < cached_earliest:
            # 请求的结束日期早于缓存的最早日期，无重叠
            return None
        if request_sdate and cached_latest and request_sdate > cached_latest:
            # 请求的开始日期晚于缓存的最晚日期，无重叠
            return None
        
        # 有重叠，返回缓存中可用的部分数据
        # 计算实际可用的日期范围
        actual_sdate = max(request_sdate, cached_earliest) if request_sdate and cached_earliest else (request_sdate or cached_earliest)
        actual_edate = min(request_edate, cached_latest) if request_edate and cached_latest else (request_edate or cached_latest)
        
        # 过滤数据
        filtered_df = self._filter_dataframe_by_date(
            df,
            actual_sdate.strftime('%Y-%m-%d') if actual_sdate else None,
            actual_edate.strftime('%Y-%m-%d') if actual_edate else None
        )
        
        if filtered_df.empty:
            return None
        
        return (symbol, name, filtered_df)
    
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        存储缓存数据
        
        Args:
            key: 缓存 key
            value: (symbol, name, DataFrame) 元组
            ttl: 过期时间（秒）
        """
        if not isinstance(value, tuple) or len(value) != 3:
            return
        
        symbol, name, df = value
        if not isinstance(df, pd.DataFrame) or df.empty:
            return
        
        _, _, _, freq, fq = self._extract_key_parts(key)
        base_key = self._get_base_key(symbol, freq, fq)
        
        # 尝试从基础 key 获取完整数据并合并
        existing = self._get_raw(base_key)
        if existing:
            _, existing_name, existing_df = existing
            # 使用新数据的 name（如果有）
            if not name:
                name = existing_name
            # 合并数据
            df = self._merge_dataframes(existing_df, df)
        
        # 获取日期范围
        earliest_date, latest_date = self._get_dataframe_date_range(df)
        earliest_str = earliest_date.strftime('%Y-%m-%d') if earliest_date else None
        latest_str = latest_date.strftime('%Y-%m-%d') if latest_date else None
        
        # 计算过期时间
        expire_at = None
        if ttl or self.ttl:
            expire_seconds = (ttl or self.ttl)
            expire_at = pd.Timestamp.now() + pd.Timedelta(seconds=expire_seconds)
        
        if self.use_duckdb:
            self._put_duckdb(base_key, symbol, name, df, earliest_str, latest_str, freq, fq, expire_at)
        else:
            self._put_pickle(base_key, symbol, name, df, earliest_str, latest_str, freq, fq, expire_at)
    
    def _get_raw(self, base_key: str) -> Optional[Tuple[str, str, pd.DataFrame]]:
        """获取原始数据（不进行日期过滤）"""
        if self.use_duckdb:
            result = self.conn.execute("""
                SELECT name, data
                FROM cache_data
                WHERE cache_key = ?
            """, [base_key]).fetchone()
            
            if not result:
                return None
            
            import pickle
            df = pickle.loads(result[1])
            return (base_key.split(':')[0], result[0], df)
        else:
            if base_key not in self._cache_data:
                return None
            cache_entry = self._cache_data[base_key]
            return (base_key.split(':')[0], cache_entry.get('name', ''), cache_entry['data'])
    
    def _put_duckdb(self, base_key: str, symbol: str, name: str, df: pd.DataFrame,
                     earliest_date: Optional[str], latest_date: Optional[str],
                     freq: str, fq: str, expire_at: Optional[pd.Timestamp]):
        """存储到 duckdb"""
        import pickle
        data_blob = pickle.dumps(df)
        
        self.conn.execute("""
            INSERT OR REPLACE INTO cache_data 
            (cache_key, symbol, name, data, earliest_date, latest_date, freq, fq, updated_at, expire_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [base_key, symbol, name, data_blob, earliest_date, latest_date, freq, fq,
              pd.Timestamp.now(), expire_at])
        self.conn.commit()
    
    def _put_pickle(self, base_key: str, symbol: str, name: str, df: pd.DataFrame,
                    earliest_date: Optional[str], latest_date: Optional[str],
                    freq: str, fq: str, expire_at: Optional[pd.Timestamp]):
        """存储到 pickle 文件"""
        self._cache_data[base_key] = {
            'symbol': symbol,
            'name': name,
            'data': df,
            'earliest_date': earliest_date,
            'latest_date': latest_date,
            'freq': freq,
            'fq': fq,
            'updated_at': pd.Timestamp.now(),
            'expire_at': expire_at
        }
        self._save_pickle()
    
    def delete(self, key: str) -> None:
        """删除缓存"""
        symbol, _, _, freq, fq = self._extract_key_parts(key)
        base_key = self._get_base_key(symbol, freq, fq)
        
        if self.use_duckdb:
            self.conn.execute("DELETE FROM cache_data WHERE cache_key = ?", [base_key])
            self.conn.commit()
        else:
            if base_key in self._cache_data:
                del self._cache_data[base_key]
                self._save_pickle()
    
    def clear(self) -> None:
        """清空所有缓存"""
        if self.use_duckdb:
            self.conn.execute("DELETE FROM cache_data")
            self.conn.commit()
        else:
            self._cache_data.clear()
            self._save_pickle()
    
    def close(self):
        """关闭连接"""
        if self.use_duckdb:
            self.conn.close()

