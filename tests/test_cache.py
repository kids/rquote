# -*- coding: utf-8 -*-
"""
缓存模块测试
"""
import unittest
import time
import os
import tempfile
from rquote.cache import MemoryCache, Cache
from rquote.cache.memory import DictCache

try:
    from rquote.cache import PersistentCache
    PERSISTENT_CACHE_AVAILABLE = True
except ImportError:
    PERSISTENT_CACHE_AVAILABLE = False
    PersistentCache = None


def _make_records(dates):
    """创建测试用 list[dict]"""
    return [
        {
            'date': d,
            'open': 100.0 + i,
            'high': 105.0 + i,
            'low': 99.0 + i,
            'close': 104.0 + i,
            'volume': 1000 + i * 100,
        }
        for i, d in enumerate(dates)
    ]


class TestMemoryCache(unittest.TestCase):

    def setUp(self):
        self.cache = MemoryCache(ttl=1)

    def test_put_get(self):
        self.cache.put('key1', 'value1')
        self.assertEqual(self.cache.get('key1'), 'value1')

    def test_expire(self):
        self.cache.put('key2', 'value2', ttl=0.1)
        time.sleep(0.2)
        self.assertIsNone(self.cache.get('key2'))

    def test_delete(self):
        self.cache.put('key3', 'value3')
        self.cache.delete('key3')
        self.assertIsNone(self.cache.get('key3'))

    def test_clear(self):
        self.cache.put('key4', 'value4')
        self.cache.put('key5', 'value5')
        self.cache.clear()
        self.assertEqual(self.cache.size(), 0)

    def test_size(self):
        self.cache.put('key6', 'value6')
        self.cache.put('key7', 'value7')
        self.assertEqual(self.cache.size(), 2)


class TestDictCache(unittest.TestCase):

    def setUp(self):
        self.dict_cache = {}
        self.cache = DictCache(self.dict_cache)

    def test_put_get(self):
        self.cache.put('key1', 'value1')
        self.assertEqual(self.cache.get('key1'), 'value1')
        self.assertEqual(self.dict_cache['key1'], 'value1')

    def test_delete(self):
        self.cache.put('key2', 'value2')
        self.cache.delete('key2')
        self.assertIsNone(self.cache.get('key2'))


@unittest.skipIf(not PERSISTENT_CACHE_AVAILABLE, "持久化缓存不可用")
class TestPersistentCache(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path_sqlite = os.path.join(self.temp_dir, 'test_cache.db')
        self.db_path_pickle = os.path.join(self.temp_dir, 'test_cache.pkl')
        self.test_records = _make_records(['2024-01-01', '2024-01-02', '2024-01-03'])

    def tearDown(self):
        if hasattr(self, 'cache_sqlite') and self.cache_sqlite:
            try:
                self.cache_sqlite.close()
            except Exception:
                pass
        if hasattr(self, 'cache_pickle') and self.cache_pickle:
            try:
                self.cache_pickle.close()
            except Exception:
                pass
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_put_get_sqlite(self):
        self.cache_sqlite = PersistentCache(db_path=self.db_path_sqlite, use_duckdb=True)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_records)

        self.cache_sqlite.put(key, value)
        result = self.cache_sqlite.get(key)

        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'test_symbol')
        self.assertEqual(result[1], '测试股票')
        self.assertEqual(len(result[2]), 3)
        self.assertEqual(result[2][0]['date'], '2024-01-01')
        self.assertEqual(result[2][0]['close'], 104.0)

    def test_put_get_pickle(self):
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_records)

        self.cache_pickle.put(key, value)
        result = self.cache_pickle.get(key)

        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'test_symbol')
        self.assertEqual(result[1], '测试股票')
        self.assertEqual(len(result[2]), 3)

    def test_delete_sqlite(self):
        self.cache_sqlite = PersistentCache(db_path=self.db_path_sqlite, use_duckdb=True)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_records)

        self.cache_sqlite.put(key, value)
        self.assertIsNotNone(self.cache_sqlite.get(key))
        self.cache_sqlite.delete(key)
        self.assertIsNone(self.cache_sqlite.get(key))

    def test_delete_pickle(self):
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_records)

        self.cache_pickle.put(key, value)
        self.assertIsNotNone(self.cache_pickle.get(key))
        self.cache_pickle.delete(key)
        self.assertIsNone(self.cache_pickle.get(key))

    def test_clear_sqlite(self):
        self.cache_sqlite = PersistentCache(db_path=self.db_path_sqlite, use_duckdb=True)
        key1 = 'symbol1:2024-01-01:2024-01-03:day:3:qfq'
        key2 = 'symbol2:2024-01-01:2024-01-03:day:3:qfq'
        r1 = _make_records(['2024-01-01', '2024-01-02', '2024-01-03'])
        r2 = _make_records(['2024-01-01', '2024-01-02', '2024-01-03'])

        self.cache_sqlite.put(key1, ('symbol1', '股票1', r1))
        self.cache_sqlite.put(key2, ('symbol2', '股票2', r2))
        self.assertIsNotNone(self.cache_sqlite.get(key1))
        self.assertIsNotNone(self.cache_sqlite.get(key2))

        self.cache_sqlite.clear()
        self.assertIsNone(self.cache_sqlite.get(key1))
        self.assertIsNone(self.cache_sqlite.get(key2))

    def test_clear_pickle(self):
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False)
        key1 = 'symbol1:2024-01-01:2024-01-03:day:3:qfq'
        key2 = 'symbol2:2024-01-01:2024-01-03:day:3:qfq'
        r1 = _make_records(['2024-01-01', '2024-01-02', '2024-01-03'])
        r2 = _make_records(['2024-01-01', '2024-01-02', '2024-01-03'])

        self.cache_pickle.put(key1, ('symbol1', '股票1', r1))
        self.cache_pickle.put(key2, ('symbol2', '股票2', r2))
        self.assertIsNotNone(self.cache_pickle.get(key1))
        self.assertIsNotNone(self.cache_pickle.get(key2))

        self.cache_pickle.clear()
        self.assertIsNone(self.cache_pickle.get(key1))
        self.assertIsNone(self.cache_pickle.get(key2))

    def test_expire_sqlite(self):
        self.cache_sqlite = PersistentCache(db_path=self.db_path_sqlite, use_duckdb=True, ttl=1)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_records)

        self.cache_sqlite.put(key, value, ttl=0.1)
        self.assertIsNotNone(self.cache_sqlite.get(key))
        time.sleep(0.2)
        self.assertIsNone(self.cache_sqlite.get(key))

    def test_expire_pickle(self):
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False, ttl=1)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_records)

        self.cache_pickle.put(key, value, ttl=0.1)
        self.assertIsNotNone(self.cache_pickle.get(key))
        time.sleep(0.2)
        self.assertIsNone(self.cache_pickle.get(key))

    def test_date_range_filter_sqlite(self):
        self.cache_sqlite = PersistentCache(db_path=self.db_path_sqlite, use_duckdb=True)
        full_key = 'test_symbol::2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_records)
        self.cache_sqlite.put(full_key, value)

        partial_key = 'test_symbol:2024-01-02:2024-01-03:day:2:qfq'
        result = self.cache_sqlite.get(partial_key)

        self.assertIsNotNone(result)
        self.assertEqual(len(result[2]), 2)
        self.assertEqual(result[2][0]['date'], '2024-01-02')
        self.assertEqual(result[2][1]['date'], '2024-01-03')

    def test_date_range_filter_pickle(self):
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False)
        full_key = 'test_symbol::2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_records)
        self.cache_pickle.put(full_key, value)

        partial_key = 'test_symbol:2024-01-02:2024-01-03:day:2:qfq'
        result = self.cache_pickle.get(partial_key)

        self.assertIsNotNone(result)
        self.assertEqual(len(result[2]), 2)
        self.assertEqual(result[2][0]['date'], '2024-01-02')

    def test_merge_records_sqlite(self):
        self.cache_sqlite = PersistentCache(db_path=self.db_path_sqlite, use_duckdb=True)

        r1 = _make_records(['2024-01-01', '2024-01-02'])
        key1 = 'test_symbol:2024-01-01:2024-01-02:day:2:qfq'
        self.cache_sqlite.put(key1, ('test_symbol', '测试股票', r1))

        # second put with overlap; day 2 in r2 has different close
        r2 = [
            {'date': '2024-01-02', 'open': 200.0, 'high': 210.0, 'low': 190.0, 'close': 206.0, 'volume': 2000},
            {'date': '2024-01-03', 'open': 201.0, 'high': 211.0, 'low': 191.0, 'close': 207.0, 'volume': 2100},
        ]
        key2 = 'test_symbol:2024-01-02:2024-01-03:day:2:qfq'
        self.cache_sqlite.put(key2, ('test_symbol', '测试股票', r2))

        full_key = 'test_symbol::2024-01-03:day:3:qfq'
        result = self.cache_sqlite.get(full_key)

        self.assertIsNotNone(result)
        self.assertEqual(len(result[2]), 3)
        # overlapping date 2024-01-02 should use r2 value (last-write-wins)
        dates = [r['date'] for r in result[2]]
        self.assertIn('2024-01-02', dates)
        for r in result[2]:
            if r['date'] == '2024-01-02':
                self.assertEqual(r['close'], 206.0)

    def test_merge_records_pickle(self):
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False)

        r1 = _make_records(['2024-01-01', '2024-01-02'])
        key1 = 'test_symbol:2024-01-01:2024-01-02:day:2:qfq'
        self.cache_pickle.put(key1, ('test_symbol', '测试股票', r1))

        r2 = [
            {'date': '2024-01-02', 'open': 200.0, 'high': 210.0, 'low': 190.0, 'close': 206.0, 'volume': 2000},
            {'date': '2024-01-03', 'open': 201.0, 'high': 211.0, 'low': 191.0, 'close': 207.0, 'volume': 2100},
        ]
        key2 = 'test_symbol:2024-01-02:2024-01-03:day:2:qfq'
        self.cache_pickle.put(key2, ('test_symbol', '测试股票', r2))

        full_key = 'test_symbol::2024-01-03:day:3:qfq'
        result = self.cache_pickle.get(full_key)

        self.assertIsNotNone(result)
        self.assertEqual(len(result[2]), 3)
        for r in result[2]:
            if r['date'] == '2024-01-02':
                self.assertEqual(r['close'], 206.0)

    def test_no_overlap_date_range(self):
        self.cache_sqlite = PersistentCache(db_path=self.db_path_sqlite, use_duckdb=True)
        full_key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_records)
        self.cache_sqlite.put(full_key, value)

        no_overlap_key = 'test_symbol:2024-01-10:2024-01-12:day:3:qfq'
        result = self.cache_sqlite.get(no_overlap_key)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
