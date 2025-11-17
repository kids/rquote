# -*- coding: utf-8 -*-
"""
缓存模块测试
"""
import unittest
import time
import os
import tempfile
import pandas as pd
from rquote.cache import MemoryCache, Cache
from rquote.cache.memory import DictCache

# 尝试导入持久化缓存（可选依赖）
try:
    from rquote.cache import PersistentCache
    PERSISTENT_CACHE_AVAILABLE = True
except ImportError:
    PERSISTENT_CACHE_AVAILABLE = False
    PersistentCache = None


class TestMemoryCache(unittest.TestCase):
    """内存缓存测试"""
    
    def setUp(self):
        """设置测试环境"""
        self.cache = MemoryCache(ttl=1)  # 1秒过期
    
    def test_put_get(self):
        """测试存储和获取"""
        self.cache.put('key1', 'value1')
        self.assertEqual(self.cache.get('key1'), 'value1')
    
    def test_expire(self):
        """测试过期"""
        self.cache.put('key2', 'value2', ttl=0.1)  # 0.1秒过期
        time.sleep(0.2)
        self.assertIsNone(self.cache.get('key2'))
    
    def test_delete(self):
        """测试删除"""
        self.cache.put('key3', 'value3')
        self.cache.delete('key3')
        self.assertIsNone(self.cache.get('key3'))
    
    def test_clear(self):
        """测试清空"""
        self.cache.put('key4', 'value4')
        self.cache.put('key5', 'value5')
        self.cache.clear()
        self.assertEqual(self.cache.size(), 0)
    
    def test_size(self):
        """测试大小"""
        self.cache.put('key6', 'value6')
        self.cache.put('key7', 'value7')
        self.assertEqual(self.cache.size(), 2)


class TestDictCache(unittest.TestCase):
    """字典缓存测试"""
    
    def setUp(self):
        """设置测试环境"""
        self.dict_cache = {}
        self.cache = DictCache(self.dict_cache)
    
    def test_put_get(self):
        """测试存储和获取"""
        self.cache.put('key1', 'value1')
        self.assertEqual(self.cache.get('key1'), 'value1')
        self.assertEqual(self.dict_cache['key1'], 'value1')
    
    def test_delete(self):
        """测试删除"""
        self.cache.put('key2', 'value2')
        self.cache.delete('key2')
        self.assertIsNone(self.cache.get('key2'))


@unittest.skipIf(not PERSISTENT_CACHE_AVAILABLE, "持久化缓存不可用（需要安装 duckdb）")
class TestPersistentCache(unittest.TestCase):
    """持久化缓存测试"""
    
    def setUp(self):
        """设置测试环境"""
        # 创建临时文件用于测试
        self.temp_dir = tempfile.mkdtemp()
        self.db_path_duckdb = os.path.join(self.temp_dir, 'test_cache.db')
        self.db_path_pickle = os.path.join(self.temp_dir, 'test_cache.pkl')
        
        # 创建测试用的 DataFrame
        self.test_df = pd.DataFrame({
            'open': [100, 101, 102],
            'high': [105, 106, 107],
            'low': [99, 100, 101],
            'close': [104, 105, 106],
            'volume': [1000, 1100, 1200]
        }, index=pd.date_range('2024-01-01', periods=3, freq='D'))
    
    def tearDown(self):
        """清理测试环境"""
        # 关闭缓存连接
        if hasattr(self, 'cache_duckdb') and self.cache_duckdb:
            try:
                self.cache_duckdb.close()
            except:
                pass
        if hasattr(self, 'cache_pickle') and self.cache_pickle:
            try:
                self.cache_pickle.close()
            except:
                pass
        
        # 删除临时文件
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_put_get_duckdb(self):
        """测试 duckdb 模式的存储和获取"""
        self.cache_duckdb = PersistentCache(db_path=self.db_path_duckdb, use_duckdb=True)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_df)
        
        self.cache_duckdb.put(key, value)
        result = self.cache_duckdb.get(key)
        
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'test_symbol')
        self.assertEqual(result[1], '测试股票')
        pd.testing.assert_frame_equal(result[2], self.test_df)
    
    def test_put_get_pickle(self):
        """测试 pickle 模式的存储和获取"""
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_df)
        
        self.cache_pickle.put(key, value)
        result = self.cache_pickle.get(key)
        
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'test_symbol')
        self.assertEqual(result[1], '测试股票')
        pd.testing.assert_frame_equal(result[2], self.test_df)
    
    def test_delete_duckdb(self):
        """测试 duckdb 模式的删除"""
        self.cache_duckdb = PersistentCache(db_path=self.db_path_duckdb, use_duckdb=True)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_df)
        
        self.cache_duckdb.put(key, value)
        self.assertIsNotNone(self.cache_duckdb.get(key))
        
        self.cache_duckdb.delete(key)
        self.assertIsNone(self.cache_duckdb.get(key))
    
    def test_delete_pickle(self):
        """测试 pickle 模式的删除"""
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_df)
        
        self.cache_pickle.put(key, value)
        self.assertIsNotNone(self.cache_pickle.get(key))
        
        self.cache_pickle.delete(key)
        self.assertIsNone(self.cache_pickle.get(key))
    
    def test_clear_duckdb(self):
        """测试 duckdb 模式的清空"""
        self.cache_duckdb = PersistentCache(db_path=self.db_path_duckdb, use_duckdb=True)
        key1 = 'symbol1:2024-01-01:2024-01-03:day:3:qfq'
        key2 = 'symbol2:2024-01-01:2024-01-03:day:3:qfq'
        value1 = ('symbol1', '股票1', self.test_df)
        value2 = ('symbol2', '股票2', self.test_df)
        
        self.cache_duckdb.put(key1, value1)
        self.cache_duckdb.put(key2, value2)
        self.assertIsNotNone(self.cache_duckdb.get(key1))
        self.assertIsNotNone(self.cache_duckdb.get(key2))
        
        self.cache_duckdb.clear()
        self.assertIsNone(self.cache_duckdb.get(key1))
        self.assertIsNone(self.cache_duckdb.get(key2))
    
    def test_clear_pickle(self):
        """测试 pickle 模式的清空"""
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False)
        key1 = 'symbol1:2024-01-01:2024-01-03:day:3:qfq'
        key2 = 'symbol2:2024-01-01:2024-01-03:day:3:qfq'
        value1 = ('symbol1', '股票1', self.test_df)
        value2 = ('symbol2', '股票2', self.test_df)
        
        self.cache_pickle.put(key1, value1)
        self.cache_pickle.put(key2, value2)
        self.assertIsNotNone(self.cache_pickle.get(key1))
        self.assertIsNotNone(self.cache_pickle.get(key2))
        
        self.cache_pickle.clear()
        self.assertIsNone(self.cache_pickle.get(key1))
        self.assertIsNone(self.cache_pickle.get(key2))
    
    def test_expire_duckdb(self):
        """测试 duckdb 模式的过期"""
        self.cache_duckdb = PersistentCache(db_path=self.db_path_duckdb, use_duckdb=True, ttl=1)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_df)
        
        self.cache_duckdb.put(key, value, ttl=0.1)  # 0.1秒过期
        self.assertIsNotNone(self.cache_duckdb.get(key))
        
        time.sleep(0.2)
        self.assertIsNone(self.cache_duckdb.get(key))
    
    def test_expire_pickle(self):
        """测试 pickle 模式的过期"""
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False, ttl=1)
        key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_df)
        
        self.cache_pickle.put(key, value, ttl=0.1)  # 0.1秒过期
        self.assertIsNotNone(self.cache_pickle.get(key))
        
        time.sleep(0.2)
        self.assertIsNone(self.cache_pickle.get(key))
    
    def test_date_range_filter_duckdb(self):
        """测试 duckdb 模式的日期范围过滤"""
        self.cache_duckdb = PersistentCache(db_path=self.db_path_duckdb, use_duckdb=True)
        # 存储完整数据
        full_key = 'test_symbol::2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_df)
        self.cache_duckdb.put(full_key, value)
        
        # 请求部分日期范围
        partial_key = 'test_symbol:2024-01-02:2024-01-03:day:2:qfq'
        result = self.cache_duckdb.get(partial_key)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result[2]), 2)  # 应该只有2行数据
        self.assertEqual(result[2].index[0], pd.Timestamp('2024-01-02'))
        self.assertEqual(result[2].index[1], pd.Timestamp('2024-01-03'))
    
    def test_date_range_filter_pickle(self):
        """测试 pickle 模式的日期范围过滤"""
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False)
        # 存储完整数据
        full_key = 'test_symbol::2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_df)
        self.cache_pickle.put(full_key, value)
        
        # 请求部分日期范围
        partial_key = 'test_symbol:2024-01-02:2024-01-03:day:2:qfq'
        result = self.cache_pickle.get(partial_key)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result[2]), 2)  # 应该只有2行数据
        self.assertEqual(result[2].index[0], pd.Timestamp('2024-01-02'))
        self.assertEqual(result[2].index[1], pd.Timestamp('2024-01-03'))
    
    def test_merge_dataframes_duckdb(self):
        """测试 duckdb 模式的数据合并"""
        self.cache_duckdb = PersistentCache(db_path=self.db_path_duckdb, use_duckdb=True)
        base_key = 'test_symbol:day:qfq'
        
        # 第一次存储
        df1 = pd.DataFrame({
            'open': [100, 101],
            'high': [105, 106],
            'low': [99, 100],
            'close': [104, 105],
            'volume': [1000, 1100]
        }, index=pd.date_range('2024-01-01', periods=2, freq='D'))
        key1 = f'test_symbol:2024-01-01:2024-01-02:day:2:qfq'
        value1 = ('test_symbol', '测试股票', df1)
        self.cache_duckdb.put(key1, value1)
        
        # 第二次存储（有重叠）
        df2 = pd.DataFrame({
            'open': [102, 103],
            'high': [107, 108],
            'low': [101, 102],
            'close': [106, 107],
            'volume': [1200, 1300]
        }, index=pd.date_range('2024-01-02', periods=2, freq='D'))
        key2 = f'test_symbol:2024-01-02:2024-01-03:day:2:qfq'
        value2 = ('test_symbol', '测试股票', df2)
        self.cache_duckdb.put(key2, value2)
        
        # 获取完整数据应该包含合并后的数据
        full_key = f'test_symbol::2024-01-03:day:3:qfq'
        result = self.cache_duckdb.get(full_key)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result[2]), 3)  # 应该合并为3行
        # 检查重叠部分使用新数据
        self.assertEqual(result[2].loc['2024-01-02', 'close'], 106)  # 应该使用 df2 的数据
    
    def test_merge_dataframes_pickle(self):
        """测试 pickle 模式的数据合并"""
        self.cache_pickle = PersistentCache(db_path=self.db_path_pickle, use_duckdb=False)
        base_key = 'test_symbol:day:qfq'
        
        # 第一次存储
        df1 = pd.DataFrame({
            'open': [100, 101],
            'high': [105, 106],
            'low': [99, 100],
            'close': [104, 105],
            'volume': [1000, 1100]
        }, index=pd.date_range('2024-01-01', periods=2, freq='D'))
        key1 = f'test_symbol:2024-01-01:2024-01-02:day:2:qfq'
        value1 = ('test_symbol', '测试股票', df1)
        self.cache_pickle.put(key1, value1)
        
        # 第二次存储（有重叠）
        df2 = pd.DataFrame({
            'open': [102, 103],
            'high': [107, 108],
            'low': [101, 102],
            'close': [106, 107],
            'volume': [1200, 1300]
        }, index=pd.date_range('2024-01-02', periods=2, freq='D'))
        key2 = f'test_symbol:2024-01-02:2024-01-03:day:2:qfq'
        value2 = ('test_symbol', '测试股票', df2)
        self.cache_pickle.put(key2, value2)
        
        # 获取完整数据应该包含合并后的数据
        full_key = f'test_symbol::2024-01-03:day:3:qfq'
        result = self.cache_pickle.get(full_key)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result[2]), 3)  # 应该合并为3行
        # 检查重叠部分使用新数据
        self.assertEqual(result[2].loc['2024-01-02', 'close'], 106)  # 应该使用 df2 的数据
    
    def test_no_overlap_date_range(self):
        """测试无重叠日期范围"""
        self.cache_duckdb = PersistentCache(db_path=self.db_path_duckdb, use_duckdb=True)
        # 存储 2024-01-01 到 2024-01-03 的数据
        full_key = 'test_symbol:2024-01-01:2024-01-03:day:3:qfq'
        value = ('test_symbol', '测试股票', self.test_df)
        self.cache_duckdb.put(full_key, value)
        
        # 请求 2024-01-10 到 2024-01-12 的数据（无重叠）
        no_overlap_key = 'test_symbol:2024-01-10:2024-01-12:day:3:qfq'
        result = self.cache_duckdb.get(no_overlap_key)
        
        self.assertIsNone(result)  # 应该返回 None


if __name__ == '__main__':
    unittest.main()

