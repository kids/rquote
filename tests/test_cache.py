# -*- coding: utf-8 -*-
"""
缓存模块测试
"""
import unittest
import time
from rquote.cache import MemoryCache, DictCache, Cache


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


if __name__ == '__main__':
    unittest.main()

