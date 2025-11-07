# -*- coding: utf-8 -*-
"""
异常模块测试
"""
import unittest
from rquote.exceptions import (
    RQuoteException,
    DataSourceError,
    ParseError,
    SymbolError,
    NetworkError,
    CacheError,
    HTTPError
)


class TestExceptions(unittest.TestCase):
    """异常测试"""
    
    def test_rquote_exception(self):
        """测试基础异常"""
        with self.assertRaises(RQuoteException):
            raise RQuoteException("Test error")
    
    def test_data_source_error(self):
        """测试数据源错误"""
        with self.assertRaises(DataSourceError):
            raise DataSourceError("Data source error")
        # 检查继承关系
        self.assertTrue(issubclass(DataSourceError, RQuoteException))
    
    def test_parse_error(self):
        """测试解析错误"""
        with self.assertRaises(ParseError):
            raise ParseError("Parse error")
        self.assertTrue(issubclass(ParseError, RQuoteException))
    
    def test_symbol_error(self):
        """测试股票代码错误"""
        with self.assertRaises(SymbolError):
            raise SymbolError("Symbol error")
        self.assertTrue(issubclass(SymbolError, RQuoteException))
    
    def test_network_error(self):
        """测试网络错误"""
        with self.assertRaises(NetworkError):
            raise NetworkError("Network error")
        self.assertTrue(issubclass(NetworkError, RQuoteException))
    
    def test_cache_error(self):
        """测试缓存错误"""
        with self.assertRaises(CacheError):
            raise CacheError("Cache error")
        self.assertTrue(issubclass(CacheError, RQuoteException))
    
    def test_http_error(self):
        """测试HTTP错误（向后兼容）"""
        with self.assertRaises(HTTPError):
            raise HTTPError("HTTP error")
        self.assertTrue(issubclass(HTTPError, RQuoteException))


if __name__ == '__main__':
    unittest.main()

