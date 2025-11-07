# -*- coding: utf-8 -*-
"""
工具模块测试
"""
import unittest
from rquote.utils import WebUtils, hget, logger
from rquote.utils.http import HTTPClient
from rquote.utils.date import check_date_format
from rquote.exceptions import SymbolError


class TestWebUtils(unittest.TestCase):
    """WebUtils测试"""
    
    def test_ua(self):
        """测试User-Agent生成"""
        ua = WebUtils.ua()
        self.assertIsInstance(ua, str)
        self.assertIn('Mozilla', ua)
    
    def test_headers(self):
        """测试请求头生成"""
        headers = WebUtils.headers()
        self.assertIn('user-agent', headers)
        self.assertIn('referer', headers)
    
    def test_http_get_fix(self):
        """测试http_get修复（确保cls.ua()被正确调用）"""
        # 这个测试确保修复后的代码不会因为cls.ua而报错
        headers = {'test': 'header'}
        # 注意：这个方法需要有效的URL才能测试，这里只测试不会报错
        result = WebUtils.http_get('https://httpbin.org/get', headers, 'text')
        # 如果网络可用，应该返回文本；否则返回None
        self.assertIsInstance(result, (str, type(None)))


class TestDateUtils(unittest.TestCase):
    """日期工具测试"""
    
    def test_check_date_format_standard(self):
        """测试标准日期格式"""
        result = check_date_format('2024-01-01')
        self.assertEqual(result, '2024-01-01')
    
    def test_check_date_format_slash(self):
        """测试斜杠格式"""
        result = check_date_format('2024/01/01')
        self.assertEqual(result, '2024-01-01')
    
    def test_check_date_format_no_separator(self):
        """测试无分隔符格式"""
        result = check_date_format('20240101')
        self.assertEqual(result, '2024-01-01')
    
    def test_check_date_format_invalid(self):
        """测试无效格式"""
        with self.assertRaises(ValueError):
            check_date_format('invalid-date')
    
    def test_check_date_format_empty(self):
        """测试空字符串"""
        result = check_date_format('')
        self.assertEqual(result, '')


class TestHTTPClient(unittest.TestCase):
    """HTTP客户端测试"""
    
    def test_http_client_init(self):
        """测试HTTP客户端初始化"""
        client = HTTPClient(timeout=5)
        self.assertEqual(client.timeout, 5)
        client.close()
    
    def test_http_client_context_manager(self):
        """测试上下文管理器"""
        with HTTPClient() as client:
            self.assertIsNotNone(client.client)
    
    def test_http_client_get_success(self):
        """测试成功请求"""
        with HTTPClient() as client:
            response = client.get('https://httpbin.org/get')
            if response:
                self.assertEqual(response.status_code, 200)


class TestHget(unittest.TestCase):
    """hget类测试"""
    
    def test_hget_init(self):
        """测试hget初始化"""
        # 使用一个简单的测试URL
        response = hget('https://httpbin.org/get')
        if response:
            self.assertIsNotNone(response.text)
            self.assertIsNotNone(response.content)


if __name__ == '__main__':
    unittest.main()

