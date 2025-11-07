# -*- coding: utf-8 -*-
"""
配置模块测试
"""
import unittest
import os
from rquote.config import Config, default_config


class TestConfig(unittest.TestCase):
    """配置测试"""
    
    def test_default_config(self):
        """测试默认配置"""
        self.assertIsInstance(default_config, Config)
        self.assertEqual(default_config.http_timeout, 10)
        self.assertTrue(default_config.cache_enabled)
    
    def test_custom_config(self):
        """测试自定义配置"""
        config = Config(
            http_timeout=20,
            cache_enabled=False,
            cache_ttl=7200
        )
        self.assertEqual(config.http_timeout, 20)
        self.assertFalse(config.cache_enabled)
        self.assertEqual(config.cache_ttl, 7200)
    
    def test_from_env(self):
        """测试从环境变量创建配置"""
        # 设置环境变量
        os.environ['RQUOTE_HTTP_TIMEOUT'] = '15'
        os.environ['RQUOTE_CACHE_ENABLED'] = 'false'
        
        config = Config.from_env()
        self.assertEqual(config.http_timeout, 15)
        self.assertFalse(config.cache_enabled)
        
        # 清理环境变量
        del os.environ['RQUOTE_HTTP_TIMEOUT']
        del os.environ['RQUOTE_CACHE_ENABLED']


if __name__ == '__main__':
    unittest.main()

