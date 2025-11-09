# -*- coding: utf-8 -*-
"""
Web工具
"""
import random
import uuid
import httpx
import logging

logger = logging.getLogger(__name__)


class WebUtils:
    """Web工具类"""
    
    UA_LIST = [
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)',
        'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.5 Safari/534.55.3',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/521.61'
    ]
    
    @staticmethod
    def ua():
        """获取随机User-Agent"""
        return random.choice(WebUtils.UA_LIST)
    
    @classmethod
    def headers(cls):
        """获取请求头"""
        return {
            'referer': str(uuid.uuid4()),
            'user-agent': cls.ua()
        }
    
    @classmethod
    def http_get(cls, url, headers, method, proxy=None):
        """
        HTTP GET请求（已废弃，建议使用HTTPClient）
        """
        headers['user-agent'] = cls.ua()
        try:
            r = httpx.get(url, allow_redirects=True, headers=headers)
        except Exception as e:
            logger.error('Fetch url {} err: {}'.format(url, e))
            return None
        if r:
            if method == 'text':
                return r.text
            elif method == 'content':
                return r.content
    
    @classmethod
    def test_proxy(cls, proxy: str):
        """
        测试代理是否可用
        
        Args:
            proxy: 代理地址，格式 'ip:port'
        
        Returns:
            1表示可用，0表示不可用
        """
        try:
            proxies = {'http://': proxy, 'https://': proxy} if '://' not in proxy else proxy
            with httpx.Client(proxies=proxies, timeout=2) as client:
                r = client.get('https://baidu.com', timeout=2)
                if r.status_code == 200:
                    logger.info(f'test proxy {proxy} positive')
                    return 1
                else:
                    logger.info(f'test proxy {proxy} negative (status: {r.status_code})')
                    return 0
        except Exception as e:
            logger.info(f'test proxy {proxy} negative: {e}')
            return 0


class hget:
    """
    HTTP GET请求类（向后兼容）
    
    注意：建议使用HTTPClient替代
    """
    def __init__(self, url, *args, **kwargs):
        self.url = url
        r = None
        try:
            r = httpx.get(
                self.url, follow_redirects=True, headers=WebUtils.headers(),
                *args, **kwargs)
            self.text = r.text
            self.content = r.content
        except Exception as e:
            logger.error(f'fetch {self.url} err: {e}')
            self.text = ''
            self.content = b''
        finally:
            # 确保响应对象被关闭，释放SSL连接
            if r is not None:
                r.close()

