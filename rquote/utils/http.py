# -*- coding: utf-8 -*-
"""
HTTP客户端模块
"""
import time
import logging
import httpx
from typing import Optional, Dict
from ..config import default_config

logger = logging.getLogger(__name__)


class HTTPClient:
    """改进的HTTP客户端"""
    
    def __init__(self, timeout: Optional[int] = None, retry_times: Optional[int] = None,
                 retry_delay: Optional[float] = None, pool_size: Optional[int] = None):
        """
        初始化HTTP客户端
        
        Args:
            timeout: 超时时间（秒）
            retry_times: 重试次数
            retry_delay: 重试延迟（秒）
            pool_size: 连接池大小
        """
        self.timeout = timeout or default_config.http_timeout
        self.retry_times = retry_times or default_config.http_retry_times
        self.retry_delay = retry_delay or default_config.http_retry_delay
        pool_size = pool_size or default_config.http_pool_size
        
        self.client = httpx.Client(
            timeout=self.timeout,
            limits=httpx.Limits(
                max_keepalive_connections=pool_size,
                max_connections=pool_size * 2
            )
        )
    
    def get(self, url: str, **kwargs) -> Optional[httpx.Response]:
        """
        带重试的GET请求
        
        Args:
            url: 请求URL
            **kwargs: 传递给httpx.get的其他参数
        
        Returns:
            Response对象，失败返回None
        """
        headers = kwargs.pop('headers', {})
        headers.update(self._get_default_headers())
        
        for attempt in range(self.retry_times):
            try:
                response = self.client.get(
                    url,
                    headers=headers,
                    **kwargs
                )
                response.raise_for_status()
                return response
            except httpx.HTTPError as e:
                if attempt == self.retry_times - 1:
                    logger.error(f'Failed to fetch {url} after {self.retry_times} attempts: {e}')
                    raise
                logger.warning(f'Attempt {attempt + 1} failed for {url}, retrying...')
                time.sleep(self.retry_delay * (attempt + 1))
        
        return None
    
    def _get_default_headers(self) -> Dict[str, str]:
        """获取默认请求头"""
        import uuid
        from .web import WebUtils
        return {
            'User-Agent': WebUtils.ua(),
            'Referer': str(uuid.uuid4())
        }
    
    def close(self):
        """关闭客户端"""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# 全局HTTP客户端实例（向后兼容）
_default_client = HTTPClient()

def hget_new(url: str, **kwargs) -> Optional[httpx.Response]:
    """
    改进的HTTP GET函数
    
    Args:
        url: 请求URL
        **kwargs: 传递给httpx.get的其他参数
    
    Returns:
        Response对象，失败返回None
    """
    try:
        return _default_client.get(url, **kwargs)
    except Exception as e:
        logger.error(f'hget failed for {url}: {e}')
        return None

