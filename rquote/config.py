# -*- coding: utf-8 -*-
"""
配置管理模块
"""
from dataclasses import dataclass
from typing import Optional
import os


@dataclass
class Config:
    """配置类"""
    # HTTP配置
    http_timeout: int = 10
    http_retry_times: int = 3
    http_retry_delay: float = 1.0
    http_pool_size: int = 10
    
    # 缓存配置
    cache_enabled: bool = True
    cache_ttl: int = 3600  # 秒
    
    # 日志配置
    log_level: str = "INFO"
    log_file: Optional[str] = "/tmp/rquote.log"
    
    @classmethod
    def from_env(cls) -> 'Config':
        """从环境变量创建配置"""
        return cls(
            http_timeout=int(os.getenv('RQUOTE_HTTP_TIMEOUT', '10')),
            http_retry_times=int(os.getenv('RQUOTE_RETRY_TIMES', '3')),
            cache_enabled=os.getenv('RQUOTE_CACHE_ENABLED', 'true').lower() == 'true',
            cache_ttl=int(os.getenv('RQUOTE_CACHE_TTL', '3600')),
            log_level=os.getenv('RQUOTE_LOG_LEVEL', 'INFO'),
            log_file=os.getenv('RQUOTE_LOG_FILE', '/tmp/rquote.log')
        )


# 全局默认配置
default_config = Config.from_env()

