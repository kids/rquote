# 重构代码示例

本文档展示如何将现有代码重构为更清晰的架构。

## 一、utils.py 改进示例

### 当前代码问题

```python
# 当前 utils.py 中的问题
class hget:
    def __init__(self, url, *args, **kwargs):
        # 类设计不合理，应该用函数
        ...
    
class WebUtils:
    @classmethod
    def http_get(cls, url, headers, method, proxy=None):
        headers['user-agent'] = cls.ua  # BUG: 应该是 cls.ua()
        # 缺少重试、超时控制等
        ...
```

### 改进后的代码

```python
# utils/http.py
import httpx
import time
import logging
from typing import Optional, Dict
from functools import wraps

logger = logging.getLogger(__name__)

class HTTPClient:
    """改进的HTTP客户端"""
    
    def __init__(self, timeout: int = 10, retry_times: int = 3, 
                 retry_delay: float = 1.0, pool_size: int = 10):
        self.timeout = timeout
        self.retry_times = retry_times
        self.retry_delay = retry_delay
        self.client = httpx.Client(
            timeout=timeout,
            limits=httpx.Limits(
                max_keepalive_connections=pool_size,
                max_connections=pool_size * 2
            )
        )
    
    def get(self, url: str, **kwargs) -> Optional[httpx.Response]:
        """带重试的GET请求"""
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
        from .user_agent import get_random_ua
        import uuid
        return {
            'User-Agent': get_random_ua(),
            'Referer': str(uuid.uuid4())
        }
    
    def close(self):
        """关闭客户端"""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# 全局HTTP客户端实例
_default_client = HTTPClient()

def hget(url: str, **kwargs) -> Optional[httpx.Response]:
    """
    简化的HTTP GET函数（向后兼容）
    
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

# utils/user_agent.py
import random

UA_LIST = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122',
    # ... 其他UA
]

def get_random_ua() -> str:
    """获取随机User-Agent"""
    return random.choice(UA_LIST)
```

## 二、main.py 拆分示例

### 当前问题
- 所有功能都在一个文件中
- 不同市场的数据获取逻辑混杂
- 难以测试和维护

### 改进方案：按市场拆分

```python
# markets/cn_stock.py
from typing import Tuple, Optional
import pandas as pd
from ..data_sources.tencent import TencentDataSource
from ..parsers.kline import KlineParser
from ..cache.base import Cache
from ..exceptions import SymbolError, DataSourceError

class CNStockMarket:
    """A股市场"""
    
    def __init__(self, data_source=None, cache: Optional[Cache] = None):
        self.data_source = data_source or TencentDataSource()
        self.cache = cache
        self.parser = KlineParser()
    
    def normalize_symbol(self, symbol: str) -> str:
        """标准化A股代码"""
        if symbol[0] in ['0', '1', '3', '5', '6']:
            prefix = 'sh' if symbol[0] in ['5', '6'] else 'sz'
            return prefix + symbol
        return symbol
    
    def get_price(self, symbol: str, sdate: str = '', edate: str = '',
                  freq: str = 'day', days: int = 320, fq: str = 'qfq') -> Tuple[str, str, pd.DataFrame]:
        """获取A股价格数据"""
        symbol = self.normalize_symbol(symbol)
        
        # 检查缓存
        if self.cache:
            cached = self.cache.get(f"price:{symbol}:{sdate}:{edate}:{freq}")
            if cached:
                return cached
        
        try:
            # 从数据源获取
            raw_data = self.data_source.fetch_kline(
                symbol, sdate=sdate, edate=edate, freq=freq, days=days, fq=fq
            )
            
            # 解析数据
            name, df = self.parser.parse_kline(raw_data)
            
            # 缓存结果
            if self.cache:
                self.cache.put(f"price:{symbol}:{sdate}:{edate}:{freq}", (symbol, name, df))
            
            return symbol, name, df
        except Exception as e:
            raise DataSourceError(f"Failed to get price for {symbol}: {e}")

# markets/future.py
class FutureMarket:
    """期货市场"""
    
    def __init__(self, data_source=None, cache: Optional[Cache] = None):
        self.data_source = data_source or SinaDataSource()  # 期货用新浪
        self.cache = cache
        self.parser = KlineParser()
    
    def get_price(self, symbol: str, **kwargs) -> Tuple[str, str, pd.DataFrame]:
        """获取期货价格数据"""
        if not symbol.startswith('fu'):
            raise SymbolError(f"Future symbol must start with 'fu': {symbol}")
        
        # 特殊处理BTC
        if symbol[2:5].lower() == 'btc':
            return self._get_btc_price(symbol, **kwargs)
        
        # 普通期货处理
        future_code = symbol[2:]  # 去掉'fu'前缀
        # ... 实现逻辑
```

## 三、数据源抽象示例

```python
# data_sources/base.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class DataSource(ABC):
    """数据源基类"""
    
    @abstractmethod
    def fetch_kline(self, symbol: str, **kwargs) -> Dict[str, Any]:
        """获取K线数据"""
        pass
    
    @abstractmethod
    def fetch_tick(self, symbols: list) -> Dict[str, Any]:
        """获取实时行情"""
        pass

# data_sources/tencent.py
from .base import DataSource
from ..utils.http import HTTPClient
from ..exceptions import DataSourceError

class TencentDataSource(DataSource):
    """腾讯数据源"""
    
    BASE_URL = "http://web.ifzq.gtimg.cn/appstock/app/newfqkline/get"
    
    def __init__(self, http_client: Optional[HTTPClient] = None):
        self.http_client = http_client or HTTPClient()
    
    def fetch_kline(self, symbol: str, freq: str = 'day', 
                   sdate: str = '', edate: str = '', 
                   days: int = 320, fq: str = 'qfq') -> Dict[str, Any]:
        """从腾讯获取K线数据"""
        url = f"{self.BASE_URL}?param={symbol},{freq},{sdate},{edate},{days},{fq}"
        
        response = self.http_client.get(url)
        if not response:
            raise DataSourceError(f"Failed to fetch from Tencent: {symbol}")
        
        # 解析响应
        try:
            # 提取JSON部分
            text = response.text
            json_start = text.find('{')
            if json_start == -1:
                raise DataSourceError("Invalid response format")
            
            data = json.loads(text[json_start:])
            if data.get('code') != 0:
                raise DataSourceError(f"API error: {data.get('msg')}")
            
            return data
        except Exception as e:
            raise DataSourceError(f"Parse error: {e}")
    
    def fetch_tick(self, symbols: list) -> Dict[str, Any]:
        """获取实时行情"""
        # 实现逻辑
        pass
```

## 四、缓存实现示例

```python
# cache/base.py
from abc import ABC, abstractmethod
from typing import Optional, Any

class Cache(ABC):
    """缓存基类"""
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        pass
    
    @abstractmethod
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        pass
    
    @abstractmethod
    def delete(self, key: str) -> None:
        pass

# cache/memory.py
import time
from typing import Optional, Any, Dict
from .base import Cache

class MemoryCache(Cache):
    """内存缓存实现"""
    
    def __init__(self, ttl: int = 3600):
        self.ttl = ttl
        self._cache: Dict[str, tuple] = {}  # {key: (value, expire_time)}
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        if key not in self._cache:
            return None
        
        value, expire_time = self._cache[key]
        if time.time() > expire_time:
            del self._cache[key]
            return None
        
        return value
    
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存"""
        ttl = ttl or self.ttl
        expire_time = time.time() + ttl
        self._cache[key] = (value, expire_time)
    
    def delete(self, key: str) -> None:
        """删除缓存"""
        self._cache.pop(key, None)
    
    def clear(self) -> None:
        """清空缓存"""
        self._cache.clear()
```

## 五、配置管理示例

```python
# config.py
from dataclasses import dataclass, field
from typing import Dict, Optional
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
    cache_ttl: int = 3600
    
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
```

## 六、异常处理示例

```python
# exceptions.py

class RQuoteException(Exception):
    """基础异常类"""
    pass

class DataSourceError(RQuoteException):
    """数据源错误"""
    pass

class ParseError(RQuoteException):
    """解析错误"""
    pass

class SymbolError(RQuoteException):
    """股票代码错误"""
    pass

class NetworkError(RQuoteException):
    """网络错误"""
    pass

class CacheError(RQuoteException):
    """缓存错误"""
    pass

# 使用示例
from rquote.exceptions import SymbolError, DataSourceError

def get_price(symbol: str):
    if not symbol:
        raise SymbolError("Symbol cannot be empty")
    
    try:
        # 获取数据
        ...
    except httpx.HTTPError as e:
        raise NetworkError(f"Network error: {e}") from e
    except json.JSONDecodeError as e:
        raise ParseError(f"Parse error: {e}") from e
```

## 七、向后兼容的API层

```python
# api/price.py
from typing import Tuple, Optional
import pandas as pd
from ..markets.factory import MarketFactory
from ..config import default_config
from ..cache.memory import MemoryCache

def get_price(i: str, sdate: str = '', edate: str = '', 
              freq: str = 'day', days: int = 320, 
              fq: str = 'qfq', dd: Optional[dict] = None) -> Tuple[str, str, pd.DataFrame]:
    """
    获取价格数据（保持原有API接口）
    
    向后兼容：支持dd参数（旧版缓存字典）
    """
    # 如果提供了dd参数，使用它作为缓存
    cache = None
    if default_config.cache_enabled:
        if dd is not None:
            # 兼容旧版缓存字典
            cache = DictCache(dd)
        else:
            cache = MemoryCache(ttl=default_config.cache_ttl)
    
    # 根据symbol判断市场类型
    market = MarketFactory.create_from_symbol(i, cache=cache)
    
    return market.get_price(i, sdate=sdate, edate=edate, 
                           freq=freq, days=days, fq=fq)

# 兼容旧版缓存字典的适配器
class DictCache:
    """将字典适配为Cache接口"""
    
    def __init__(self, cache_dict: dict):
        self._dict = cache_dict
    
    def get(self, key: str):
        return self._dict.get(key)
    
    def put(self, key: str, value):
        self._dict[key] = value
```

## 八、类型提示改进示例

```python
# 改进前
def get_price(i, sdate='', edate='', freq='day', days=320, fq='qfq', dd=None):
    return i, name, d

# 改进后
from typing import Tuple, Optional, Dict, Any
import pandas as pd

def get_price(
    i: str,
    sdate: str = '',
    edate: str = '',
    freq: str = 'day',
    days: int = 320,
    fq: str = 'qfq',
    dd: Optional[Dict[str, Any]] = None
) -> Tuple[str, str, pd.DataFrame]:
    """
    获取价格数据
    
    Args:
        i: 股票代码
        sdate: 开始日期，格式：YYYY-MM-DD
        edate: 结束日期，格式：YYYY-MM-DD
        freq: 频率，可选：'day', 'week', 'month', 'min'
        days: 获取天数
        fq: 复权方式，'qfq'前复权，'hfq'后复权
        dd: 缓存字典（已废弃，建议使用Cache接口）
    
    Returns:
        Tuple[symbol, name, DataFrame]: 股票代码、名称、价格数据
    
    Raises:
        SymbolError: 股票代码格式错误
        DataSourceError: 数据源错误
        NetworkError: 网络错误
    """
    ...
```

这些示例展示了如何逐步重构代码，保持向后兼容的同时提升代码质量。

