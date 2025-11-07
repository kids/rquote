# rquote 架构改进建议

## 一、当前问题分析

### 1.1 代码组织问题
- **单一文件过大**：`main.py` 478行，包含过多职责（数据获取、列表获取、价格获取等）
- **缺乏模块化**：所有功能混在一个文件中，难以维护和扩展
- **职责不清**：数据源、数据解析、业务逻辑混杂

### 1.2 utils.py 问题
- **hget 类设计不合理**：应该使用函数而非类
- **WebUtils 实现缺陷**：
  - `http_get` 方法中 `cls.ua` 应该是 `cls.ua()`
  - 缺少重试机制、超时控制、连接池管理
  - 代理功能未完整实现
- **BasicFactors 缺少文档**：因子计算逻辑不清晰

### 1.3 架构设计问题
- **缺少配置管理**：URL、超时时间等硬编码
- **缺少缓存层**：每次请求都重新获取数据
- **缺少数据源抽象**：不同市场的数据获取逻辑耦合
- **缺少异常处理体系**：错误处理不统一
- **缺少类型提示**：代码可读性和IDE支持差
- **缺少接口抽象**：难以替换数据源或扩展功能

## 二、推荐架构方案

### 2.1 目录结构重构

```
rquote/
├── __init__.py              # 公共API导出
├── config.py                # 配置管理
├── exceptions.py             # 异常定义
├── cache/                    # 缓存模块
│   ├── __init__.py
│   ├── base.py              # 缓存基类
│   └── memory.py            # 内存缓存实现
├── data_sources/             # 数据源模块
│   ├── __init__.py
│   ├── base.py              # 数据源基类
│   ├── sina.py              # 新浪数据源
│   ├── tencent.py           # 腾讯数据源
│   ├── eastmoney.py         # 东方财富数据源
│   └── factory.py           # 数据源工厂
├── parsers/                  # 数据解析模块
│   ├── __init__.py
│   ├── base.py              # 解析器基类
│   ├── kline.py             # K线数据解析
│   └── tick.py              # 实时行情解析
├── markets/                  # 市场模块
│   ├── __init__.py
│   ├── base.py              # 市场基类
│   ├── cn_stock.py          # A股市场
│   ├── hk_stock.py          # 港股市场
│   ├── us_stock.py          # 美股市场
│   ├── future.py            # 期货市场
│   └── fund.py              # 基金市场
├── utils/                    # 工具模块
│   ├── __init__.py
│   ├── http.py              # HTTP客户端
│   ├── date.py              # 日期工具
│   └── validators.py        # 验证工具
├── factors/                  # 因子计算模块
│   ├── __init__.py
│   ├── base.py              # 因子基类
│   └── technical.py         # 技术因子
├── plots/                    # 绘图模块
│   ├── __init__.py
│   └── candle.py            # K线图
└── api/                      # 公共API层
    ├── __init__.py
    ├── price.py             # 价格API
    ├── list.py              # 列表API
    └── tick.py              # 实时行情API
```

### 2.2 核心设计模式

#### 2.2.1 数据源抽象层
```python
# data_sources/base.py
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

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
```

#### 2.2.2 市场抽象层
```python
# markets/base.py
from abc import ABC, abstractmethod
from typing import Tuple, Optional
import pandas as pd

class Market(ABC):
    """市场基类"""
    
    def __init__(self, data_source: DataSource, cache: Optional[Cache] = None):
        self.data_source = data_source
        self.cache = cache
    
    @abstractmethod
    def get_price(self, symbol: str, **kwargs) -> Tuple[str, str, pd.DataFrame]:
        """获取价格数据"""
        pass
    
    @abstractmethod
    def normalize_symbol(self, symbol: str) -> str:
        """标准化股票代码"""
        pass
```

#### 2.2.3 缓存抽象层
```python
# cache/base.py
from abc import ABC, abstractmethod
from typing import Optional, Any

class Cache(ABC):
    """缓存基类"""
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        pass
    
    @abstractmethod
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存"""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> None:
        """删除缓存"""
        pass
```

### 2.3 配置管理

```python
# config.py
from dataclasses import dataclass
from typing import Dict, Optional

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
    
    # 数据源配置
    data_sources: Dict[str, str] = None
    
    # 日志配置
    log_level: str = "INFO"
    log_file: Optional[str] = "/tmp/rquote.log"
```

### 2.4 HTTP客户端改进

```python
# utils/http.py
import httpx
from typing import Optional, Dict, Any
from .config import Config

class HTTPClient:
    """改进的HTTP客户端"""
    
    def __init__(self, config: Config):
        self.config = config
        self.client = httpx.Client(
            timeout=config.http_timeout,
            limits=httpx.Limits(
                max_keepalive_connections=config.http_pool_size,
                max_connections=config.http_pool_size * 2
            )
        )
    
    def get(self, url: str, **kwargs) -> Optional[httpx.Response]:
        """带重试的GET请求"""
        for attempt in range(self.config.http_retry_times):
            try:
                response = self.client.get(
                    url,
                    headers=self._get_headers(),
                    **kwargs
                )
                response.raise_for_status()
                return response
            except Exception as e:
                if attempt == self.config.http_retry_times - 1:
                    raise
                time.sleep(self.config.http_retry_delay * (attempt + 1))
        return None
    
    def _get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        return {
            'User-Agent': WebUtils.ua(),
            'Referer': str(uuid.uuid4())
        }
```

### 2.5 异常处理体系

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
```

## 三、具体改进建议

### 3.1 立即改进项（高优先级）

1. **拆分 main.py**
   - 将数据获取功能按市场类型拆分到 `markets/` 模块
   - 将列表获取功能拆分到 `api/list.py`
   - 将价格获取功能拆分到 `api/price.py`

2. **修复 utils.py 中的bug**
   - 修复 `WebUtils.http_get` 中的 `cls.ua` 调用
   - 将 `hget` 类改为函数或改进设计
   - 添加重试机制和超时控制

3. **添加类型提示**
   - 为所有公共API添加类型提示
   - 使用 `typing` 模块增强代码可读性

4. **统一异常处理**
   - 定义统一的异常类
   - 替换所有 `raise ValueError` 为具体异常类型

### 3.2 中期改进项（中优先级）

1. **实现配置管理**
   - 创建 `config.py` 统一管理配置
   - 支持环境变量和配置文件

2. **实现缓存层**
   - 添加内存缓存实现
   - 可选支持 Redis 等外部缓存

3. **重构数据源**
   - 抽象数据源接口
   - 实现不同数据源的适配器

4. **添加单元测试**
   - 为核心功能添加测试
   - 使用 mock 隔离外部依赖

### 3.3 长期改进项（低优先级）

1. **性能优化**
   - 实现连接池复用
   - 添加异步支持（async/await）

2. **扩展性增强**
   - 支持插件机制
   - 支持自定义数据源

3. **文档完善**
   - API文档（Sphinx）
   - 使用示例和最佳实践

## 四、实施步骤

### 阶段一：基础重构（1-2周）
1. 创建新的目录结构
2. 拆分 `main.py` 到对应模块
3. 修复 `utils.py` 中的bug
4. 添加类型提示和异常处理

### 阶段二：架构优化（2-3周）
1. 实现配置管理
2. 实现缓存层
3. 重构数据源抽象
4. 添加单元测试

### 阶段三：功能增强（1-2周）
1. 性能优化
2. 文档完善
3. 添加更多功能特性

## 五、代码示例

### 5.1 改进后的API使用方式

```python
# 使用方式保持不变，但内部实现更清晰
from rquote import get_price, get_cn_stock_list

# 内部会使用配置、缓存、数据源抽象等
sid, name, df = get_price('sh000001', sdate='2024-01-01')
```

### 5.2 高级用法（支持配置和缓存）

```python
from rquote import MarketFactory, Config, MemoryCache

# 自定义配置
config = Config(
    http_timeout=15,
    cache_enabled=True,
    cache_ttl=7200
)

# 使用缓存
cache = MemoryCache(ttl=config.cache_ttl)

# 获取市场实例
market = MarketFactory.create('cn_stock', cache=cache)

# 获取数据
sid, name, df = market.get_price('sh000001')
```

## 六、注意事项

1. **向后兼容**：保持现有API接口不变，确保现有代码可以继续使用
2. **渐进式重构**：分阶段实施，避免一次性大改动
3. **测试覆盖**：每次重构都要有对应的测试
4. **文档同步**：代码改动要及时更新文档

