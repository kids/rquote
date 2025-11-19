# rquote

`rquote` 是一个提供 A股/港股/美股/ETF基金/期货 历史数据获取的Python库

## 版本信息

当前版本：**0.4.6**

## 主要特性

- ✅ 支持多市场数据获取（A股、港股、美股、期货、基金）
- ✅ 统一的API接口，使用简单
- ✅ 内置缓存机制，提升性能
- ✅ 完善的错误处理和异常体系
- ✅ 可配置的HTTP客户端（超时、重试等）
- ✅ 模块化设计，易于扩展

## 安装

```bash
pip install rquote
```

或使用 uv：

```bash
uv pip install rquote
```

## 快速开始

### 基本使用

```python
from rquote import get_price

# 获取上证指数数据
sid, name, df = get_price('sh000001')
print(df.head())  # 数据为pandas DataFrame
```

### 获取指定日期范围的数据

```python
# 获取指定日期范围的数据
sid, name, df = get_price('sz000001', sdate='2024-01-01', edate='2024-02-01')
```

### 使用缓存

#### 内存缓存（MemoryCache）

```python
from rquote import get_price, MemoryCache

# 创建缓存实例
cache = MemoryCache(ttl=3600)  # 缓存1小时

# 使用缓存（通过dd参数传递MemoryCache实例）
sid, name, df = get_price('sh000001', dd=cache)

# 注意：MemoryCache 是内存缓存，数据仅在当前进程运行期间有效
# 脚本运行结束后，缓存数据会丢失
```

**缓存生命周期说明：**
- `MemoryCache` 是纯内存缓存，数据存储在进程内存中
- 缓存数据仅在当前脚本运行期间有效
- 脚本运行结束后，所有缓存数据会丢失

#### 持久化缓存（PersistentCache）

持久化缓存支持跨进程/跨运行的缓存持久化，数据会保存到本地文件。

**安装可选依赖：**
```bash
pip install rquote[persistent]
# 或
uv pip install "rquote[persistent]"
```

**使用持久化缓存：**
```python
from rquote import get_price, PersistentCache

# 创建持久化缓存实例
# 默认使用 duckdb（如果已安装），否则使用 pickle 文件
cache = PersistentCache(ttl=86400)  # 缓存24小时，默认路径：~/.rquote/cache.db

# 或指定自定义路径
cache = PersistentCache(db_path='./my_cache.db', use_duckdb=True)

# 使用缓存
sid, name, df = get_price('sh000001', dd=cache)

# 持久化缓存支持智能扩展：
# - 当请求的结束日期不在缓存中时，会自动从缓存的最新日期向前扩展
# - 当请求的开始日期不在缓存中时，会自动从缓存的最早日期向后扩展
# - 数据会自动合并，避免重复请求

# 关闭缓存（可选，程序退出时会自动保存）
cache.close()
```

**持久化缓存特性：**
- ✅ 跨进程/跨运行持久化：数据保存在本地文件，下次运行仍可使用
- ✅ 智能数据合并：相同股票的数据会自动合并，key 不包含日期范围
- ✅ 智能扩展：当请求的日期范围超出缓存时，自动扩展并合并数据
- ✅ 支持 TTL：可设置缓存过期时间
- ✅ 可选 duckdb：如果安装了 duckdb，使用 duckdb 存储（性能更好），否则使用 pickle 文件

## 主要功能

### 历史价格数据获取

#### `get_price(i, sdate='', edate='', freq='day', days=320, fq='qfq', dd=None)`

获取股票、基金、期货的历史价格数据

**参数:**
- `i`: 股票代码，使用新浪/腾讯的id形式
- `sdate`: 开始日期 (可选，格式：YYYY-MM-DD)
- `edate`: 结束日期 (可选，格式：YYYY-MM-DD)
- `freq`: 频率，默认'day' (日线)，可选：(港A)'week', 'month', (美股)'min'
- `days`: 获取天数，默认320天
- `fq`: 复权方式，默认'qfq' (前复权)，可选：'hfq' (后复权)
- `dd`: 本地缓存字典 (可选，已废弃，建议使用MemoryCache)

**代码格式说明:**
- A股: `sh000001`表示上证指数，`sz000001`表示深市000001股票`平安银行`
- ETF: `sh510050`表示上证50指数ETF
- 港股: `hk00700`表示港股腾讯
- 期货: 需加`fu`前缀，如`fuAP2110`，`fuBTC`表示比特币
- 美股: 需加对应交易所后缀，如`usBABA.N`，`usC.N`，`usAAPL.OQ`等
- 比特币：使用`fuBTC`代码

**示例:**
```python
from rquote import get_price

# 获取上证指数数据
sid, nm, df = get_price('sh000001')
print(df.head())

# 获取指定日期范围的数据
sid, nm, df = get_price('sz000001', sdate='2024-01-01', edate='2024-02-01')

# 获取比特币数据
sid, nm, df = get_price('fuBTC')

# 获取期货分钟数据
sid, nm, df = get_price('fuM2601', freq='min')
```

**返回数据格式:**
| date       | open    | close   | high    | low     | vol        |
|------------|---------|---------|---------|---------|------------|
| 2024-02-06 | 2680.48 | 2789.49 | 2802.93 | 2669.67 | 502849313  |
| 2024-02-07 | 2791.51 | 2829.70 | 2829.70 | 2770.53 | 547117439  |

#### `get_price_longer(i, l=2, dd={})`

获取更长时间的历史数据，默认获取2年数据

```python
from rquote import get_price_longer

# 获取3年的历史数据
sid, nm, df = get_price_longer('sh000001', l=3)
```

### 股票列表获取

#### `get_cn_stock_list(money_min=2e8)`

获取A股股票列表，按成交额排序，默认筛选成交额大于2亿的股票

```python
from rquote import get_cn_stock_list

# 获取成交额大于5亿的股票列表
stocks = get_cn_stock_list(money_min=5e8)
# 返回格式: [{code, name, pe_ttm, volume, turnover/亿, ...}, ...]
```

#### `get_hk_stocks_500()`

获取港股前500只股票列表(按当日成交额排序)

```python
from rquote import get_hk_stocks_500

stocks = get_hk_stocks_500()
# 返回格式: [[code, name, price, -, -, -, -, volume, turnover, ...], ...]
```

#### `get_us_stocks(k=100)`

获取美股最大市值的k支股票列表

```python
from rquote import get_us_stocks

us_stocks = get_us_stocks(k=100)  # 获取前100只
# 返回格式: [{name, symbol, market, mktcap, pe, ...}, ...]
```

#### `get_cnindex_stocks(index_type='hs300')`

获取中国指数成分股列表

```python
from rquote import get_cnindex_stocks

# 获取沪深300成分股
hs300_stocks = get_cnindex_stocks('hs300')
# 获取中证500成分股
zz500_stocks = get_cnindex_stocks('zz500')
# 获取中证1000成分股
zz1000_stocks = get_cnindex_stocks('zz1000')

# 返回格式: [{SECURITY_CODE, SECURITY_NAME_ABBR, INDUSTRY, WEIGHT, EPS, BPS, ROE, FREE_CAP, ...}, ...]
```

支持的指数类型：
- `'hs300'`: 沪深300
- `'zz500'`: 中证500
- `'zz1000'`: 中证1000

### 基金和期货

#### `get_cn_fund_list()`

获取A股ETF基金列表，按成交额排序

```python
from rquote import get_cn_fund_list

funds = get_cn_fund_list()
# 返回格式: [code, name, change, amount, price]
```

#### `get_cn_future_list()`

获取国内期货合约列表

```python
from rquote import get_cn_future_list

futures = get_cn_future_list()
# 返回格式: ['fuSC2109', 'fuRB2110', 'fuHC2110', ...]
```

### 板块和概念

#### `get_all_industries()`

获取所有行业板块列表

```python
from rquote import get_all_industries

industries = get_all_industries()
# 返回格式: [code, name, change, amount, price, sina_sw2_id]
```

#### `get_stock_concepts(i)`

获取指定股票所属的概念板块

```python
from rquote import get_stock_concepts

# 获取平安银行的概念板块
concepts = get_stock_concepts('sz000001')
# 返回概念代码列表，如 ['BK0420', 'BK0900', ...]
```

#### `get_stock_industry(i)`

获取指定股票所属的行业板块

```python
from rquote import get_stock_industry

# 获取平安银行的行业板块
industries = get_stock_industry('sz000001')
```

#### `get_industry_stocks(node)`

获取指定行业板块的股票列表

```python
from rquote import get_industry_stocks

# 获取行业板块股票
stocks = get_industry_stocks('sw2_480200')
```

### 实时行情

#### `get_tick(tgts=[])`

获取实时行情数据

```python
from rquote import get_tick

# 获取美股实时行情
tick_data = get_tick(['AAPL', 'GOOGL'])
# 返回格式: [{'name': 'Apple Inc', 'price': '150.25', 'price_change_rate': '1.2%', ...}]
```

### 可视化工具

#### `PlotUtils.plot_candle(i, sdate='', edate='', dsh=False, vol=True)`

绘制K线图

```python
from rquote import PlotUtils
import plotly.graph_objs as go

# 绘制平安银行的K线图
data, layout = PlotUtils.plot_candle('sz000001', sdate='2024-01-01', edate='2024-02-01')

# 使用plotly显示
fig = go.Figure(data=data, layout=layout)
fig.show()
```

## 高级功能

### 配置管理

```python
from rquote import config

# 使用默认配置
default_config = config.default_config

# 创建自定义配置
custom_config = config.Config(
    http_timeout=15,
    http_retry_times=5,
    cache_enabled=True,
    cache_ttl=7200
)

# 从环境变量创建配置
import os
os.environ['RQUOTE_HTTP_TIMEOUT'] = '20'
config_from_env = config.Config.from_env()
```

### 日志配置

**默认情况下，日志功能是关闭的。** 如果需要启用日志，可以通过环境变量手动开启：

#### 通过环境变量开启日志

```bash
# 设置日志级别为 INFO（会同时输出到文件和控制台）
export RQUOTE_LOG_LEVEL=INFO

# 可选：自定义日志文件路径（默认为 /tmp/rquote.log）
export RQUOTE_LOG_FILE=/path/to/your/logfile.log

# 然后运行你的Python脚本
python your_script.py
```

#### 支持的日志级别

- `DEBUG`: 详细的调试信息
- `INFO`: 一般信息（推荐）
- `WARNING`: 警告信息
- `ERROR`: 错误信息
- `CRITICAL`: 严重错误

#### 在Python代码中开启日志

```python
import os

# 在导入 rquote 之前设置环境变量
os.environ['RQUOTE_LOG_LEVEL'] = 'INFO'
os.environ['RQUOTE_LOG_FILE'] = '/tmp/rquote.log'  # 可选

from rquote import get_price

# 现在日志已启用
sid, name, df = get_price('sh000001')
```

#### 关闭日志

如果不设置 `RQUOTE_LOG_LEVEL` 环境变量，或者设置为空值，日志功能将保持关闭状态（默认行为）。

### 使用改进的HTTP客户端

```python
from rquote.utils.http import HTTPClient

# 创建HTTP客户端
with HTTPClient(timeout=15, retry_times=3) as client:
    response = client.get('https://example.com')
    if response:
        print(response.text)
```

### 使用缓存

```python
from rquote.cache import MemoryCache

# 创建缓存
cache = MemoryCache(ttl=3600)  # 缓存1小时

# 使用缓存
cache.put('key1', 'value1')
value = cache.get('key1')
cache.delete('key1')
cache.clear()  # 清空所有缓存
```

### 异常处理

```python
from rquote import get_price
from rquote.exceptions import SymbolError, DataSourceError, NetworkError

try:
    sid, name, df = get_price('invalid_symbol')
except SymbolError as e:
    print(f"股票代码错误: {e}")
except DataSourceError as e:
    print(f"数据源错误: {e}")
except NetworkError as e:
    print(f"网络错误: {e}")
```

### 工具类

#### `WebUtils`

网络请求工具类

```python
from rquote import WebUtils

# 获取随机User-Agent
ua = WebUtils.ua()

# 获取请求头
headers = WebUtils.headers()

# 测试代理
result = WebUtils.test_proxy('127.0.0.1:8080')
```

#### `BasicFactors`

基础因子计算工具类

```python
from rquote import BasicFactors
import pandas as pd

# 假设df是价格数据DataFrame
# break_rise: 突破上涨
break_rise = BasicFactors.break_rise(df)

# min_resist: 最小阻力
min_resist = BasicFactors.min_resist(df)

# vol_extreme: 成交量极值
vol_extreme = BasicFactors.vol_extreme(df)

# bias_rate_over_ma60: 偏离MA60的比率
bias_rate = BasicFactors.bias_rate_over_ma60(df)

# op_ma: MA评分
ma_score = BasicFactors.op_ma(df)
```

## 架构改进

### 新版本改进

**v0.3.5** 主要改进：

1. **修复Critical Bugs**
   - 修复了 `WebUtils.http_get` 中的 `cls.ua` bug
   - 修复了 `test_proxy` 方法中的逻辑错误
   - 改进了异常处理

2. **新增模块化架构**
   - 配置管理模块 (`config.py`)
   - 异常处理体系 (`exceptions.py`)
   - 缓存抽象层 (`cache/`)
   - 数据源抽象层 (`data_sources/`)
   - 改进的HTTP客户端 (`utils/http.py`)

3. **向后兼容**
   - 所有原有API保持不变
   - 新增功能为可选使用

### 目录结构

```
rquote/
├── __init__.py              # 公共API导出
├── config.py                # 配置管理
├── exceptions.py             # 异常定义
├── main.py                   # 主要功能（向后兼容）
├── utils.py                  # 工具类（向后兼容）
├── plots.py                  # 绘图工具
├── cache/                    # 缓存模块
│   ├── __init__.py
│   ├── base.py              # 缓存基类
│   └── memory.py            # 内存缓存实现
├── data_sources/             # 数据源模块
│   ├── __init__.py
│   ├── base.py              # 数据源基类
│   ├── sina.py              # 新浪数据源
│   └── tencent.py           # 腾讯数据源
├── parsers/                  # 数据解析模块
│   ├── __init__.py
│   └── kline.py             # K线数据解析
└── utils/                    # 工具模块
    ├── __init__.py
    ├── http.py              # HTTP客户端
    └── date.py               # 日期工具
```

## 测试

运行单元测试：

```bash
# 运行所有测试
python -m pytest tests/

# 运行特定测试
python -m pytest tests/test_utils.py
python -m pytest tests/test_cache.py
python -m pytest tests/test_config.py
python -m pytest tests/test_exceptions.py
python -m pytest tests/test_api.py
```

## 注意事项

1. **数据来源**: 数据来源于新浪财经、腾讯财经、东方财富等公开数据源
2. **请求频率**: 建议合理控制请求频率，避免被限制访问
3. **代码格式**:
   - 期货代码需要加`fu`前缀，如`fuAP2110`
   - 美股代码需要加对应后缀，如`usAAPL.OQ` （OQ->NASDAQ, N->NYSE, AM->ETF）
4. **网络要求**: 部分功能需要网络连接，请确保网络畅通
5. **缓存使用**: 建议使用缓存机制减少网络请求，提升性能

## 更新日志

### v0.3.5 (2024)
- 修复Critical Bugs
- 新增配置管理模块
- 新增异常处理体系
- 新增缓存抽象层
- 改进HTTP客户端
- 新增单元测试
- 完善文档

### v0.3.4
- 初始版本

## 贡献

欢迎提交Issue和Pull Request！

## 许可证

Copyright (c) 2021 Roi ZHAO

## 相关文档

- [架构改进建议](ARCHITECTURE_IMPROVEMENTS.md)
- [重构代码示例](REFACTORING_EXAMPLES.md)
- [快速修复清单](QUICK_FIXES.md)
