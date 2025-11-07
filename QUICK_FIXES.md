# 快速修复清单

本文档列出当前代码中需要立即修复的bug和问题。

## 一、Critical Bugs（必须立即修复）

### 1. utils.py 中的 bug

**位置**: `utils.py:58`

**问题**: `WebUtils.http_get` 方法中 `cls.ua` 应该是 `cls.ua()`

```python
# 当前代码（错误）
headers['user-agent'] = cls.ua  # 这会导致 TypeError

# 应该改为
headers['user-agent'] = cls.ua()
```

**影响**: 会导致运行时错误

### 2. utils.py 中的逻辑错误

**位置**: `utils.py:71-89`

**问题**: `test_proxy` 方法中有重复的返回逻辑

```python
# 当前代码
def test_proxy(cls, proxy: str):
    try:
        with httpx.Client(proxies=proxy) as client:
            r = client.get('https://baidu.com', timeout=2)
            if r.ok:
                return 1
            else:
                return 0
    except Exception as e:
        logger.info(f'test proxy {proxy} negative')
        return 0
    if r.ok:  # 这行代码永远不会执行（在except之后）
        logger.info(f'test proxy {proxy} positive')
        return 1
```

**修复**:
```python
def test_proxy(cls, proxy: str) -> int:
    """测试代理是否可用"""
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
```

### 3. main.py 中的异常处理

**位置**: `main.py:433`

**问题**: `HTTPError.warning` 应该是 `logger.warning`

```python
# 当前代码（错误）
raise HTTPError.warning('Error parsing industries data: {}'.format(e))

# 应该改为
logger.warning('Error parsing industries data: {}'.format(e))
# 或者
raise HTTPError('Error parsing industries data: {}'.format(e))
```

**注意**: 还需要检查是否导入了 `HTTPError`，如果没有需要导入或使用其他异常类型。

## 二、设计问题（建议修复）

### 1. hget 类设计不合理

**位置**: `utils.py:181-197`

**问题**: 使用类而不是函数，设计不合理

**当前代码**:
```python
class hget:
    def __init__(self, url, *args, **kwargs):
        self.url = url
        try:
            r = httpx.get(...)
            self.text = r.text
            self.content = r.content
        except Exception as e:
            self.text = ''
            self.content = b''
```

**建议**: 改为函数或改进设计
```python
def hget(url: str, **kwargs) -> Optional[httpx.Response]:
    """HTTP GET请求"""
    try:
        response = httpx.get(
            url, 
            follow_redirects=True, 
            headers=WebUtils.headers(),
            timeout=10,  # 添加超时
            **kwargs
        )
        return response
    except Exception as e:
        logger.error(f'fetch {url} err: {e}')
        return None

# 使用方式改为
response = hget(url)
if response:
    text = response.text
    content = response.content
```

### 2. 缺少错误处理

**位置**: 多个函数

**问题**: 很多函数缺少异常处理，直接返回空值或None

**建议**: 添加适当的异常处理和日志记录

例如 `get_cn_stock_list`:
```python
def get_cn_stock_list(money_min=2e8):
    offset = 0
    count = 200
    df = []
    try:
        while not df or float(df[-1]['turnover'])*1e4 > money_min:
            a = hget(...)
            if a:
                a = json.loads(a.text)
                if a['data']['rank_list']:
                    df.extend(a['data']['rank_list'])
                    offset += count
                else:
                    break
            else:
                logger.warning(f'Failed to fetch stock list at offset {offset}')
                break
    except json.JSONDecodeError as e:
        logger.error(f'Failed to parse stock list data: {e}')
        raise
    except Exception as e:
        logger.error(f'Unexpected error in get_cn_stock_list: {e}')
        raise
    
    return df
```

### 3. 硬编码的URL和配置

**位置**: 整个 `main.py`

**问题**: URL、超时时间等硬编码在代码中

**建议**: 提取到配置文件或常量

```python
# config.py 或 constants.py
class URLs:
    TENCENT_STOCK = 'http://web.ifzq.gtimg.cn/appstock/app/newfqkline/get'
    TENCENT_STOCK_HK = 'http://web.ifzq.gtimg.cn/appstock/app/hkfqkline/get'
    SINA_FUTURE = 'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/'
    # ...

class Config:
    DEFAULT_TIMEOUT = 10
    DEFAULT_RETRY_TIMES = 3
    MAX_PAGE_SIZE = 200
```

## 三、代码质量问题

### 1. 缺少类型提示

**问题**: 所有函数都缺少类型提示

**建议**: 逐步添加类型提示

```python
from typing import List, Dict, Tuple, Optional
import pandas as pd

def get_cn_stock_list(money_min: float = 2e8) -> List[Dict[str, str]]:
    ...

def get_price(
    i: str,
    sdate: str = '',
    edate: str = '',
    freq: str = 'day',
    days: int = 320,
    fq: str = 'qfq',
    dd: Optional[dict] = None
) -> Tuple[str, str, pd.DataFrame]:
    ...
```

### 2. 魔法数字和字符串

**问题**: 代码中有很多魔法数字和字符串

**建议**: 提取为常量

```python
# 当前
if d.open[-1] / d.close[-2] > 1.002:  # 1.002 是什么？

# 改进
BREAK_RISE_THRESHOLD = 1.002  # 突破上涨阈值
if d.open[-1] / d.close[-2] > BREAK_RISE_THRESHOLD:
```

### 3. 函数过长

**问题**: `get_price` 函数超过200行，包含太多逻辑

**建议**: 拆分为多个函数

```python
def get_price(i: str, **kwargs) -> Tuple[str, str, pd.DataFrame]:
    """主函数，根据symbol类型分发到具体处理函数"""
    if i[:2] == 'BK':
        return _get_bk_price(i, **kwargs)
    elif i[:2] == 'fu':
        return _get_future_price(i, **kwargs)
    elif i[:2] == 'pt':
        return _get_pt_price(i, **kwargs)
    elif i[:2] in ['sh', 'sz']:
        return _get_cn_stock_price(i, **kwargs)
    # ...
```

## 四、立即修复步骤

### 步骤1: 修复 Critical Bugs
1. 修复 `WebUtils.http_get` 中的 `cls.ua` bug
2. 修复 `test_proxy` 中的逻辑错误
3. 修复 `get_all_industries` 中的 `HTTPError.warning` 错误

### 步骤2: 改进错误处理
1. 添加统一的异常类
2. 为关键函数添加异常处理
3. 添加日志记录

### 步骤3: 代码质量改进
1. 添加类型提示（从公共API开始）
2. 提取魔法数字为常量
3. 改进函数命名和文档字符串

## 五、测试建议

修复后应该测试：

1. **单元测试**
   - 测试 `WebUtils.http_get` 方法
   - 测试 `hget` 函数
   - 测试各个数据获取函数

2. **集成测试**
   - 测试完整的数据获取流程
   - 测试错误处理路径

3. **回归测试**
   - 确保修复后不影响现有功能
   - 验证向后兼容性

## 六、优先级排序

1. **P0 (立即修复)**
   - `WebUtils.http_get` 中的 `cls.ua` bug
   - `test_proxy` 中的逻辑错误
   - `get_all_industries` 中的异常处理错误

2. **P1 (本周修复)**
   - 改进 `hget` 设计
   - 添加基本错误处理
   - 修复硬编码问题

3. **P2 (下个迭代)**
   - 添加类型提示
   - 代码重构
   - 添加测试

