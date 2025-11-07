# 架构迁移说明

## 概述

`main.py` 已经重构为使用新的模块化架构，同时保持向后兼容性。

## 主要改进

### 1. 使用新的数据源抽象层

- **A股、港股、美股**: 使用 `TencentDataSource` 获取数据
- **期货**: 使用 `SinaDataSource` 获取数据
- **PT代码**: 使用腾讯数据源和解析器

### 2. 使用数据解析器

- 所有K线数据通过 `KlineParser` 解析
- 统一的解析逻辑，易于维护

### 3. 使用缓存系统

- 支持 `MemoryCache` 和 `DictCache`（向后兼容）
- 自动缓存结果，提升性能
- 可通过配置启用/禁用

### 4. 使用日期工具

- `_check_date_format` 现在使用 `check_date_format` 工具函数
- 统一的日期格式处理

### 5. 改进的异常处理

- 使用 `SymbolError`, `DataSourceError`, `ParseError` 等
- 更清晰的错误信息

## 降级机制

为了确保向后兼容性，新架构实现了降级机制：

1. **优先使用新架构**: 首先尝试使用新的数据源和解析器
2. **自动降级**: 如果新架构失败，自动降级到旧方法
3. **日志记录**: 所有降级操作都会记录警告日志

## 代码示例

### 使用新架构（自动）

```python
from rquote import get_price

# 自动使用新架构（TencentDataSource + KlineParser）
sid, name, df = get_price('sh000001')
```

### 使用缓存

```python
from rquote import get_price, MemoryCache

# 使用内存缓存
cache = MemoryCache(ttl=3600)
cache_dict = {}
sid, name, df = get_price('sh000001', dd=cache_dict)
```

### 向后兼容

```python
# 旧的用法仍然有效
sid, name, df = get_price('sh000001', dd={})
```

## 迁移状态

### ✅ 已迁移

- `get_price` - 核心价格获取函数
  - A股、港股、美股（非分钟数据）
  - 期货数据
  - PT代码
  - 缓存支持
  - 日期格式检查

### 🔄 部分迁移

- `get_price` 中的特殊处理（BK、BTC等）仍使用旧方法，但添加了缓存支持

### ⏳ 待迁移

- `get_cn_stock_list`
- `get_hk_stocks_500`
- `get_us_stocks`
- `get_cn_fund_list`
- `get_cn_future_list`
- `get_tick`
- `get_stock_concepts`
- `get_stock_industry`
- `get_all_industries`
- `get_industry_stocks`

## 性能改进

1. **缓存机制**: 减少重复的网络请求
2. **连接池**: HTTP客户端使用连接池，提升性能
3. **重试机制**: 自动重试失败的请求

## 测试建议

运行测试确保新架构正常工作：

```bash
# 测试基本功能
python -m pytest tests/test_api.py -v

# 测试缓存
python -m pytest tests/test_cache.py -v

# 测试数据源
python -c "from rquote import get_price; print(get_price('sh000001', days=10))"
```

## 故障排查

如果遇到问题：

1. **检查日志**: 查看是否有降级警告
2. **验证网络**: 确保可以访问数据源
3. **检查配置**: 验证 `config.py` 中的配置是否正确

## 下一步

1. 逐步迁移其他函数到新架构
2. 完善数据源实现
3. 添加更多测试
4. 性能优化

