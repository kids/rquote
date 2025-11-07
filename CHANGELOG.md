# 更新日志

## v0.3.5 (2024) - 架构重构版本

### 🐛 Bug修复

1. **修复 `WebUtils.http_get` 中的bug**
   - 修复了 `cls.ua` 应该是 `cls.ua()` 的问题
   - 修复了headers参数传递问题

2. **修复 `test_proxy` 方法中的逻辑错误**
   - 移除了永远不会执行的代码
   - 改进了代理测试逻辑
   - 修复了代理格式处理

3. **修复 `get_all_industries` 中的异常处理**
   - 修复了 `HTTPError.warning` 的错误用法
   - 改进了异常处理逻辑

### ✨ 新功能

1. **配置管理模块** (`config.py`)
   - 支持环境变量配置
   - 可自定义HTTP超时、重试次数等
   - 支持缓存配置

2. **异常处理体系** (`exceptions.py`)
   - 定义了统一的异常类层次结构
   - 提供更清晰的错误信息
   - 向后兼容原有的HTTPError

3. **缓存抽象层** (`cache/`)
   - 实现了内存缓存 (`MemoryCache`)
   - 实现了字典缓存适配器 (`DictCache`) 用于向后兼容
   - 支持TTL（过期时间）

4. **改进的HTTP客户端** (`utils/http.py`)
   - 支持重试机制
   - 支持连接池
   - 支持超时控制
   - 改进的错误处理

5. **数据源抽象层** (`data_sources/`)
   - 定义了数据源基类
   - 实现了腾讯数据源 (`TencentDataSource`)
   - 实现了新浪数据源 (`SinaDataSource`)

6. **数据解析器** (`parsers/`)
   - K线数据解析器 (`KlineParser`)
   - 支持多种数据格式解析

7. **工具模块改进** (`utils/`)
   - 日期格式化工具 (`date.py`)
   - 改进的HTTP客户端

### 📝 文档改进

1. **更新README.md**
   - 添加了版本信息
   - 添加了高级功能说明
   - 添加了架构改进说明
   - 添加了测试说明

2. **新增文档**
   - `ARCHITECTURE_IMPROVEMENTS.md` - 架构改进建议
   - `REFACTORING_EXAMPLES.md` - 重构代码示例
   - `QUICK_FIXES.md` - 快速修复清单
   - `CHANGELOG.md` - 更新日志

### 🧪 测试

1. **新增单元测试**
   - `tests/test_utils.py` - 工具模块测试
   - `tests/test_cache.py` - 缓存模块测试
   - `tests/test_config.py` - 配置模块测试
   - `tests/test_exceptions.py` - 异常模块测试
   - `tests/test_api.py` - API集成测试

### 🔧 代码质量改进

1. **模块化设计**
   - 创建了清晰的目录结构
   - 分离了关注点
   - 提高了代码可维护性

2. **向后兼容**
   - 所有原有API保持不变
   - 新增功能为可选使用
   - 支持旧的缓存字典参数

3. **类型提示**
   - 为新模块添加了类型提示
   - 改进了代码可读性

### 📦 目录结构

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
├── utils/                    # 工具模块
│   ├── __init__.py
│   ├── http.py              # HTTP客户端
│   └── date.py               # 日期工具
└── api/                      # API层（向后兼容）
    └── __init__.py
```

### 🔄 向后兼容性

- ✅ 所有原有API函数保持不变
- ✅ 支持旧的缓存字典参数 (`dd`)
- ✅ 保持原有的导入方式
- ✅ 新增功能为可选使用

### 📋 待完成（未来版本）

1. **市场模块重构** (`markets/`)
   - 按市场类型拆分代码
   - 实现市场抽象层

2. **更多测试**
   - 集成测试
   - 性能测试
   - 覆盖率提升

3. **异步支持**
   - 支持async/await
   - 异步HTTP客户端

4. **更多数据源**
   - 东方财富数据源
   - 其他数据源支持

### ⚠️ 注意事项

1. 需要安装依赖：`pandas`, `httpx`
2. 部分功能需要网络连接
3. 建议使用缓存机制减少网络请求

### 🙏 致谢

感谢所有贡献者和用户的支持！

