'''
rquote

A stock history data api and related tools

Copyright (c) 2021 Roi ZHAO

'''

import re
from pathlib import Path

# API函数
from .api import (
    get_price,
    get_price_longer,
    get_all_industries,
    get_stock_concepts,
    get_stock_industry,
    get_cn_stock_list,
    get_hk_stocks_500,
    get_cn_future_list,
    get_us_stocks,
    get_cn_fund_list,
    get_tick,
    get_industry_stocks,
    get_cnindex_stocks
)

# 工具类
from .utils import WebUtils, hget, logger
from .factors import BasicFactors
from .plots import PlotUtils

# 新增模块（可选使用）
from . import config
from . import exceptions
from .cache import MemoryCache, Cache
# 尝试导入持久化缓存（可选依赖）
try:
    from .cache import PersistentCache
except ImportError:
    PersistentCache = None
from .utils.http import HTTPClient


def _get_version():
    """从 pyproject.toml 读取版本号"""
    # 优先尝试从已安装的包中读取版本
    try:
        from importlib import metadata
        return metadata.version("rquote")
    except Exception:
        pass
    
    # 如果包未安装，从 pyproject.toml 读取
    try:
        # 获取项目根目录（__init__.py 的父目录的父目录）
        current_file = Path(__file__).resolve()
        project_root = current_file.parent.parent
        pyproject_path = project_root / "pyproject.toml"
        
        if pyproject_path.exists():
            with open(pyproject_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # 使用正则表达式匹配 version = "x.x.x"
                match = re.search(r'version\s*=\s*["\']([^"\']+)["\']', content)
                if match:
                    return match.group(1)
    except Exception:
        pass
    
    # 如果都失败了，返回默认版本
    return "0.0.0"


__version__ = _get_version()

__all__ = [
    # API函数
    'get_price',
    'get_price_longer',
    'get_all_industries',
    'get_stock_concepts',
    'get_stock_industry',
    'get_cn_stock_list',
    'get_hk_stocks_500',
    'get_cn_future_list',
    'get_us_stocks',
    'get_cn_fund_list',
    'get_tick',
    'get_industry_stocks',
    'get_cnindex_stocks',
    # 工具类
    'WebUtils',
    'BasicFactors',
    'PlotUtils',
    # 新增模块
    'config',
    'exceptions',
    'MemoryCache',
    'Cache',
    'PersistentCache',
    'HTTPClient',
]
