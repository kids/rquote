'''
rquote

A stock history data api and related tools

Copyright (c) 2021 Roi ZHAO

'''

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
    get_industry_stocks
)

# 工具类
from .utils import WebUtils, hget, logger
from .factors import BasicFactors
from .plots import PlotUtils

# 新增模块（可选使用）
from . import config
from . import exceptions
from .cache import MemoryCache, Cache
from .utils.http import HTTPClient

__version__ = '0.3.5'

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
    # 工具类
    'WebUtils',
    'BasicFactors',
    'PlotUtils',
    # 新增模块
    'config',
    'exceptions',
    'MemoryCache',
    'Cache',
    'HTTPClient',
]
