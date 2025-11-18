# -*- coding: utf-8 -*-
"""
股票信息相关API
"""
import json
from typing import List, Optional, Any
from ..utils import hget
from ..exceptions import HTTPError
from ..cache import Cache
from ..cache.memory import DictCache as DictCacheAdapter


def _normalize_stock_code(code: str) -> str:
    """标准化股票代码"""
    return {'6': 'sh', '0': 'sz', '3': 'sz'}.get(code[0], '') + code if code[0] in ['6', '0', '3'] else code


def _get_cache_adapter(dd: Optional[Any]) -> Optional[Cache]:
    """获取缓存适配器"""
    if dd is None:
        return None
    if isinstance(dd, dict):
        return DictCacheAdapter(dd)
    elif isinstance(dd, Cache):
        return dd
    elif hasattr(dd, 'get') and hasattr(dd, 'put'):
        return DictCacheAdapter(dd)
    return None


def _fetch_stock_plate_data(normalized_code: str, data_key: str, error_msg: str) -> List[str]:
    """获取股票板块数据的通用函数"""
    url = f'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/stockinfo/plateNew?code={normalized_code}&app=wzq&zdf=1'
    a = hget(url)
    if not a:
        raise HTTPError(f'Failed to fetch {error_msg} from QQ Finance')
    data = json.loads(a.text)
    if data.get('code') != 0:
        raise HTTPError('API returned error: {}'.format(data.get('msg', 'Unknown error')))
    return data.get('data', {}).get(data_key, [])


def get_stock_concepts(i: str, dd: Optional[Any] = None) -> List[str]:
    """
    获取指定股票所属的概念板块
    
    Args:
        i: 股票代码
        dd: data dictionary或Cache对象，任何有get/put方法的本地缓存
    
    Returns:
        概念代码列表
    """
    cache = _get_cache_adapter(dd)
    normalized_code = _normalize_stock_code(i)
    cache_key = f'stock_concepts:{normalized_code}'
    
    # 尝试从缓存获取
    if cache:
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            return cached_result
    
    # 缓存未命中，请求网络
    result = _fetch_stock_plate_data(normalized_code, 'concept', 'concepts')
    
    # 存入缓存
    if cache:
        cache.put(cache_key, result)
    
    return result


def get_stock_industry(i: str, dd: Optional[Any] = None) -> List[str]:
    """
    获取指定股票所属的行业板块
    
    Args:
        i: 股票代码
        dd: data dictionary或Cache对象，任何有get/put方法的本地缓存
    
    Returns:
        行业代码列表
    """
    cache = _get_cache_adapter(dd)
    normalized_code = _normalize_stock_code(i)
    cache_key = f'stock_industry:{normalized_code}'
    
    # 尝试从缓存获取
    if cache:
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            return cached_result
    
    # 缓存未命中，请求网络
    result = _fetch_stock_plate_data(normalized_code, 'plate', 'industry')
    
    # 存入缓存
    if cache:
        cache.put(cache_key, result)
    
    return result

