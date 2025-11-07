# -*- coding: utf-8 -*-
"""
股票信息相关API
"""
import json
from typing import List
from ..utils import hget
from ..exceptions import HTTPError


def get_stock_concepts(i: str) -> List[str]:
    """
    获取指定股票所属的概念板块
    
    Args:
        i: 股票代码
    
    Returns:
        概念代码列表
    """
    url = f'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/stockinfo/plateNew?code={i}&app=wzq&zdf=1'
    a = hget(url)
    if not a:
        raise HTTPError('Failed to fetch concepts from QQ Finance')
    data = json.loads(a.text)
    if data.get('code') != 0:
        raise HTTPError('API returned error: {}'.format(data.get('msg', 'Unknown error')))
    return data.get('data', {}).get('concept', [])


def get_stock_industry(i: str) -> List[str]:
    """
    获取指定股票所属的行业板块
    
    Args:
        i: 股票代码
    
    Returns:
        行业代码列表
    """
    url = f'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/stockinfo/plateNew?code={i}&app=wzq&zdf=1'
    a = hget(url)
    if not a:
        raise HTTPError('Failed to fetch industry from QQ Finance')
    data = json.loads(a.text)
    if data.get('code') != 0:
        raise HTTPError('API returned error: {}'.format(data.get('msg', 'Unknown error')))
    return data.get('data', {}).get('plate', [])

