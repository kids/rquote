# -*- coding: utf-8 -*-
"""
实时行情相关API
"""
from typing import List, Union
from ..utils import hget
from ..exceptions import HTTPError


def get_tick(tgts: Union[List[str], str] = []):
    """
    获取实时行情数据
    
    Args:
        tgts: 股票代码列表或单个代码（美股需要gb_前缀）
    
    Returns:
        行情数据列表
    """
    if not tgts:
        return []
    
    sina_tick = 'https://hq.sinajs.cn/?list='
    head_row = ['name', 'price', 'price_change_rate', 'timesec',
        'price_change', '_', '_', '_', '_', '_', 'volume', '_', '_',
         '_', '_', '_', '_', '_', '_', '_', '_', '_', '_', '_', '_',
         '_', 'last_close', '_', '_', '_', 'turnover', '_', '_', '_', '_']

    if isinstance(tgts, list):
        tgts = ['gb_' + i.lower() for i in tgts]
    elif isinstance(tgts, str):
        tgts = ['gb_' + tgts]
    else:
        raise ValueError('tgt should be list or str, e.g. APPL,')

    a = hget(sina_tick + ','.join(tgts))
    if not a:
        raise HTTPError('hget failed {}'.format(tgts))

    try:
        dat = [i.split('"')[1].split(',') for i in a.text.split(';\n') if ',' in i]
        dat_trim = [{k:i[j] for j,k in enumerate(head_row) if k!='_'} for i in dat]
    except Exception as e:
        raise HTTPError('data not complete, check tgt be code str or list without'+
            ' prefix, your given: {}'.format(tgts))
    return dat_trim

