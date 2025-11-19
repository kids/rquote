# -*- coding: utf-8 -*-
"""
列表相关API
"""
import json
import re
import base64
import time
from urllib.parse import urlencode
from ..utils import hget, logger
from ..exceptions import HTTPError


def get_cn_stock_list(money_min=2e8):
    """
    获取A股股票列表
    
    Args:
        money_min: 最小成交额
    
    Returns:
        股票列表
    """
    offset = 0
    count = 200  # max, or error
    df = []
    while not df or float(df[-1]['turnover'])*1e4 > money_min:
        a = hget(
            'https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList?_appver=11.17.0'+
            f'&board_code=aStock&sort_type=turnover&direct=down&offset={offset}&count={count}'
        )
        if a:
            a = json.loads(a.text)
            if a['data']['rank_list']:
                df.extend(a['data']['rank_list'])
                offset += count
            else:
                break
    return df


def get_hk_stocks_500():
    """
    获取港股前500只股票列表
    
    Returns:
        股票列表
    """
    a = hget(
        'https://stock.gtimg.cn/data/hk_rank.php?board=main_all&metric=amount&' +
        'pageSize=500&reqPage=1&order=desc&var_name=list_data')
    if a:
        a = [i.split('~') for i in json.loads(a.text.split('list_data=')[1])['data']['page_data']]
    return a


def get_us_stocks(k=100):
    """
    获取美股列表
    
    Args:
        k: 获取数量
    
    Returns:
        股票列表
    """
    uscands = []
    page_n = k//20 + 1
    for page in range(1, page_n+1):
        a = hget(
            "https://stock.finance.sina.com.cn/usstock/api/jsonp.php/IO.XSRV2."+
            f"CallbackList['f0j3ltzVzdo2Fo4p']/US_CategoryService.getList?page={page}"+
            "&num=20&sort=&asc=0&market=&id=")
        if a:
            uslist = json.loads(a.text.split('(',1)[1][:-2])['data']
            uscands.extend(uslist)
    return uscands


def get_cn_fund_list():
    """
    获取A股ETF基金列表（sina）
    
    Returns:
        基金列表，格式: [code, name, change, amount, price]
    """
    a = hget(base64.b64decode('aHR0cDovL3ZpcC5zdG9jay5maW5hbmNlLnNpbmEuY29tL'+
        'mNuL3F1b3Rlc19zZXJ2aWNlL2FwaS9qc29ucC5waHAvSU8uWFNSVjIuQ2FsbGJhY2tMaX'+
        'N0WydrMldhekswNk5Rd2xoeVh2J10vTWFya2V0X0NlbnRlci5nZXRIUU5vZGVEYXRhU2l'+
        'tcGxlP3BhZ2U9MSZudW09MTAwMCZzb3J0PWFtb3VudCZhc2M9MCZub2RlPWV0Zl9ocV9m'+
        'dW5kJiU1Qm9iamVjdCUyMEhUTUxEaXZFbGVtZW50JTVEPXhtNGkw').decode())
    if a:
        fundcands = [[i['symbol'], i['name'], i['changepercent'], i['amount'], i['trade']]
                     for i in json.loads(a.text.split('k2WazK06NQwlhyXv')[1][3:-2])]
    return fundcands


def get_cn_future_list():
    """
    获取国内期货合约列表
    
    Returns:
        期货代码列表，带fu前缀
    """
    a = hget('https://finance.sina.com.cn/futuremarket/')
    if a:
        futurelist_active = [
            'fu' + i for i in re.findall(r'quotes/(.*?\d+).shtml', a.text)]
    return futurelist_active


def get_all_industries():
    """
    获取所有行业板块列表
    
    Returns:
        行业列表，格式: [code, name, change, amount, price, sina_sw2_id]
    """
    try:
        url = 'https://proxy.finance.qq.com/cgi/cgi-bin/rank/pt/getRank?board_type=hy2&sort_type=price&direct=down&offset=0&count=200'
        a = hget(url)
        if not a:
            raise HTTPError('Failed to fetch industries from QQ Finance')
        data = json.loads(a.text)
        if data.get('code') != 0:
            logger.warning('API returned error: {}'.format(data.get('msg', 'Unknown error')))
            return []
        
        rank_list = data.get('data', {}).get('rank_list', [])
        industries = [
            [item['code'], item['name'], item.get('zdf', '0'), 
             item.get('zllr', '0'), item.get('zxj', '0')]
            for item in rank_list
        ]
        logger.debug('get industries {}'.format(len(industries)))
    except Exception as e:
        logger.warning('Error parsing industries data: {}'.format(e))
        industries = []
    
    try:
        url = 'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodes'
        sina_industries = hget(url)
        if not sina_industries:
            raise HTTPError('Failed to fetch industries from Sina Finance')
        data = json.loads(sina_industries.text)
        sina_industries = data[1][0][1]
        sina_sw2 = sina_industries[3][1]
        sina_sw2_dict = {i[0]:i[2] for i in sina_sw2}
    except Exception as e:
        logger.warning(f'Error parsing sina industries data: {e}')
        sina_sw2_dict = {}
    
    # 利用sina_sw2_dict给industries添加sina_sw2_id
    for industry in industries:
        industry.append(sina_sw2_dict.get(industry[1], ''))
    return industries


def get_industry_stocks(node):
    """
    获取指定行业板块的股票列表
    
    Args:
        node: 行业节点名称
    
    Returns:
        股票列表
    """
    url = f'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/'+\
    f'Market_Center.getHQNodeData?page=1&num=40&sort=symbol&asc=1&node={node}&symbol=&_s_r_a=init'
    a = hget(url)
    if not a:
        raise HTTPError('Failed to fetch industry stocks from Sina Finance')
    data = json.loads(a.text)
    return data


def get_cnindex_stocks(index_type='hs300'):
    """
    获取中国指数成分股列表
    
    Args:
        index_type: 指数类型，可选值: 'hs300', 'zz500', 'zz1000'
                    hs300: 沪深300 (TYPE=1)
                    zz500: 中证500 (TYPE=3)
                    zz1000: 中证1000 (TYPE=7)
    
    Returns:
        股票列表，包含 SECUCODE, SECURITY_CODE, SECURITY_NAME_ABBR, CLOSE_PRICE 等字段
    """
    # 指数类型到 TYPE 值的映射
    index_type_map = {
        'hs300': '1',
        'zz500': '3',
        'zz1000': '7'
    }
    
    if index_type not in index_type_map:
        raise ValueError(f"不支持的指数类型: {index_type}，支持的类型: {list(index_type_map.keys())}")
    
    type_value = index_type_map[index_type]
    
    # 构建 URL
    base_url = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
    params = {
        'callback': 'jQuery112308471143523381743_1763517709888',
        'sortColumns': 'SECURITY_CODE',
        'sortTypes': '-1',
        'pageSize': '500',
        'pageNumber': '1',
        'reportName': 'RPT_INDEX_TS_COMPONENT',
        'columns': 'SECURITY_CODE,SECURITY_NAME_ABBR,INDUSTRY,WEIGHT,EPS,BPS,ROE,FREE_CAP',
        'quoteColumns': '',
        'quoteType': '0',
        'source': 'WEB',
        'client': 'WEB',
        'filter': f'(TYPE="{type_value}")'
    }
    
    # 构建完整 URL
    url = f'{base_url}?{urlencode(params)}'
    
    # 发送请求
    a = hget(url)
    if not a:
        raise HTTPError(f'Failed to fetch {index_type} stocks from EastMoney')
    
    # 解析 JSONP 格式的返回数据
    # 格式: jQuery112308471143523381743_1763517709888({...})
    json_str = a.text.split('(', 1)[1].rstrip(');')
    data = json.loads(json_str)
    
    # 返回 result.data 中的数据列表
    if data.get('result') and data['result'].get('data'):
        return data['result']['data']
    else:
        logger.warning(f'No data found in response for {index_type}')
        return []

