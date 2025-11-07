# -*- coding: utf-8 -*-
"""
列表相关API
"""
import json
import re
import base64
import time
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
    获取A股ETF基金列表
    
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
        node: 行业节点代码
    
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

