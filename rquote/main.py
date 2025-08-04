# -*- coding: utf-8 -*-

import json
import time
import re
import base64
import pandas as pd
from .utils import WebUtils, hget, logger



def get_cn_stock_list(money_min=2e8):
    ret = []
    try:
        ret = get_cn_stock_list_qq(money_min)
    except Exception as e:
        ret = get_cn_stock_list_eastmoney(money_min)
    return ret

def get_cn_stock_list_eastmoney(money_min=2e8):
    '''
    Return sorted stock list ordered by latest amount of money, cut at `money_min`
    item in returned list are [code, name, change, amount, mktcap]
    '''
    a = hget(
        base64.b64decode('aHR0cDovLzM4LnB1c2gyLmVhc3Rtb25leS5jb20vYXBpL3F0L2Ns'+
            'aXN0L2dldD9jYj1qUXVlcnkxMTI0MDk0NTg3NjE4NDQzNzQ4MDFfMTYyNzI4ODQ4O'+
            'Tk2MSZwbj0xJnB6PTEwMDAwJnBvPTEmbnA9MSZ1dD1iZDFkOWRkYjA0MDg5NzAwY2'+
            'Y5YzI3ZjZmNzQyNjI4MSZmbHR0PTImaW52dD0yJmZpZD1mNiZmcz1tOjArdDo2LG0'+
            '6MCt0OjgwLG06MSt0OjIsbToxK3Q6MjMmZmllbGRzPWYxMixmMTQsZjMsZjYsZjIxJl89'
            ).decode() + str(int(time.time()*1e3))
    )
    if a:
        a = json.loads(a.text.split(
            'jQuery112409458761844374801_1627288489961(')[1][:-2])
    a = [ ['sh'+i['f12'] if i['f12'][0]=='6' else 'sz'+i['f12'],
         i['f14'], i['f3'], i['f6'], i['f21']] for i in a['data']['diff']
        if i['f6']!='-' and float(i['f6']) > money_min]
    #cands=[(i.code,i.name) for i in a[['code','name']].itertuples()]
    return a

def get_cn_stock_list_qq(money_min=2e8):
    offset = 0
    count = 200 # max, or error
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
    a = hget(
        'https://stock.gtimg.cn/data/hk_rank.php?board=main_all&metric=amount&' +
        'pageSize=500&reqPage=1&order=desc&var_name=list_data').text
    if a:
        a = [i.split('~') for i in json.loads(a.split('list_data=')[1])['data']['page_data']]
    return a


def get_us_stocks(k=100):
    # return list of [symbol, name, price, volume, mktcap, pe]
    uscands = []
    page_n = k//20 + 1
    for page in range(1, page_n+1):
        a = hget(
            "https://stock.finance.sina.com.cn/usstock/api/jsonp.php/IO.XSRV2."+
            f"CallbackList['f0j3ltzVzdo2Fo4p']/US_CategoryService.getList?page={page}"+
            "&num=20&sort=&asc=0&market=&id=").text
        if a:
            uslist = json.loads(a.split('(',1)[1][:-2])['data']
            uscands.extend(uslist)
    return uscands


def get_cn_fund_list():
    '''
    Return sorted etf list (ordered by latest amount of money),
        of [code, name, change, amount, price]
    '''
    a = hget(base64.b64decode('aHR0cDovL3ZpcC5zdG9jay5maW5hbmNlLnNpbmEuY29tL'+
        'mNuL3F1b3Rlc19zZXJ2aWNlL2FwaS9qc29ucC5waHAvSU8uWFNSVjIuQ2FsbGJhY2tMaX'+
        'N0WydrMldhekswNk5Rd2xoeVh2J10vTWFya2V0X0NlbnRlci5nZXRIUU5vZGVEYXRhU2l'+
        'tcGxlP3BhZ2U9MSZudW09MTAwMCZzb3J0PWFtb3VudCZhc2M9MCZub2RlPWV0Zl9ocV9m'+
        'dW5kJiU1Qm9iamVjdCUyMEhUTUxEaXZFbGVtZW50JTVEPXhtNGkw').decode()).text
    if a:
        fundcands = [[i['symbol'], i['name'], i['changepercent'], i['amount'], i['trade']]
                     for i in json.loads(a.split('k2WazK06NQwlhyXv')[1][3:-2])]
    return fundcands


def get_cn_future_list():
    '''
    Return cn future id list, with prefix of `fu`
    e.g. ['fuSC2109',
          'fuRB2110',
          'fuHC2110',
          'fuFU2109',
          ...]
    '''
    a = hget('https://finance.sina.com.cn/futuremarket/').text
    if a:
        futurelist_active = [
            'fu' + i for i in re.findall(r'quotes/(.*?\d+).shtml', a)]
    return futurelist_active


def _check_date_format(date_str):
    # 允许格式: 2099-01-01
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        # 尝试转换
        try:
            # 常见格式尝试
            t_struct = None
            for fmt in ("%Y/%m/%d", "%Y%m%d", "%Y.%m.%d", "%Y_%m_%d", "%Y-%m-%d"):
                try:
                    t_struct = time.strptime(date_str, fmt)
                    break
                except Exception:
                    continue
            if t_struct is None:
                raise ValueError(f"date format not recognized: {date_str}")
            # 转换为标准格式
            date_str_std = time.strftime("%Y-%m-%d", t_struct)
            return date_str_std
        except Exception as e:
            raise ValueError(f"date format error: {date_str}, {e}")
    return date_str


def get_price(i, sdate='', edate='', freq='day', days=320, fq='qfq',
              dd=None) -> (str, str, pd.DataFrame):
    '''
    Args:
        sdate: start date
        edate: end date
        dd: data dictionary, any local cache with get/put methods
        days: day length of fetching, overwriting sdate
        fq: qfq for non
    '''
    if dd is not None:
        a = dd.get(i)
        if a:
            n, d = a
            logger.debug('loading price from dd {}'.format(i))
            return i, n, d
    logger.debug('fetching price of {}'.format(i))

    # 检查sdate和edate格式
    sdate = _check_date_format(sdate) if sdate else ''
    edate = _check_date_format(edate) if edate else ''
        

    qtimg_stock = 'http://web.ifzq.gtimg.cn/appstock/app/newfqkline/get?param=' + \
            '{},{},{},{},{},{}'
    qtimg_stock_hk = 'http://web.ifzq.gtimg.cn/appstock/app/hkfqkline/get?' + \
            'param={},{},{},{},{},{}'
    qtimg_stock_us = 'http://web.ifzq.gtimg.cn/appstock/app/usfqkline/get?' + \
            'param={},{},{},{},{},{}'
    qtimg_stock_us_min = 'https://web.ifzq.gtimg.cn/appstock/app/UsMinute/query?' + \
            '_var=min_data_{}&code={}'
    sina_future_d = 'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/' + \
            'var%20t1nf_{}=/InnerFuturesNewService.getDailyKLine?symbol={}'
    sina_btc = 'https://quotes.sina.cn/fx/api/openapi.php/BtcService.getDayKLine?' + \
        'symbol=btcbtcusd'

    # sina_future_d.format('FB0','FB0')

    if i[:2] == 'BK':
        try:
            a = hget(base64.b64decode('aHR0cDovL3B1c2gyaGlzLmVhc3' +
                               'Rtb25leS5jb20vYXBpL3F0L3N0b2NrL2tsaW5lL2dldD9jYj1qUX' +
                               'VlcnkxMTI0MDIyNTY2NDQ1ODczNzY2OTcyXzE2MTc4NjQ1NjgxMz' +
                               'Emc2VjaWQ9OTAu').decode() + i +
                          '&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5' +
                          '&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58' +
                          '&klt=101&fqt=0&beg=19900101&end=20990101&_=1')
            if not a:
                logger.warning('{} hget failed: {}'.format(i, a))
                return i, 'None', pd.DataFrame([])
            a = json.loads(a.text.split(
                'jQuery1124022566445873766972_1617864568131(')[1][:-2])
            if not a['data']:
                logger.warning('{} data empty: {}'.format(i, a))
                return i, 'None', pd.DataFrame([])
            name = a['data']['name']
            d = pd.DataFrame([i.split(',') for i in a['data']['klines']], columns=[
                             'date', 'open', 'close', 'high', 'low', 'vol', 'money', 'p'])
            d = d.set_index(['date']).astype(float)
            # d.index = pd.DatetimeIndex(d.index)
            return i, name, d
        except Exception as e:
            logger.warning('error fetching {}, err: {}'.format(i, e))
            return i, 'None', pd.DataFrame([])

    if i[:2] == 'fu':
        try:
            if i[2:5].lower() == 'btc':
                url = sina_btc
                d = json.loads(hget(url).text)['result']['data'].split('|')
                d = pd.DataFrame([i.split(',') for i in d], 
                                 columns=['date', 'open', 'high', 'low', 'close', 'vol', 'amount'])
                return i, 'BTC', d
            else:
                d = pd.DataFrame(json.loads(hget(sina_future_d.format(
                        ix, ix)).text.split('(')[1][:-2]))
                d.columns = ['date', 'open', 'high', 'low', 'close', 'vol', 'p', 's']
            d = d.set_index(['date']).astype(float)
            # d.index = pd.DatetimeIndex(d.index)
            return i, ix, d
        except Exception as e:
            logger.warning('error get price {}, err {}'.format(i[2:-4], e))
            return i, 'None', pd.DataFrame([])

    if i[0] in ['0', '1', '3', '5', '6']:
        i = 'sh'+i if i[0] in ['5', '6'] else 'sz'+i
    if i[:2] in ['sh', 'sz']:
        url = qtimg_stock.format(i, freq, sdate, edate, days, fq)
    elif i[:2] == 'hk':
        url = qtimg_stock_hk.format(i, freq, sdate, edate, days, fq)
    elif i[:2] == 'us':
        if freq in ('min', '1min', 'minute'):
            url = qtimg_stock_us_min.format(i.replace('.', ''), i)
        else:
            url = qtimg_stock_us.format(i, freq, sdate, edate, days, fq)
    else:
        raise ValueError(f'target market not supported: {i}')
    a = hget(url)
    #a = json.loads(a.text.replace('kline_dayqfq=', ''))['data'][i]
    if i[:2] == 'us' and freq in ('min', '1min', 'minute'):
        a = json.loads(a.text.split('=')[1])['data'][i]
        nm = a['qt']['usAMZN.OQ'][1]
        b = pd.DataFrame([i.split() for i in a['data']['data']],
        columns=['minute','price','volume']).set_index(['minute']).astype(str)
        return i, nm, b
    a = json.loads(a.text)['data'][i]
    name = ''
    try:
        for tkt in ['day', 'qfqday', 'hfqday', 'week', 'qfqweek', 'hfqweek',
                    'month', 'qfqmonth', 'hfqmonth']:
            if tkt in a:
                tk = tkt
                break
        b = pd.DataFrame([j[:6] for j in a[tk]],
                         columns=['date',
                                  'open',
                                  'close',
                                  'high',
                                  'low',
                                  'vol']).set_index(['date']).astype(float)
        if 'qt' in a:
            name = a['qt'][i][1]
    except Exception as e:
        raise ValueError('error fetching {}, err: {}'.format(i, e))
    return i, name, b


def get_price_longer(i, l=2, dd={}):
    # default get price 320 day, l as years
    _, name, a = get_price(i, dd=dd)
    d1 = a.index.format()[0]
    for y in range(1, l):
        d0 = str(int(d1[:4]) - 1) + d1[4:]
        a = pd.concat((get_price(i, d0, d1)[2], a), 0).drop_duplicates()
        d1 = d0
    return i, name, a


def get_tick(tgts=[]):
    '''
    Get quotes of a tick
    tgt list format:
        us stocks like gb_symbol, e.g. gb_aapl, gb_goog
    Return list of dict of given symbols for current timestamp
    '''
    if not tgts:
        return []
    sina_tick = 'https://hq.sinajs.cn/?list='
    head_row = ['name', 'price', 'price_change_rate', 'timesec',
        'price_change', '_', '_', '_', '_', '_', 'volume', '_', '_',
         '_', '_', '_', '_', '_', '_', '_', '_', '_', '_', '_', '_',
         '_', 'last_close', '_', '_', '_', 'turnover', '_', '_', '_', '_']

    if type(tgts) == list:
        tgts = ['gb_' + i.lower() for i in tgts]
    elif type(tgts) == str:
        tgts = ['gb_' + tgts]
    else:
        raise ValueError('tgt should be list or str, e.g. APPL,')

    a = hget(sina_tick + ','.join(tgts))
    if not a:
        raise ValueError('hget failed {}'.format(tgts))

    try:
        dat = [i.split('"')[1].split(',') for i in a.text.split(';\n') if ',' in i]
        dat_trim = [{k:i[j] for j,k in enumerate(head_row) if k!='_'} for i in dat]
    except Exception as e:
        raise ValueError('data not complete, check tgt be code str or list without'+
            ' prefix, your given: {}'.format(tgts))
    return dat_trim


def get_stock_concepts(i) -> []:
    '''
    Return concept id(start with `BK`) list of a stock, from eastmoney
    '''
    f10url = base64.b64decode('aHR0cDovL2YxMC5lYXN0bW9uZXkuY29tLy9Db3JlQ29uY2V' +
                              'wdGlvbi9Db3JlQ29uY2VwdGlvbkFqYXg/Y29kZT0=').decode()
    #drop_cons = ['融资融券', '创业板综', '深股通', '沪股通', '深成500', '长江三角']
    #drop_tails = ['板块', '概念', '0_', '成份', '重仓']
    url = f10url + i
    try:
        concepts = json.loads(hget(url).text)[
            'hxtc'][0]['ydnr'].split()
    except Exception as e:
        raise ValueError(f'error fetching concepts of {i}, err: {e}')
    #concepts = [i for i in concepts if i not in drop_cons]
    #concepts = [i for i in concepts if i[-2:] not in drop_tails]
    #concepts = [i for i in concepts if '股' not in i]
    return concepts


def get_concept_stocks(bkid, dc=None):
    '''
    Return stocks of input bkid, e.g. BK0420, BK0900
    dc : dictionary of concepts, local cache with get/put
    '''
    if dc is not None:
        a = dc.get(bkid)
        if a:
            return a
    bkid = bkid if isinstance(bkid, str) else 'BK' + str(bkid).zfill(4)
    a = hget(
        base64.b64decode('aHR0cDovL3B1c2gyLmVhc3Rtb25leS5jb20vYXBpL3F0L2NsaXN0' +
                         'L2dldD9jYj1qUXVlcnkxMTIzMDQwNTcwNTM4NTY5NDcwMTA1XzE2MTgwNDc5OTA2O' +
                         'TAmZmlkPWY2MiZwbz0xJnB6PTUwMCZwbj0xJm5wPTEmZmx0dD0yJmludnQ9MiZmcz' +
                         '1iJTNB').decode() +
        bkid +
        '&fields=f3%2Cf6%2Cf12%2Cf14%2Cf21').text
    a = json.loads(
        a.split('jQuery1123040570538569470105_1618047990690(')[1][:-2])['data']['diff']
    logger.debug('get fresh conc {}'.format(bkid))
    a = [ ['sh'+i['f12'] if i['f12'][0]=='6' else 'sz'+i['f12'],
         i['f14'], i['f3'], i['f6'], i['f21']] for i in a]
    return a


def _east_list_fmt(burl, api_name):
    '''
    formatter of eastmoney api
    Return list of list:
        [sid, name, rise, amount, mkt]
    '''
    a = hget(base64.b64decode(burl).decode() +
        str(int(time.time()*1e3)))
    if a:
        a = a.text
    else:
        return
    a = json.loads(a.split(api_name+'(')[1][:-2])['data']['diff']
    a = [ [i['f12'],i['f14'], i['f3'], i['f6'], i['f20']] for i in a]
    return a


def get_all_industries():
    '''
    Return sorted industry item list ordered by latest amount of money,
    item in returned list are [code, name, change, amount, price]
    '''
    a = _east_list_fmt('aHR0cHM6Ly84Ny5wdXNoMi5lYXN0bW9uZXkuY29tL2FwaS9xdC9jbGl'+
        'zdC9nZXQ/Y2I9alF1ZXJ5MTEyNDAzNzExNzU2NTU3MTk3MTM0NV8xNjI3MDQ3MTg4NTk5'+
        'JnBuPTEmcHo9MTAwJnBvPTEmbnA9MSZ1dD1iZDFkOWRkYjA0MDg5NzAwY2Y5YzI3ZjZmN'+
        'zQyNjI4MSZmbHR0PTImaW52dD0yJmZpZD1mMyZmcz1tOjkwK3Q6MitmOiE1MCZmaWVsZH'+
        'M9ZjMsZjYsZjEyLGYxNCxmMjAsZjEwNCxmMTA1Jl89',
        'jQuery1124037117565571971345_1627047188599')
    logger.debug('get industries {}'.format(len(a)))
    return a


def get_all_concepts():
    '''
    Return sorted concept item list ordered by latest amount of money,
    item in returned list are [code, name, change, amount, price]
    '''
    a = _east_list_fmt('aHR0cHM6Ly8yMi5wdXNoMi5lYXN0bW9uZXkuY29tL2FwaS9xdC9jbGl'+
        'zdC9nZXQ/Y2I9alF1ZXJ5MTEyNDA3MzI5ODQxOTMwNzY4OTc5XzE2MjcxMDk0NjA2MzMm'+
        'cG49MSZwej00MDAmcG89MSZucD0xJnV0PWJkMWQ5ZGRiMDQwODk3MDBjZjljMjdmNmY3N'+
        'DI2MjgxJmZsdHQ9MiZpbnZ0PTImZmlkPWYzJmZzPW06OTArdDozK2Y6ITUwJmZpZWxkcz'+
        '1mMyxmNixmMTIsZjE0LGYyMCxmMTA0LGYxMDUmXz0='
        ,'jQuery112407329841930768979_1627109460633')
    logger.debug('get concepts {}'.format(len(a)))
    return a


def get_bk_stocks(bkid):
    '''
    Return stock item list of given bk id,
    item in returned list are [code, name, change, amount, price]
    '''
    url = base64.b64decode('aHR0cDovLzgyLnB1c2gyLmVhc3Rtb25leS5jb20vYXBpL3F0L2'+
        'NsaXN0L2dldD9jYj1qUXVlcnkxMTI0MDQ4Njk5NjMwMDk1MTM3NzE0XzE2Mjc0Nzc0OTU'+
        'wNjQmcG49MSZwej0yMDAwJnBvPTAmbnA9MSZ1dD1iZDFkOWRkYjA0MDg5NzAwY2Y5YzI3'+
        'ZjZmNzQyNjI4MSZmbHR0PTImaW52dD0yJmZpZD1mNiZmcz1iOg==').decode()+ \
        bkid + '+f:!50&fields=f3,f6,f12,f14,f20&_='
    a = _east_list_fmt(base64.b64encode(bytes(url, encoding='utf-8')),
        'jQuery1124048699630095137714_1627477495064')
    logger.debug('get bk stocks {}'.format(len(a)))
    return a


def get_industry_stocks(bkid):
    '''
    Return sorted industry item list ordered by latest amount of money,
    item in returned list are [code, name, change, amount, price]
    '''
    url = base64.b64decode('aHR0cHM6Ly82Mi5wdXNoMi5lYXN0bW9uZXkuY29tL2FwaS9xdC'+
        '9jbGlzdC9nZXQ/Y2I9alF1ZXJ5MTEyNDA4Mzc4MjAwMDc0NDQ0MzA5XzE2Mjc4MjQ2MDM'+
        '1NjImcG49MSZwej0yMDAwJnBvPTAmbnA9MSZ1dD1iZDFkOWRkYjA0MDg5NzAwY2Y5YzI3'+
        'ZjZmNzQyNjI4MSZmbHR0PTImaW52dD0yJmZpZD1mNiZmcz1iOg==').decode()+ \
        bkid + '+f:!50&fields=f3,f6,f12,f14,f20&_='
    a = _east_list_fmt(base64.b64encode(bytes(url, encoding='utf-8')),
        'jQuery112408378200074444309_1627824603562')
    logger.debug('get industry stocks {}'.format(len(a)))
    return a


def get_hk_stocks_ggt():
    '''
    Return sorted stock item list in GangGuTong, ordered by amount of money,
    item in returned list are [code, name, change, amount, price]
    '''
    a = _east_list_fmt('aHR0cHM6Ly8yLnB1c2gyLmVhc3Rtb25leS5jb20vYXBpL3F0L2NsaX'+
        'N0L2dldD9jYj1qUXVlcnkxMTI0MDI0MzYyMzA4OTA2NjE1MDgyXzE2MjgyNTg5MzEyMjQ'+
        'mcG49MSZwej0xMDAwJnBvPTAmbnA9MSZ1dD1iZDFkOWRkYjA0MDg5NzAwY2Y5YzI3ZjZm'+
        'NzQyNjI4MSZmbHR0PTImaW52dD0yJmZpZD1mNiZmcz1iOkRMTUswMTQ2LGI6RExNSzAxN'+
        'DQmZmllbGRzPWYzLGY2LGYxMixmMTQsZjIwJl89',
        'jQuery1124024362308906615082_1628258931224')
    a = [ ['hk'+i[0], i[1], i[2], i[3], i[4]] for i in a]
    logger.debug('get hk stocks GangGuTong {}'.format(len(a)))
    return a


def get_hk_stocks_hsi():
    '''
    Return sorted stock item list in HSI, ordered by amount of money,
    item in returned list are [code, name, change, amount, price]
    '''
    a = _east_list_fmt('aHR0cHM6Ly81Ni5wdXNoMi5lYXN0bW9uZXkuY29tL2FwaS9xdC9jbG'+
        'lzdC9nZXQ/Y2I9alF1ZXJ5MTEyNDA3ODg4ODY4NDU5NDc5NzkyXzE2MjgyNTk1NjQ2NzE'+
        'mcG49MSZwej0xMDAwJnBvPTEmbnA9MSZ1dD1iZDFkOWRkYjA0MDg5NzAwY2Y5YzI3ZjZm'+
        'NzQyNjI4MSZmbHR0PTImaW52dD0yJmZpZD1mNiZmcz1iOkRMTUswMTQxJmZpZWxkcz1mM'+
        'yxmNixmMTIsZjE0LGYyMCZfPQ==',
        'jQuery112407888868459479792_1628259564671')
    a = [ ['hk'+i[0], i[1], i[2], i[3], i[4]] for i in a]
    logger.debug('get hk stocks HSI {}'.format(len(a)))
    return a



if __name__ == "__main__":
    # print(get_cn_stock_list())
    # print(get_price('fuBTC',sdate='20250101'))
    # print(get_price('sz000001', sdate='20240101', edate='20250101'))
    print(get_price('usAMZN.OQ', sdate='20250101', edate='20250101', freq='min'))

