# -*- coding: utf-8 -*-

import os
import requests
import json
import time
import random
import re
import sys
import base64
import logging
import pandas as pd
from .utils import WebUtils, reqget
# logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(filename='/tmp/rproxy.log',
                    format='%(asctime)-15s:%(lineno)s %(message)s',
                    level=logging.INFO)


def make_tgts(mkts=['ch', 'hk', 'us', 'fund', 'future'], money_min=2e8) -> []:
    cands = []


def get_cn_stocks_by_amount(money_min=2e8):
    '''
    Return sorted stock list ordered by latest amount of money, cut at `money_min`
    item in returned list are [code, name, change, amount, mktcap]
    '''
    a = reqget(
        base64.b64decode('aHR0cDovLzM4LnB1c2gyLmVhc3Rtb25leS5jb20vYXBpL3F0L2Ns'+
            'aXN0L2dldD9jYj1qUXVlcnkxMTI0MDk0NTg3NjE4NDQzNzQ4MDFfMTYyNzI4ODQ4O'+
            'Tk2MSZwbj0xJnB6PTEwMDAwJnBvPTEmbnA9MSZ1dD1iZDFkOWRkYjA0MDg5NzAwY2'+
            'Y5YzI3ZjZmNzQyNjI4MSZmbHR0PTImaW52dD0yJmZpZD1mNiZmcz1tOjArdDo2LG0'+
            '6MCt0OjgwLG06MSt0OjIsbToxK3Q6MjMmZmllbGRzPWYxMixmMTQsZjMsZjYsZjIxJl89'
            ).decode()+str(int(time.time()*1e3))
    )
    if a:
        a = json.loads(a.text.split(
            'jQuery112409458761844374801_1627288489961(')[1][:-2])

    # cdir = os.path.dirname(__file__)
    # with open(os.path.join(cdir, 'ranka'), 'wb') as f:
    #     f.write(a.content)
    # a = pd.read_excel(os.path.join(cdir, 'ranka'), header=1)
    # os.remove(os.path.join(cdir, 'ranka'))
    # a.columns = ['code', 'name', 'close', 'p_change', 'change', '_', '_', '_',
    #              'money', 'open', 'yest_close', 'high', 'low']
    # a = a[a.money > money_min]
    a = [ ['sh'+i['f12'] if i['f12'][0]=='6' else 'sz'+i['f12'],
         i['f14'], i['f3'], i['f6'], i['f21']] for i in a['data']['diff']
        if i['f6']!='-' and float(i['f6']) > money_min]
    #cands=[(i.code,i.name) for i in a[['code','name']].itertuples()]
    return a


def get_hk_stocks_hotest80():
    a = reqget(
        'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php' +
        '/Market_Center.getHKStockData?page=1&num=80&sort=amount&asc=0&node=' +
        'qbgg_hk&_s_r_a=sort').text
    if a:
        a = [['hk'+i['symbol'], i['name'], i['changepercent'], i['amount'],
            i['market_value']] for i in json.loads(a)]
    return a


def get_us_stocks_hotest30():
    a = reqget(
        'https://xueqiu.com/service/v5/stock/screener/quote/list?page=1&' +
        'size=30&order=desc&orderby=amount&order_by=amount&market=US&type=us&_=' +
        str(int(time.time() * 1000)), headers=WebUtils.headers()).text
    if a:
        uslist = json.loads(a)['data']['list']
        # Warning: symbol not fitted
        uscands = ['us' + i['symbol'] + '.OQ' for i in uslist]
    return uscands


def get_cn_fund_hotest200():
    a = reqget(base64.b64decode('aHR0cDovL3ZpcC5zdG9jay5maW5hbmNlLnNpbmEuY29' +
                                'tLmNuL3F1b3Rlc19zZXJ2aWNlL2FwaS9qc29ucC5waHAvSU8uWFNSVjIuQ2FsbGJh' +
                                'Y2tMaXN0WydrMldhekswNk5Rd2xoeVh2J10vTWFya2V0X0NlbnRlci5nZXRIUU5vZ' +
                                'GVEYXRhU2ltcGxlP3BhZ2U9MSZudW09MTIwJnNvcnQ9YW1vdW50JmFzYz0wJm5vZG' +
                                'U9ZXRmX2hxX2Z1bmQmJTVCb2JqZWN0JTIwSFRNTERpdkVsZW1lbnQlNUQ9eG00aTA='
                                ).decode()).text
    if a:
        fundcands = [i['symbol']
                     for i in json.loads(a.split('k2WazK06NQwlhyXv')[1][3:-2])]
    return fundcands


def get_cn_future():
    a = reqget('https://finance.sina.com.cn/futuremarket/').text
    if a:
        futurelist_active = [
            'fu' + i for i in re.findall(r'quotes/(.*?\d+).shtml', a)]
    return futurelist_active


def get_price(i, sdate='', edate='', freq='day', days=320, fq='hfq',
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
        logging.debug('loading price from dd {}'.format(i))
        return i, n, d
    logging.debug('fetching price of {}'.format(i))
    qtimg_stock = base64.b64decode('aHR0cDovL3dlYi5pZnpxLmd0aW1nLmNuL2FwcHN0b2' +
                                   'NrL2FwcC9uZXdmcWtsaW5lL2dldD9wYXJhbT17fSx7' +
                                   'fSx7fSx7fSx7fSxoZnE=').decode('utf-8')
    qtimg_stock_hk = base64.b64decode('aHR0cDovL3dlYi5pZnpxLmd0aW1nLmNuL2FwcHN' +
                                      '0b2NrL2FwcC9oa2Zxa2xpbmUvZ2V0P3BhcmFtPX' +
                                      't9LHt9LHt9LHt9LHt9LGhmcQ==').decode('utf-8')
    qtimg_stock_us = base64.b64decode('aHR0cDovL3dlYi5pZnpxLmd0aW1nLmNuL2FwcHN' +
                                      '0b2NrL2FwcC91c2Zxa2xpbmUvZ2V0P3BhcmFtPX' +
                                      't9LHt9LHt9LHt9LHt9LGhmcQ==').decode('utf-8')
    sina_future_d = base64.b64decode('aHR0cHM6Ly9zdG9jazIuZmluYW5jZS5zaW5hLmNv' +
                                     'bS5jbi9mdXR1cmVzL2FwaS9qc29ucC5waHAvdmFy' +
                                     'JTIwX3t9e309L0lubmVyRnV0dXJlc05ld1NlcnZp' +
                                     'Y2UuZ2V0RGFpbHlLTGluZT9zeW1ib2w9e30mXz17fQ==').decode()
    # sina_future_d.format('FB0','2020_11_14','FB0','2020_11_14')

    if i[:2] == 'BK':
        try:
            a = reqget(base64.b64decode('aHR0cDovL3B1c2gyaGlzLmVhc3' +
                                                   'Rtb25leS5jb20vYXBpL3F0L3N0' +
                                                   'b2NrL2tsaW5lL2dldD9jYj1qUX' +
                                                   'VlcnkxMTI0MDIyNTY2NDQ1ODcz' +
                                                   'NzY2OTcyXzE2MTc4NjQ1NjgxMz' +
                                                   'Emc2VjaWQ9OTAu').decode() + i +
                                  '&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5' +
                                  '&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58' +
                                  '&klt=101&fqt=0&beg=19900101&end=20220101&_=1',
                                  headers=WebUtils.headers())
            if not a:
                logging.warning('{} reqget failed: {}'.format(i, a))
                return i, 'None', pd.DataFrame([])
            a = json.loads(a.text.split(
                'jQuery1124022566445873766972_1617864568131(')[1][:-2])
            if not a['data']:
                logging.warning('{} data empty: {}'.format(i, a))
                return i, 'None', pd.DataFrame([])
            name = a['data']['name']
            d = pd.DataFrame([i.split(',') for i in a['data']['klines']], columns=[
                             'date', 'open', 'close', 'high', 'low', 'vol', 'money', 'p'])
            d = d.set_index(['date']).astype(float)
            d.index = pd.DatetimeIndex(d.index)
            return i, name, d
        except Exception as e:
            logging.warning('error fetching {}, err: {}'.format(i, e))
            return i, 'None', pd.DataFrame([])

    if i[:2] == 'fu':
        try:
            d = pd.DataFrame(json.loads(reqget(sina_future_d.format(
                i[2:-4] + '0', '', i[2:-4] + '0', '')).text.split('(')[1][:-2]))
            d.columns = ['date', 'open', 'close', 'high', 'low', 'vol', 'p']
            d = d.set_index(['date']).astype(float)
            d.index = pd.DatetimeIndex(d.index)
            return i[2:-4], '', d
        except Exception as e:
            logging.warning('error get price {}, err {}'.format(i[2:-4], e))
            return i, 'None', pd.DataFrame([])

    if i[0] in ['0', '1', '3', '5', '6']:
        i = 'sh'+i if i[0] in ['5', '6'] else 'sz'+i
    if i[:2] in ['sh', 'sz']:
        url = qtimg_stock.format(i, freq, sdate, edate, days, fq)
    elif i[:2] == 'hk':
        url = qtimg_stock_hk.format(i, freq, sdate, edate, days, fq)
    elif i[:2] == 'us':
        url = qtimg_stock_us.format(i, freq, sdate, edate, days, fq)
    else:
        raise ValueError('target market not supported')
    a = reqget(url)
    #a = json.loads(a.text.replace('kline_dayqfq=', ''))['data'][i]
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
        logging.warning('error fetching {}, err: {}'.format(i, e))
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


def get_stock_concepts(i) -> []:
    '''
    Return concept list of a stock, from eastmoney
    '''
    f10url = base64.b64decode('aHR0cDovL2YxMC5lYXN0bW9uZXkuY29tLy9Db3JlQ29uY2V' +
                              'wdGlvbi9Db3JlQ29uY2VwdGlvbkFqYXg/Y29kZT0=').decode()
    #drop_cons = ['融资融券', '创业板综', '深股通', '沪股通', '深成500', '长江三角']
    #drop_tails = ['板块', '概念', '0_', '成份', '重仓']
    url = f10url + i
    try:
        concepts = json.loads(reqget(url).text)[
            'hxtc'][0]['ydnr'].split()
    except Exception as e:
        logging.error(str(e))
        concepts = ['']
    #concepts = [i for i in concepts if i not in drop_cons]
    #concepts = [i for i in concepts if i[-2:] not in drop_tails]
    #concepts = [i for i in concepts if '股' not in i]
    return concepts


def get_concept_stks(bkid, dc=None):
    '''
    Return stocks of input bkid, e.g. BK0420, BK0900
    dc : dictionary of concepts, local cache with get/put
    '''
    if dc is not None:
        a = dc.get(bkid)
        if a:
            return a
    bkid = bkid if isinstance(bkid, str) else 'BK' + str(bkid).zfill(4)
    a = reqget(
        base64.b64decode('aHR0cDovL3B1c2gyLmVhc3Rtb25leS5jb20vYXBpL3F0L2NsaXN0' +
                         'L2dldD9jYj1qUXVlcnkxMTIzMDQwNTcwNTM4NTY5NDcwMTA1XzE2MTgwNDc5OTA2O' +
                         'TAmZmlkPWY2MiZwbz0xJnB6PTUwMCZwbj0xJm5wPTEmZmx0dD0yJmludnQ9MiZmcz' +
                         '1iJTNB').decode() +
        bkid +
        '&fields=f12%2Cf14').text
    a = json.loads(
        a.split('jQuery1123040570538569470105_1618047990690(')[1][:-2])['data']['diff']
    logging.debug('get fresh conc {}'.format(bkid))
    a = [ ['sh'+i['f12'] if i['f12'][0]=='6' else 'sz'+i['f12'],
         i['f14'], i['f3'], i['f6'], i['f21']] for i in a]
    return a


def east_list_fmt(burl, api_name):
    '''
    Return list of list:
        [sid, name, rise, amount, mkt]
    '''
    a = reqget(base64.b64decode(burl).decode() +
        str(int(time.time()*1e3)))
    if a:
        a = a.text
    else:
        return
    a = json.loads(a.split(api_name+'(')[1][:-2])['data']['diff']
    a = [ [i['f12'],i['f14'], i['f3'], i['f6'], i['f20']] for i in a]
    return a


def get_all_industries():
    a = east_list_fmt('aHR0cHM6Ly84Ny5wdXNoMi5lYXN0bW9uZXkuY29tL2FwaS9xdC9jbGl'+
        'zdC9nZXQ/Y2I9alF1ZXJ5MTEyNDAzNzExNzU2NTU3MTk3MTM0NV8xNjI3MDQ3MTg4NTk5'+
        'JnBuPTEmcHo9MTAwJnBvPTEmbnA9MSZ1dD1iZDFkOWRkYjA0MDg5NzAwY2Y5YzI3ZjZmN'+
        'zQyNjI4MSZmbHR0PTImaW52dD0yJmZpZD1mMyZmcz1tOjkwK3Q6MitmOiE1MCZmaWVsZH'+
        'M9ZjMsZjYsZjEyLGYxNCxmMjAsZjEwNCxmMTA1Jl89',
        'jQuery1124037117565571971345_1627047188599')
    logging.debug('get industries {}'.format(len(a)))
    return a


def get_all_concepts():
    a = east_list_fmt('aHR0cHM6Ly8yMi5wdXNoMi5lYXN0bW9uZXkuY29tL2FwaS9xdC9jbGl'+
        'zdC9nZXQ/Y2I9alF1ZXJ5MTEyNDA3MzI5ODQxOTMwNzY4OTc5XzE2MjcxMDk0NjA2MzMm'+
        'cG49MSZwej00MDAmcG89MSZucD0xJnV0PWJkMWQ5ZGRiMDQwODk3MDBjZjljMjdmNmY3N'+
        'DI2MjgxJmZsdHQ9MiZpbnZ0PTImZmlkPWYzJmZzPW06OTArdDozK2Y6ITUwJmZpZWxkcz'+
        '1mMyxmNixmMTIsZjE0LGYyMCxmMTA0LGYxMDUmXz0='
        ,'jQuery112407329841930768979_1627109460633')
    logging.debug('get concepts {}'.format(len(a)))
    return a


def get_bk_stocks(bkid):
    url = base64.b64decode('aHR0cDovLzgyLnB1c2gyLmVhc3Rtb25leS5jb20vYXBpL3F0L2'+
        'NsaXN0L2dldD9jYj1qUXVlcnkxMTI0MDQ4Njk5NjMwMDk1MTM3NzE0XzE2Mjc0Nzc0OTU'+
        'wNjQmcG49MSZwej0yMDAwJnBvPTAmbnA9MSZ1dD1iZDFkOWRkYjA0MDg5NzAwY2Y5YzI3'+
        'ZjZmNzQyNjI4MSZmbHR0PTImaW52dD0yJmZpZD1mNiZmcz1iOg==').decode()+ \
        bkid + base64.b64decode('K2Y6ITUwJmZpZWxkcz1mMyxmNixmMTIsZjE0LGYyMCZfPQ==').decode()
    a = east_list_fmt(base64.b64encode(bytes(url, encoding='utf-8')),
        'jQuery1124048699630095137714_1627477495064')
    return a
