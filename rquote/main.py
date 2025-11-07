# -*- coding: utf-8 -*-

import json
import time
import re
import base64
import pandas as pd
from .utils import WebUtils, hget, logger




def get_cn_stock_list(money_min=2e8):
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


def load_js_var_json(url):
    a = hget(url)
    if a:
        a = json.loads(a.text.split('(')[1].split(')')[0])
    return a


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
    sina_future_min = 'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/' + \
            'var%20t1nf_{}=/InnerFuturesNewService.getMinLine?symbol={}'
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
                for col in ['open','high','low','close','vol','amount']:
                    d[col] = pd.to_numeric(d[col], errors='coerce')
                d = d.set_index(['date']).astype(float)
                return i, 'BTC', d
            else:
                ix = i[2:]
                if freq in ('min', '1min', 'minute'):
                    url = sina_future_min.format(ix, ix)
                    # rtext = hget(url).text
                    # d = pd.DataFrame(json.loads(rtext.split(i[2:])[1][2:-2]))
                    d = pd.DataFrame(load_js_var_json(url))
                    d.columns = ['dtime', 'close', 'avg', 'vol', 'hold','last_close','cur_date']
                    for col in ['close','avg','vol','hold']:
                        d[col] = pd.to_numeric(d[col], errors='coerce')
                    d = d.set_index(['dtime'])
                    return i[2:], i[2:], d
                else:
                    # d = pd.DataFrame(json.loads(hget(sina_future_d.format(
                    #         ix, ix)).text.split('(')[1][:-2]))
                    d = pd.DataFrame(load_js_var_json(sina_future_d.format(ix, ix)))
                    d.columns = ['date', 'open', 'high', 'low', 'close', 'vol', 'p', 's']
                    for col in ['open','high','low','close','vol','p','s']:
                        d[col] = pd.to_numeric(d[col], errors='coerce')
            d = d.set_index(['date']).astype(float)
            # d.index = pd.DatetimeIndex(d.index)
            return i, ix, d
        except Exception as e:
            logger.warning('error get price {}, err {}'.format(i[2:-4], e))
            return i, 'None', pd.DataFrame([])

    if i[:2] == 'pt':
        try:
            # URL format: https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get?_var=kline_dayqfq&param=pt01801125,day,,,320,qfq
            url = f'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get?_var=kline_dayqfq&param={i},{freq},{sdate},{edate},{days},{fq}'
            a = hget(url)
            if not a:
                logger.warning('{} hget failed: {}'.format(i, a))
                return i, 'None', pd.DataFrame([])
            
            # Parse JavaScript variable assignment: kline_dayqfq={...}
            response_text = a.text
            # Extract JSON part after the variable name
            json_start = response_text.find('{')
            if json_start == -1:
                logger.warning('{} invalid response format: {}'.format(i, response_text[:100]))
                return i, 'None', pd.DataFrame([])
            
            data = json.loads(response_text[json_start:])
            if data.get('code') != 0:
                logger.warning('{} API returned error: {}'.format(i, data.get('msg', 'Unknown error')))
                return i, 'None', pd.DataFrame([])
            
            # Extract data for this symbol
            symbol_data = data.get('data', {}).get(i, {})
            if not symbol_data:
                logger.warning('{} data empty in response'.format(i))
                return i, 'None', pd.DataFrame([])
            
            # Find the appropriate time key (day, week, month, etc.)
            name = ''
            tk = None
            for tkt in ['day', 'qfqday', 'hfqday', 'week', 'qfqweek', 'hfqweek',
                        'month', 'qfqmonth', 'hfqmonth']:
                if tkt in symbol_data:
                    tk = tkt
                    break
            
            if not tk:
                logger.warning('{} no time key found in data'.format(i))
                return i, 'None', pd.DataFrame([])
            
            # Extract name from qt if available
            if 'qt' in symbol_data and i in symbol_data['qt']:
                name = symbol_data['qt'][i][1] if len(symbol_data['qt'][i]) > 1 else ''
            
            # Parse kline data: each entry is [date, open, close, high, low, vol, {}, ...]
            kline_data = symbol_data[tk]
            b = pd.DataFrame([j[:6] for j in kline_data],
                             columns=['date', 'open', 'close', 'high', 'low', 'vol']
                             ).set_index(['date'])
            for col in ['open', 'high', 'low', 'close', 'vol']:
                b[col] = pd.to_numeric(b[col], errors='coerce')
            
            return i, name, b
        except Exception as e:
            logger.warning('error fetching {}, err: {}'.format(i, e))
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
        nm = a['qt'][i][1]
        b = pd.DataFrame([i.split() for i in a['data']['data']],
                columns=['minute','price','volume']).set_index(['minute'])
        for col in ['price','volume']:
            b[col] = pd.to_numeric(b[col], errors='coerce')
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
                         columns=['date','open','close','high','low','vol']
                         ).set_index(['date'])
        for col in ['open','high','low','close','vol']:
            b[col] = pd.to_numeric(b[col], errors='coerce')
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
    url = f'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/stockinfo/plateNew?code={i}&app=wzq&zdf=1'
    a = hget(url)
    if not a:
        raise HTTPError('Failed to fetch concepts from QQ Finance')
    data = json.loads(a.text)
    if data.get('code') != 0:
        raise HTTPError('API returned error: {}'.format(data.get('msg', 'Unknown error')))
    return data.get('data', {}).get('concept', [])


def get_stock_industry(i) -> []:
    url = f'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/stockinfo/plateNew?code={i}&app=wzq&zdf=1'
    a = hget(url)
    if not a:
        raise HTTPError('Failed to fetch industry from QQ Finance')
    data = json.loads(a.text)
    if data.get('code') != 0:
        raise HTTPError('API returned error: {}'.format(data.get('msg', 'Unknown error')))
    return data.get('data', {}).get('plate', [])


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
        # Format: [code, name, change, amount, price]
        # change: zdf (涨跌幅), amount: zllr (主力净流入), price: zxj (最新价)
        industries = [
            [item['code'], item['name'], item.get('zdf', '0'), 
             item.get('zllr', '0'), item.get('zxj', '0')]
            for item in rank_list
        ]
        logger.debug('get industries {}'.format(len(industries)))
    except Exception as e:
        raise HTTPError.warning('Error parsing industries data: {}'.format(e))
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
        print(f'Error parsing sina industries data: {e}, get {data}')
    # 利用sina_sw2_dict给industries添加sina_sw2_id
    for industry in industries:
        industry.append(sina_sw2_dict.get(industry[1], ''))
    return industries


def get_industry_stocks(node):
    '''
    Return sorted industry item list ordered by latest amount of money,
    item in returned list are [code, name, change, amount, price]
    '''
    url = f'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/'+\
    f'Market_Center.getHQNodeData?page=1&num=40&sort=symbol&asc=1&node={node}&symbol=&_s_r_a=init'
    a = hget(url)
    if not a:
        raise HTTPError('Failed to fetch industry stocks from Sina Finance')
    data = json.loads(a.text)
    return data


if __name__ == "__main__":
    # print(get_cn_stock_list())
    # print(get_price('fuBTC',sdate='20250101'))
    # print(get_price('fuM2601',sdate='20250101', freq='min'))
    # print(get_price('fuM2601',sdate='2025-01-01'))
    # print(get_price('pt02B20001'))
    # print(get_price('sz000001', sdate='20240101', edate='20250101'))
    # print(get_price('usAMZN.OQ', sdate='20250101', edate='20250101', freq='min'))
    print(get_all_industries())
    # print(get_stock_concepts('sz000858'))
    # print(get_stock_industry('sz000858'))
    # print(get_industry_stocks('sw2_480200'))

