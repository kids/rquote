# -*- coding: utf-8 -*-

import re
import os
import time
import json
import random
import logging
import requests
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
hdl = logging.FileHandler('/tmp/rquote.log')
hdl.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(hdl)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class CommonUtils:
    @staticmethod
    def rand_string():
        return ''.join([chr(random.choice(range(97, 123))) for _ in
                        range(random.choice(range(3, 7)))])

    @staticmethod
    def yesterday_of(day):
        '''return 2020-12-31 if day = 2021-01-01'''
        return time.strftime('%Y-%m-%d', time.localtime(time.mktime(
            time.strptime(day, '%Y-%m-%d')) - 24 * 60 * 60))

    @staticmethod
    def sample_dates(year_earliest=2010, year_range=2):
        y = random.randint(year_earliest, 2021 - 2)
        m = str(random.randint(1, 12)).zfill(2)
        d = str(random.randint(1, 28)).zfill(2)
        date_begin = '{}-{}-{}'.format(y, m, d)
        date_end = '{}-{}-{}'.format(y + 2, m, d)
        return date_begin, date_end


class WebUtils:
    @staticmethod
    def ua():
        ua_list = [
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)',
            'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.5 Safari/534.55.3',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/521.61'
        ]
        return random.choice(ua_list)

    @classmethod
    def headers(cls):
        header = {
            'referer': CommonUtils.rand_string(),
            'user-agent': cls.ua()
            }
        return header

    @classmethod
    def reqget(cls, url, headers, method, proxy=None, proxy_type='http'):
        '''
        request.get() wrapper
        '''
        headers['user-agent'] = cls.ua
        try:
            if proxy is not None:
                r = requests.get(
                    url, allow_redirects=True, proxies={
                        proxy_type: proxy})
            else:
                r = requests.get(url, allow_redirects=True)
        except Exception as e:
            logger.error('Fetch url {} err: {}'.format(url, e))
            return None
        if r:
            if method == 'text':
                return r.text
            elif method == 'content':
                return r.content

    @classmethod
    def test_proxy(cls, proxy: str, proxy_type='http'):
        '''
        proxy format 'ip:port'
        test baidu.com for cn
        # test google.com for non-cn (not effective due to DNS hijacking)
        '''
        try:
            r = requests.get(
                'https://baidu.com',
                proxies={
                    proxy_type: proxy},
                timeout=2)
        except Exception as e:
            logger.info(f'test proxy {proxy} negative')
            return 0
        if r.ok:
            logger.info(f'test proxy {proxy} positive')
            return 1


class BasicFactors:

    @staticmethod
    def break_rise(d) -> float:
        if d.open[-1] / d.close[-2] > 1.002 and d.close[-1] > d.open[-1]:
            return round((d.open[-1] - d.close[-2]) / d.close[-2], 2)
        else:
            return 0

    @staticmethod
    def min_resist(d) -> float:
        sup, pre, pcur = 0, 0, d.close[-1]
        for i in d.iterrows():
            p = (i[1].open + i[1].close) / 2
            if p > pcur:
                pre += i[1].vol
            if p < pcur:
                sup += i[1].vol
        minres = (sup - pre) / (sup + pre)
        if abs(minres - 1) < .01 and d.close[-2] < max(d.close[:-2]):
            minres += .2
        minres = round(minres, 2)
        return minres

    @staticmethod
    def ma_divergence(d) -> float:
        '''moving averages diverging rate'''
        d = d.close
        dlog5 = np.log(d.rolling(5).mean())
        dlog10 = np.log(d.rolling(10).mean())
        dlog20 = np.log(d.rolling(20).mean())
        dlog60 = np.log(d.rolling(60).mean())
        logmas = pd.DataFrame({'dl5': dlog5, 'dl10': dlog10, 'dl20': dlog20})
        mstd20 = logmas.std(1).rolling(5).mean()
        logmas = pd.DataFrame({'dl5': dlog5, 'dl10': dlog10,
                              'dl20': dlog20, 'dl60': dlog60})
        # mstd60=logmas.std(1).rolling(5).mean()
        mv = logmas.max(1) + logmas.min(1) - \
            logmas.max(1).shift(1) - logmas.min(1).shift(1)
        return round(mv[-1] / mstd20[-2], 2)

    @staticmethod
    def vol_extreme(d):
        d = d.vol
        v60max = d.rolling(60).max()
        v60min = d.rolling(60).min()
        # any in last 3days
        for i in range(1, 3):
            if d[-i] > v60max[-i - 1]:
                return round(d[-i] / v60max[-i - 1], 2)
            if d[-i] < v60min[-i - 1]:
                return round(-d[-i] / v60min[-i - 1], 2)
            else:
                return 0

    @staticmethod
    def bias_rate_over_ma60(d):
        r60 = d.close - d.close.rolling(60).mean()
        if r60[-1] > 0:
            return round(r60[-1] / r60.rolling(60).max()[-1], 2)
        else:
            return round(-r60[-1] / r60.rolling(60).min()[-1], 2)

    @staticmethod
    def op_ma(d) -> float:
        ''' op: ma score'''
        if len(d) < 22:
            return
        d['mv5'] = d.close.rolling(5).mean()
        d['mv10'] = d.close.rolling(10).mean()
        d['mv20'] = d.close.rolling(20).mean()
        d['mv60'] = d.close.rolling(60).mean()

        def ma20(d):
            ret = 0
            # .2 for over ma60
            if d.close[-1] > d.mv60[-1]:
                ret += 0.2
            # .2 for all upwards ma's
            if (d.mv5[-1] > d.mv5[-2] and d.mv10[-1] >
                    d.mv10[-2] and d.mv20[-1] > d.mv20[-2]):
                ret += 0.2
                for j in range(1, 3):
                    if not (d.close[-j] > d.mv5[-j] and d.close[-j]
                            > d.mv10[-j] and d.close[-j] > d.mv20[-j]):
                        return ret
                for j in range(3, 5):
                    if (d.close[-j] > d.mv5[-j] and d.close[-j] >
                            d.mv10[-j] and d.close[-j] > d.mv20[-j]):
                        return ret
                # .2 for just rush over ma's (fresh score)
                ret += 0.2
            return ret
        return ma20(d)

    @staticmethod
    def op_cnt(d, cont_min=3) -> (int):
        ''' op: count continous bulling days over index'''
        d.index = pd.DatetimeIndex(d.index)
        td = (d.p_change_on_sh.rolling(cont_min).min() > 0).astype(int) * \
            (d.p_change.rolling(cont_min).min() > 0).astype(int)
        ret = 0 if td[-1] <= 0 else td[-1]
        # is_first_day = True if td[-2] <= 0 else False
        return ret


class DataFormatter:
    @staticmethod
    def s_join_sh_close_change(d, dsh=None, sdate='', edate=''):
        '''
        join 2 DataFrames, used in single stock with sh index;
        keeping columns of 'close' and 'change'
        '''
        dsh['p_change'] = (dsh.close - dsh.close.shift(1)) * 100 / dsh.close.shift(1)
        d['p_change'] = (d.close - d.close.shift(1)) * 100 / d.close.shift(1)
        if not len(d) or dsh is None:
            return d
        d = d.join(dsh[['open', 'close', 'p_change']], rsuffix='_sh').sort_index()
        d['p_change_on_sh'] = d['p_change'] - d['p_change_sh']
        return d

    @staticmethod
    def sort_keys_by_cossim(df):
        '''
        Input:
            DataFrame with index column as keys
        sort by dataframe values cosine similarity
        '''
        from sklearn.metrics.pairwise import cosine_similarity
        keys = list(df.index)
        cs = cosine_similarity(df)

        to_sort_keys = [i for i in keys[1:]]
        sorted_keys = [keys[0]]
        
        cid = 0
        for i in range(len(keys)-1):
            for j in np.argsort(cs[cid])[::-1]:
                if keys[j] not in sorted_keys:
                    sorted_keys.append(keys[j])
                    to_sort_keys.remove(keys[j])
                    cid = j
                    break
        return sorted_keys

    @staticmethod
    def join_stock_concepts(nhe, nhb, dc):
        '''
        merge stock df with concept df with summerized result
        nhe: stock df with sid, sname
        nhb: concept df with sid, sname
        dc: dict of concept {concept: [stock]}
        TODO abstract it
        '''
        from collections import Counter
        nhb.index = nhb.sid
        nhe.index = nhe.sname
        nhe['conc'] = ''
        nhb['list'] = [''] * len(nhb)
        stks = nhe.sname.tolist()  # candidates
        stkinconc = []
        for i, j in nhb.sort_values('imf2', ascending=False)[
                ['sid', 'sname']].iterrows():
            ti = []
            n = dc.get(j.sid)
            for s in n:
                if s in stks:
                    stkinconc.append(s)
                    ti.append(s)
                    nhe.loc[s, 'conc'] = nhe.loc[s, 'conc'] + ',' + j.sname
            if ti:
                nhb.loc[j.sid,
                        'list'] = '{}/{}:{}'.format(len(ti),
                                                    len(n),
                                                    ';'.join(ti))
        stkr = Counter(stkinconc).most_common(200)
        stkr = pd.DataFrame(stkr, columns=['sname', 'concs'])
        nhe.index = nhe.sid
        nhe = nhe.merge(
            stkr,
            on='sname',
            how='left',
            suffixes=(
                '',
                '_')).fillna('')
        return nhe, nhb



class reqget:
    '''
    class version request.get wrapper
    '''
    def __init__(self, url, *args, **kwargs):
        self.url = url
        try:
            self.r = requests.get(
                self.url, allow_redirects=True, *args, **kwargs)
            self.text = self.r.text
            self.content = self.r.content
        except BaseException:
            logger.error(f'fetch {self.url} err')
            self.text = ''
            self.content = b''

