# -*- coding: utf-8 -*-

import time
import random
import logging
import httpx
import pandas as pd
import uuid

def setup_logger():
    logger = logging.getLogger('rquote')
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler('/tmp/rquote.log')
        
        formatter = logging.Formatter('%(asctime)-15s:%(lineno)s %(message)s')
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(logging.StreamHandler())
    
    return logger

logger = setup_logger()


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
            'referer': str(uuid.uuid4()),
            'user-agent': cls.ua()
            }
        return header

    @classmethod
    def http_get(cls, url, headers, method, proxy=None):
        '''
        request.get() wrapper
        '''
        headers['user-agent'] = cls.ua
        try:
            r = httpx.get(url, allow_redirects=True)
        except Exception as e:
            logger.error('Fetch url {} err: {}'.format(url, e))
            return None
        if r:
            if method == 'text':
                return r.text
            elif method == 'content':
                return r.content

    @classmethod
    def test_proxy(cls, proxy: str):
        '''
        proxy format 'ip:port'
        test baidu.com for cn
        # test google.com for non-cn (not effective due to DNS hijacking)
        '''
        try:
            with httpx.Client(proxies=proxy) as client:
                r = client.get('https://baidu.com', timeout=2)
                if r.ok:
                    return 1
                else:
                    return 0
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


class hget:
    '''
    class version request.get wrapper
    '''
    def __init__(self, url, *args, **kwargs):
        self.url = url
        try:
            r = httpx.get(
                self.url, follow_redirects=True, headers=WebUtils.headers(),
                *args, **kwargs)
            self.text = r.text
            self.content = r.content
        except Exception as e:
            logger.error(f'fetch {self.url} err: {e}')
            self.text = ''
            self.content = b''

