# -*- coding: utf-8 -*-

import requests
import re
import os
import time
import logging
import random
import json

logger = logging.getLogger(__name__)
hdl = logging.FileHandler('/tmp/rproxy.log')
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
        headers = {
            'referer': CommonUtils.rand_string(),
            'user-agent': cls.ua()}

    @classmethod
    def reqget(cls, url, headers, method, proxy=None):
        '''
        request.get() wrapper
        '''
        headers['user-agent'] = cls.ua
        try:
            if proxy is not None:
                r = requests.get(
                    url, allow_redirects=True, proxies={
                        'http': proxy})
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
    def test_proxy(cls, proxy: str):
        '''
        proxy format 'ip:port'
        test baidu.com for cn
        test google.com for non-cn
        '''
        try:
            r = requests.get(
                'https://baidu.com',
                proxies={
                    'http': proxy},
                timeout=3)
        except BaseException:
            return
        if r.ok:
            flag = 'cn'
            try:
                r = requests.get(
                    'https://google.com',
                    proxies={
                        'http': proxy},
                    timeout=3)
            except BaseException:
                return
            if r.ok:
                flag = 'ncn'
            return {proxy: flag}


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
