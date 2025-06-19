import os
import sys # add .. to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rquote

def test_get_price():
    a = rquote.get_price('usIXIC')
    print(a)

def test_get_cn_stock_list():
    a = rquote.get_cn_stock_list()
    print(a)

def test_get_hk_stocks_hsi():
    a = rquote.get_hk_stocks_hsi()
    print(a)

def test_get_hk_stocks_ggt():
    a = rquote.get_hk_stocks_ggt()
    print(a)

if __name__ == '__main__':
    test_get_cn_stock_list()
    