import os
import sys # add .. to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rquote

def test_get_price():
    a = rquote.get_price('fuBTC')
    print(a)

def test_get_cn_stock_list():
    a = rquote.get_cn_stock_list()
    print('cn_stock_list', a)

def test_get_hk_stocks_500():
    a = rquote.get_hk_stocks_500()
    print('hk_stocks_500', a)


if __name__ == '__main__':
    # test_get_cn_stock_list()
    test_get_price()
