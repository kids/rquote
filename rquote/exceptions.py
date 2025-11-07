# -*- coding: utf-8 -*-
"""
异常定义模块
"""


class RQuoteException(Exception):
    """基础异常类"""
    pass


class DataSourceError(RQuoteException):
    """数据源错误"""
    pass


class ParseError(RQuoteException):
    """解析错误"""
    pass


class SymbolError(RQuoteException):
    """股票代码错误"""
    pass


class NetworkError(RQuoteException):
    """网络错误"""
    pass


class CacheError(RQuoteException):
    """缓存错误"""
    pass


class HTTPError(RQuoteException):
    """HTTP错误（向后兼容）"""
    pass

