# -*- coding: utf-8 -*-
"""
腾讯数据源
"""
import json
from typing import Dict, Any, List
from .base import DataSource
from ..utils.http import HTTPClient
from ..exceptions import DataSourceError, ParseError


class TencentDataSource(DataSource):
    """腾讯数据源"""
    
    BASE_URL = "https://web.ifzq.gtimg.cn/appstock/app/newfqkline/get"
    BASE_URL_HK = "https://web.ifzq.gtimg.cn/appstock/app/hkfqkline/get"
    BASE_URL_US = "https://web.ifzq.gtimg.cn/appstock/app/usfqkline/get"
    
    def __init__(self, http_client: HTTPClient = None):
        """
        初始化腾讯数据源
        
        Args:
            http_client: HTTP客户端，如果为None则创建新实例
        """
        self.http_client = http_client or HTTPClient()
    
    def fetch_kline(self, symbol: str, freq: str = 'day', 
                   sdate: str = '', edate: str = '', 
                   days: int = 320, fq: str = 'qfq') -> Dict[str, Any]:
        """
        从腾讯获取K线数据
        
        Args:
            symbol: 股票代码
            freq: 频率
            sdate: 开始日期
            edate: 结束日期
            days: 天数
            fq: 复权方式
        
        Returns:
            数据字典
        """
        # 根据市场选择URL
        if symbol[:2] in ['sh', 'sz']:
            url = f"{self.BASE_URL}?param={symbol},{freq},{sdate},{edate},{days},{fq}"
        elif symbol[:2] == 'hk':
            url = f"{self.BASE_URL_HK}?param={symbol},{freq},{sdate},{edate},{days},{fq}"
        elif symbol[:2] == 'us':
            url = f"{self.BASE_URL_US}?param={symbol},{freq},{sdate},{edate},{days},{fq}"
        else:
            raise DataSourceError(f"Unsupported symbol format: {symbol}")
        
        response = self.http_client.get(url)
        if not response:
            raise DataSourceError(f"Failed to fetch from Tencent: {symbol}")
        
        # 解析响应，确保响应对象被正确关闭
        try:
            text = response.text
            # 处理不同的响应格式
            if text.startswith('{'):
                # 直接是JSON
                data = json.loads(text)
            elif '=' in text and '{' in text:
                # JavaScript变量赋值格式: var_name={...}
                json_start = text.find('{')
                if json_start == -1:
                    raise ParseError("Invalid response format")
                data = json.loads(text[json_start:])
            else:
                # 尝试直接解析
                json_start = text.find('{')
                if json_start == -1:
                    raise ParseError("Invalid response format")
                data = json.loads(text[json_start:])
            
            # 检查API返回码
            if isinstance(data, dict) and data.get('code') != 0:
                raise DataSourceError(f"API error: {data.get('msg', 'Unknown error')}")
            
            return data
        except json.JSONDecodeError as e:
            raise ParseError(f"Parse error: {e}")
        finally:
            # 确保响应对象被关闭，释放SSL连接
            response.close()
    
    def fetch_tick(self, symbols: List[str]) -> Dict[str, Any]:
        """获取实时行情（暂未实现）"""
        raise NotImplementedError("Tencent tick data not implemented yet")

