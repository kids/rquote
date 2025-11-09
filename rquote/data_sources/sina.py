# -*- coding: utf-8 -*-
"""
新浪数据源
"""
import json
from typing import Dict, Any, List
from .base import DataSource
from ..utils.http import HTTPClient
from ..exceptions import DataSourceError, ParseError


class SinaDataSource(DataSource):
    """新浪数据源"""
    
    BASE_URL_FUTURE = "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/"
    BASE_URL_TICK = "https://hq.sinajs.cn/?list="
    
    def __init__(self, http_client: HTTPClient = None):
        """
        初始化新浪数据源
        
        Args:
            http_client: HTTP客户端
        """
        self.http_client = http_client or HTTPClient()
    
    def fetch_kline(self, symbol: str, **kwargs) -> Dict[str, Any]:
        """
        从新浪获取K线数据（主要用于期货）
        
        Args:
            symbol: 期货代码（不含fu前缀）
            **kwargs: 其他参数
        """
        freq = kwargs.get('freq', 'day')
        
        if freq in ('min', '1min', 'minute'):
            url = f"{self.BASE_URL_FUTURE}var%20t1nf_{symbol}=/InnerFuturesNewService.getMinLine?symbol={symbol}"
        else:
            url = f"{self.BASE_URL_FUTURE}var%20t1nf_{symbol}=/InnerFuturesNewService.getDailyKLine?symbol={symbol}"
        
        response = self.http_client.get(url)
        if not response:
            raise DataSourceError(f"Failed to fetch from Sina: {symbol}")
        
        try:
            # 解析JavaScript变量赋值格式
            text = response.text
            # 提取JSON部分
            json_start = text.find('[')
            if json_start == -1:
                json_start = text.find('{')
            if json_start == -1:
                raise ParseError("Invalid response format")
            
            data = json.loads(text[json_start:])
            return {'data': data}
        except json.JSONDecodeError as e:
            raise ParseError(f"Parse error: {e}")
        finally:
            # 确保响应对象被关闭，释放SSL连接
            response.close()
    
    def fetch_tick(self, symbols: List[str]) -> Dict[str, Any]:
        """
        获取实时行情
        
        Args:
            symbols: 股票代码列表（美股需要gb_前缀）
        """
        # 美股需要gb_前缀
        if isinstance(symbols, str):
            symbols = [symbols]
        
        tick_symbols = ['gb_' + s.lower() if not s.startswith('gb_') else s for s in symbols]
        url = f"{self.BASE_URL_TICK}{','.join(tick_symbols)}"
        
        response = self.http_client.get(url)
        if not response:
            raise DataSourceError("Failed to fetch tick data from Sina")
        
        try:
            # 解析响应
            lines = response.text.split(';\n')
            result = []
            for line in lines:
                if ',' in line and '=' in line:
                    parts = line.split('=')
                    if len(parts) == 2:
                        data_str = parts[1].strip('"')
                        result.append(data_str.split(','))
            return {'data': result}
        except Exception as e:
            raise ParseError(f"Parse tick error: {e}")
        finally:
            # 确保响应对象被关闭，释放SSL连接
            response.close()

