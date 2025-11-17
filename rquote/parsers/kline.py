# -*- coding: utf-8 -*-
"""
K线数据解析器
"""
import pandas as pd
from typing import Dict, Any, Tuple, Optional
from ..exceptions import ParseError


class KlineParser:
    """K线数据解析器"""
    
    @staticmethod
    def parse_tencent_kline(data: Dict[str, Any], symbol: str, fq: str = 'qfq') -> Tuple[str, pd.DataFrame]:
        """
        解析腾讯K线数据
        
        Args:
            data: 腾讯API返回的数据
            symbol: 股票代码
        
        Returns:
            (name, DataFrame)
        """
        try:
            symbol_data = data.get('data', {}).get(symbol, {})
            if not symbol_data:
                raise ParseError(f"No data for symbol {symbol}")
            
            # 查找时间键，优先使用与fq参数匹配的键
            # 根据fq参数确定优先级：qfq -> qfqday优先，hfq -> hfqday优先，否则day优先
            if fq == 'qfq':
                time_keys = ['qfqday', 'day', 'hfqday', 'qfqweek', 'week', 'hfqweek',
                            'qfqmonth', 'month', 'hfqmonth']
            elif fq == 'hfq':
                time_keys = ['hfqday', 'day', 'qfqday', 'hfqweek', 'week', 'qfqweek',
                            'hfqmonth', 'month', 'qfqmonth']
            else:
                time_keys = ['day', 'qfqday', 'hfqday', 'week', 'qfqweek', 'hfqweek',
                            'month', 'qfqmonth', 'hfqmonth']
            tk = None
            for tkt in time_keys:
                if tkt in symbol_data:
                    tk = tkt
                    break
            
            if not tk:
                raise ParseError(f"No time key found for {symbol}")
            
            # 提取名称
            name = ''
            if 'qt' in symbol_data and symbol in symbol_data['qt']:
                qt_data = symbol_data['qt'][symbol]
                if len(qt_data) > 1:
                    name = qt_data[1]
            
            # 解析K线数据
            kline_data = symbol_data[tk]
            df = pd.DataFrame(
                [j[:6] for j in kline_data],
                columns=['date', 'open', 'close', 'high', 'low', 'vol']
            ).set_index('date')
            
            # 转换数据类型
            for col in ['open', 'high', 'low', 'close', 'vol']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return name, df
        except Exception as e:
            raise ParseError(f"Failed to parse Tencent kline data: {e}")
    
    @staticmethod
    def parse_sina_future_kline(data: Dict[str, Any], freq: str = 'day') -> pd.DataFrame:
        """
        解析新浪期货K线数据
        
        Args:
            data: 新浪API返回的数据
            freq: 频率
        
        Returns:
            DataFrame
        """
        try:
            raw_data = data.get('data', [])
            if not raw_data:
                raise ParseError("Empty data from Sina")
            
            if freq in ('min', '1min', 'minute'):
                # 分钟数据
                df = pd.DataFrame(raw_data)
                if 'dtime' in df.columns:
                    df = df.set_index('dtime')
                # 转换数值列
                numeric_cols = ['close', 'avg', 'vol', 'hold']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                # 日线数据
                df = pd.DataFrame(
                    raw_data,
                    columns=['date', 'open', 'high', 'low', 'close', 'vol', 'p', 's']
                ).set_index('date')
                # 转换数值列
                for col in ['open', 'high', 'low', 'close', 'vol', 'p', 's']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
        except Exception as e:
            raise ParseError(f"Failed to parse Sina future kline data: {e}")

