# -*- coding: utf-8 -*-
"""
技术因子
"""
import pandas as pd
from typing import Union


class BasicFactors:
    """基础技术因子"""
    
    @staticmethod
    def break_rise(d: pd.DataFrame) -> float:
        """
        突破上涨
        
        Args:
            d: 价格数据DataFrame
        
        Returns:
            突破上涨幅度
        """
        if d.open[-1] / d.close[-2] > 1.002 and d.close[-1] > d.open[-1]:
            return round((d.open[-1] - d.close[-2]) / d.close[-2], 2)
        else:
            return 0
    
    @staticmethod
    def min_resist(d: pd.DataFrame) -> float:
        """
        最小阻力
        
        Args:
            d: 价格数据DataFrame
        
        Returns:
            最小阻力值
        """
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
    def vol_extreme(d: pd.DataFrame):
        """
        成交量极值
        
        Args:
            d: 价格数据DataFrame
        
        Returns:
            成交量极值比率
        """
        vol_series = d.vol
        v60max = vol_series.rolling(60).max()
        v60min = vol_series.rolling(60).min()
        # any in last 3days
        for i in range(1, 3):
            if vol_series[-i] > v60max[-i - 1]:
                return round(vol_series[-i] / v60max[-i - 1], 2)
            if vol_series[-i] < v60min[-i - 1]:
                return round(-vol_series[-i] / v60min[-i - 1], 2)
        return 0
    
    @staticmethod
    def bias_rate_over_ma60(d: pd.DataFrame) -> float:
        """
        偏离MA60的比率
        
        Args:
            d: 价格数据DataFrame
        
        Returns:
            偏离比率
        """
        r60 = d.close - d.close.rolling(60).mean()
        if r60[-1] > 0:
            return round(r60[-1] / r60.rolling(60).max()[-1], 2)
        else:
            return round(-r60[-1] / r60.rolling(60).min()[-1], 2)
    
    @staticmethod
    def op_ma(d: pd.DataFrame) -> Union[float, None]:
        """
        MA评分
        
        Args:
            d: 价格数据DataFrame
        
        Returns:
            MA评分
        """
        if len(d) < 22:
            return None
        
        d = d.copy()
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
    def op_cnt(d: pd.DataFrame, cont_min: int = 3) -> int:
        """
        连续上涨天数计数
        
        Args:
            d: 价格数据DataFrame
            cont_min: 最小连续天数
        
        Returns:
            连续上涨天数
        """
        d.index = pd.DatetimeIndex(d.index)
        td = (d.p_change_on_sh.rolling(cont_min).min() > 0).astype(int) * \
            (d.p_change.rolling(cont_min).min() > 0).astype(int)
        ret = 0 if td[-1] <= 0 else td[-1]
        return ret

