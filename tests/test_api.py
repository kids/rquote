# -*- coding: utf-8 -*-
"""
API测试（集成测试）
"""
import unittest
from rquote import get_price, get_cn_stock_list, get_all_industries
from rquote.exceptions import SymbolError, DataSourceError, NetworkError


class TestAPI(unittest.TestCase):
    """API测试"""
    
    def test_get_price_basic(self):
        """测试基本价格获取（需要网络）"""
        try:
            sid, name, df = get_price('sh000001', days=10)
            self.assertIsNotNone(sid)
            self.assertIsNotNone(name)
            self.assertIsNotNone(df)
            if not df.empty:
                self.assertIn('open', df.columns)
                self.assertIn('close', df.columns)
        except (DataSourceError, NetworkError) as e:
            # 网络问题不视为测试失败
            self.skipTest(f"Network issue: {e}")
    
    def test_get_price_with_dates(self):
        """测试带日期的价格获取"""
        try:
            sid, name, df = get_price('sz000001', sdate='2024-01-01', edate='2024-01-10')
            self.assertIsNotNone(df)
        except (DataSourceError, NetworkError) as e:
            self.skipTest(f"Network issue: {e}")
    
    def test_get_cn_stock_list(self):
        """测试A股列表获取"""
        try:
            stocks = get_cn_stock_list(money_min=1e8)
            self.assertIsInstance(stocks, list)
            if stocks:
                self.assertIsInstance(stocks[0], dict)
        except (DataSourceError, NetworkError) as e:
            self.skipTest(f"Network issue: {e}")
    
    def test_get_all_industries(self):
        """测试行业列表获取"""
        try:
            industries = get_all_industries()
            self.assertIsInstance(industries, list)
        except (DataSourceError, NetworkError) as e:
            self.skipTest(f"Network issue: {e}")


if __name__ == '__main__':
    unittest.main()

