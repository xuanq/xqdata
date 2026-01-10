import pandas as pd
import pytest

from xqdata.dataapi import get_dataapi


class TestMockDataApi:
    """测试Mock数据API的功能"""

    def setup_method(self):
        """每个测试方法执行前的准备"""
        self.api = get_dataapi("mock")

    def test_get_info_without_schema(self):
        """测试未设置schema时的行为"""
        # 尝试获取未设置schema的信息类型
        # 应该返回空的DataFrame并打印警告
        # 检查是否打印了警告
        with pytest.warns(
            UserWarning,
            match="No mock schema set for type 'nonexistent'. Returning empty DataFrame.",
        ):
            df = self.api.get_info("nonexistent")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_set_mock_info_and_get_info_stock(self):
        """测试设置股票信息schema并获取数据"""
        # 设置股票信息的schema
        stock_schema = {
            "code": "str",
            "listed_date": "datetime",
            "name": "str",
            "market_value": "float64",
            "industry": "str",
        }
        self.api.set_mock_info("stock", stock_schema)

        # 获取股票信息
        df_stock = self.api.get_info("stock")

        # 验证返回的是DataFrame且长度在30-100之间
        assert isinstance(df_stock, pd.DataFrame)
        assert 30 <= len(df_stock) <= 100
        assert list(df_stock.columns) == list(stock_schema.keys())

        # 验证数据类型
        assert df_stock["code"].dtype == "object"
        assert df_stock["listed_date"].dtype == "datetime64[ns]"
        assert df_stock["market_value"].dtype == "float64"

    def test_multiple_data_types(self):
        """测试多种数据类型"""
        # 设置测试数据的schema
        test_schema = {
            "id": "int64",
            "value": "float64",
            "active": "bool",
            "category": "str",
            "created_at": "datetime",
        }
        self.api.set_mock_info("test_data", test_schema)

        df_test = self.api.get_info("test_data")

        # 验证返回的是DataFrame且长度在30-100之间
        assert isinstance(df_test, pd.DataFrame)
        assert 30 <= len(df_test) <= 100
        assert list(df_test.columns) == list(test_schema.keys())

        # 验证数据类型
        assert df_test["id"].dtype == "int64"
        assert df_test["value"].dtype == "float64"
        assert df_test["active"].dtype == "bool"
        assert df_test["category"].dtype == "object"
        assert df_test["created_at"].dtype == "datetime64[ns]"

    def test_provided_index(self):
        index = pd.date_range("20250101", "20251231")
        test_schema = {"is_tradeday": "bool"}
        self.api.set_mock_info("test_data", test_schema, index)
        df_test = self.api.get_info("test_data")
        assert isinstance(df_test, pd.DataFrame)
        assert len(df_test) == 365
        assert df_test["is_tradeday"].dtype == "bool"
        assert isinstance(df_test.index, pd.DatetimeIndex)

    def test_get_factor_default(self):
        """测试默认的get_factor行为"""
        # 测试单个因子
        df = self.api.get_factor("pe_ratio", "000001.XSHE")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        # 检查索引层级
        assert df.index.names == ["datetime", "code"]
        assert df.columns == ["pe_ratio"]

    def test_get_factor_multiple_factors_panel_true(self):
        """测试多个因子且panel=True"""
        factors = ["pe_ratio", "pb_ratio", "ps_ratio"]
        codes = ["000001.XSHE", "000002.XSHE"]

        df = self.api.get_factor(factors, codes, panel=True)
        # 检查索引层级
        assert len(df.index.names) == 2  # datetime, code
        # 检查列是否为因子名称
        assert set(df.columns) == set(factors)

    def test_get_factor_multiple_factors_panel_false(self):
        """测试多个因子且panel=False"""
        factors = ["pe_ratio", "pb_ratio"]
        codes = ["000001.XSHE", "000002.XSHE"]

        df = self.api.get_factor(factors, codes, panel=False)
        # 检查索引层级
        assert len(df.index.names) == 2  # datetime, code
        assert df.columns.to_list() == ["attribute", "value"]

    def test_get_factor_with_time_range(self):
        """测试带时间范围的get_factor"""
        factors = ["pe_ratio"]
        codes = ["000001.XSHE"]

        start_time = "2024-01-01"
        end_time = "2024-01-31"

        df = self.api.get_factor(
            factors, codes, start_time=start_time, end_time=end_time
        )
        assert df.index.get_level_values("datetime").min() >= pd.to_datetime(start_time)
        assert df.index.get_level_values("datetime").max() <= pd.to_datetime(end_time)

    def test_get_factor_with_frequency(self):
        """测试不同频率的get_factor"""
        factors = ["pe_ratio"]
        codes = ["000001.XSHE"]
        start_time = "2024-01-01"
        end_time = "2024-06-30"
        # 日频数据
        df_daily = self.api.get_factor(
            factors, codes, start_time=start_time, end_time=end_time, frequency="B"
        )
        assert len(df_daily) == 130  # 2024年1月至6月的工作日数量130天

        # 周频数据
        df_weekly = self.api.get_factor(
            factors, codes, start_time=start_time, end_time=end_time, frequency="W"
        )
        assert len(df_weekly) == 26  # 26周

        # 月频数据
        df_monthly = self.api.get_factor(
            factors, codes, start_time=start_time, end_time=end_time, frequency="ME"
        )
        assert len(df_monthly) == 6  # 6个月

    def test_get_dualkey_factor_default(self):
        """测试默认的get_dualkey_factor行为"""
        # 测试单个因子
        df = self.api.get_dualkey_factor("pe_ratio", "000001.XSHE", "market")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        # 检查索引层级
        assert df.index.names == ["datetime", "code", "object"]
        assert df.columns == ["pe_ratio"]

    def test_get_dualkey_factor_multiple_factors_panel_true(self):
        """测试多个因子且panel=True"""
        factors = ["pe_ratio", "pb_ratio", "ps_ratio"]
        codes = ["000001.XSHE", "000002.XSHE"]
        objects = ["market", "industry"]

        df = self.api.get_dualkey_factor(factors, codes, objects, panel=True)
        # 检查索引层级
        assert len(df.index.names) == 3  # datetime, code, object
        # 检查列是否为因子名称
        assert set(df.columns) == set(factors)

    def test_get_dualkey_factor_multiple_factors_panel_false(self):
        """测试多个因子且panel=False"""
        factors = ["pe_ratio", "pb_ratio"]
        codes = ["000001.XSHE", "000002.XSHE"]
        objects = ["market", "industry"]

        df = self.api.get_dualkey_factor(factors, codes, objects, panel=False)
        # 检查索引层级
        assert len(df.index.names) == 3  # datetime, code, object
        assert df.columns.to_list() == ["attribute", "value"]

    def test_get_dualkey_factor_with_time_range(self):
        """测试带时间范围的get_dualkey_factor"""
        factors = ["pe_ratio"]
        codes = ["000001.XSHE"]
        objects = ["market"]

        start_time = "2024-01-01"
        end_time = "2024-01-31"

        df = self.api.get_dualkey_factor(
            factors, codes, objects, start_time=start_time, end_time=end_time
        )
        assert df.index.get_level_values("datetime").min() >= pd.to_datetime(start_time)
        assert df.index.get_level_values("datetime").max() <= pd.to_datetime(end_time)

    def test_get_dualkey_factor_with_frequency(self):
        """测试不同频率的get_dualkey_factor"""
        factors = ["pe_ratio"]
        codes = ["000001.XSHE"]
        objects = ["market"]
        start_time = "2024-01-01"
        end_time = "2024-06-30"
        # 日频数据
        df_daily = self.api.get_dualkey_factor(
            factors,
            codes,
            objects,
            start_time=start_time,
            end_time=end_time,
            frequency="B",
        )
        assert len(df_daily) == 130  # 2024年1月至6月的工作日数量130天

        # 周频数据
        df_weekly = self.api.get_dualkey_factor(
            factors,
            codes,
            objects,
            start_time=start_time,
            end_time=end_time,
            frequency="W",
        )
        assert len(df_weekly) == 26  # 26周

        # 月频数据
        df_monthly = self.api.get_dualkey_factor(
            factors,
            codes,
            objects,
            start_time=start_time,
            end_time=end_time,
            frequency="ME",
        )
        assert len(df_monthly) == 6  # 6个月


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
