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
        # df = self.api.get_info("nonexistent")
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
