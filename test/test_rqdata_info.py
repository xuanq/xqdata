import os

import pandas as pd
import pytest

from xqdata.dataapi import get_dataapi


class TestRQDataApi:
    """测试RQData API的功能"""

    @classmethod
    def setup_class(cls):
        """整个测试类执行前的准备，只执行一次"""
        cls.api = get_dataapi("rq")
        try:
            # 尝试进行身份验证，只执行一次
            cls.api.auth("license", os.getenv("RQ_LICENSE", ""))
        except Exception:
            # 如果无法进行身份验证，跳过整个测试类
            pytest.skip("无法连接到RQData API，请检查网络连接和认证信息")

    def setup_method(self):
        """每个测试方法执行前的准备"""
        # 确保api实例可用
        pass

    def test_get_info_stock(self):
        """测试获取股票信息"""
        # 获取股票信息
        df_stock = self.api.get_info("stock")

        # 验证返回的是DataFrame或者是None
        assert isinstance(df_stock, pd.DataFrame)

        # 如果返回了数据，验证包含基本列
        if not df_stock.empty:
            expected_columns = [
                "code",
                "symbol",
                "type",
                "listed_date",
                "de_listed_date",
            ]
            assert all(col in df_stock.columns for col in expected_columns)

    def test_get_info_fund(self):
        """测试获取基金信息"""
        # 获取基金信息
        df_fund = self.api.get_info("fund")

        # 验证返回的是DataFrame或者是None
        assert isinstance(df_fund, pd.DataFrame)

        # 如果返回了数据，验证包含基本列
        if not df_fund.empty:
            expected_columns = [
                "code",
                "symbol",
                "type",
                "listed_date",
                "de_listed_date",
            ]
            assert all(col in df_fund.columns for col in expected_columns)

    def test_get_info_futures(self):
        """测试获取期货信息"""
        # 获取期货信息
        df_futures = self.api.get_info("futures")

        # 验证返回的是DataFrame或者是None
        assert isinstance(df_futures, pd.DataFrame)

        # 如果返回了数据，验证包含基本列
        if not df_futures.empty:
            expected_columns = [
                "code",
                "symbol",
                "type",
                "listed_date",
                "de_listed_date",
            ]
            assert all(col in df_futures.columns for col in expected_columns)

    def test_get_info_etf(self):
        """测试获取ETF信息"""
        # 获取ETF信息
        df_etf = self.api.get_info("etf")

        # 验证返回的是DataFrame或者是None
        assert isinstance(df_etf, pd.DataFrame)

        # 如果返回了数据，验证包含基本列
        if not df_etf.empty:
            expected_columns = [
                "code",
                "symbol",
                "type",
                "listed_date",
                "de_listed_date",
            ]
            assert all(col in df_etf.columns for col in expected_columns)

    def test_get_info_unregistered_type(self):
        """测试获取未注册类型的信息"""
        # 获取未注册类型的信息，应该发出警告并返回空DataFrame
        with pytest.warns(
            UserWarning,
            match="No configuration for info type 'CSS'. Return empty DataFrame.",
        ):
            df = self.api.get_info("CSS")  # CSS是没有注册的类型
        # 验证返回的是空DataFrame
        assert df.empty

    def test_register_new_info_type(self):
        """测试注册新的信息类型"""
        import rqdatac as rq

        from xqdata.rq.func_factor import rename_columns

        # 注册新的信息类型
        self.api.register_info_type(
            "test_stock", rq.all_instruments, {"type": "CS"}, rename_columns, {}
        )

        # 验证新类型已注册
        assert "test_stock" in self.api.info_config

        # 使用新类型获取信息
        df_test_stock = self.api.get_info("test_stock")
        df_stock = self.api.get_info("stock")

        # 验证返回的和stock一样
        assert df_test_stock.equals(df_stock)
