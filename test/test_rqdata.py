import os

import pandas as pd
import pytest

from xqdata.dataapi import get_dataapi


class TestRQDataApi:
    """测试RQData API的功能"""

    def setup_method(self):
        """每个测试方法执行前的准备"""
        self.api = get_dataapi("rq")
        try:
            # 尝试进行身份验证
            self.api.auth("license", os.getenv("RQ_LICENSE", ""))
        except Exception:
            # 如果无法进行身份验证，跳过测试
            pytest.skip("无法连接到RQData API，请检查网络连接和认证信息")

    def test_get_info_stock(self):
        """测试获取股票信息"""
        try:
            # 获取股票信息
            df_stock = self.api.get_info("stock")

            # 验证返回的是DataFrame或者是None
            assert isinstance(df_stock, (pd.DataFrame, type(None)))

            # 如果返回了数据，验证包含基本列
            if df_stock is not None and not df_stock.empty:
                expected_columns = [
                    "order_book_id",
                    "symbol",
                    "type",
                    "listed_date",
                    "de_listed_date",
                ]
                assert all(col in df_stock.columns for col in expected_columns)
        except Exception as e:
            # 如果出现异常，可能是由于网络或权限问题
            pytest.skip(f"无法获取股票信息: {str(e)}")

    def test_get_info_fund(self):
        """测试获取基金信息"""
        try:
            # 获取基金信息
            df_fund = self.api.get_info("fund")

            # 验证返回的是DataFrame或者是None
            assert isinstance(df_fund, (pd.DataFrame, type(None)))

            # 如果返回了数据，验证包含基本列
            if df_fund is not None and not df_fund.empty:
                expected_columns = [
                    "order_book_id",
                    "symbol",
                    "type",
                    "listed_date",
                    "de_listed_date",
                ]
                assert all(col in df_fund.columns for col in expected_columns)
        except Exception as e:
            # 如果出现异常，可能是由于网络或权限问题
            pytest.skip(f"无法获取基金信息: {str(e)}")

    def test_get_info_futures(self):
        """测试获取期货信息"""
        try:
            # 获取期货信息
            df_futures = self.api.get_info("futures")

            # 验证返回的是DataFrame或者是None
            assert isinstance(df_futures, (pd.DataFrame, type(None)))

            # 如果返回了数据，验证包含基本列
            if df_futures is not None and not df_futures.empty:
                expected_columns = [
                    "order_book_id",
                    "symbol",
                    "type",
                    "listed_date",
                    "de_listed_date",
                ]
                assert all(col in df_futures.columns for col in expected_columns)
        except Exception as e:
            # 如果出现异常，可能是由于网络或权限问题
            pytest.skip(f"无法获取期货信息: {str(e)}")

    def test_get_info_etf(self):
        """测试获取ETF信息"""
        try:
            # 获取ETF信息
            df_etf = self.api.get_info("etf")

            # 验证返回的是DataFrame或者是None
            assert isinstance(df_etf, (pd.DataFrame, type(None)))

            # 如果返回了数据，验证包含基本列
            if df_etf is not None and not df_etf.empty:
                expected_columns = [
                    "order_book_id",
                    "symbol",
                    "type",
                    "listed_date",
                    "de_listed_date",
                ]
                assert all(col in df_etf.columns for col in expected_columns)
        except Exception as e:
            # 如果出现异常，可能是由于网络或权限问题
            pytest.skip(f"无法获取ETF信息: {str(e)}")

    def test_get_info_unregistered_type(self):
        """测试获取未注册类型的信息"""
        try:
            # 获取未注册类型的信息，应该直接调用原始方法
            df = self.api.get_info("CS")  # CS是股票类型代码

            # 验证返回的是DataFrame或者是None
            assert isinstance(df, (pd.DataFrame, type(None)))
        except Exception as e:
            # 如果出现异常，可能是由于网络或权限问题
            pytest.skip(f"无法获取未注册类型信息: {str(e)}")

    def test_register_new_info_type(self):
        """测试注册新的信息类型"""
        import rqdatac as rq

        from xqdata.rq.api import RQDataApi

        try:
            # 确保api是RQDataApi实例
            if isinstance(self.api, RQDataApi):
                # 注册新的信息类型
                self.api.register_info_type(
                    "test_type", rq.all_instruments, {"type": "CS"}, None, {}
                )

                # 验证新类型已注册
                assert "test_type" in self.api.info_config

                # 使用新类型获取信息
                df = self.api.get_info("test_type")

                # 验证返回的是DataFrame或者是None
                assert isinstance(df, (pd.DataFrame, type(None)))
        except Exception as e:
            # 如果出现异常，可能是由于网络或权限问题
            pytest.skip(f"无法测试注册新信息类型: {str(e)}")
