import os

import pytest

from xqdata.dataapi import get_dataapi


class TestRQDataFactorApi:
    """测试RQData API的因子功能"""

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

    def test_rq_get_price_1d(self):
        """测试内部的rq_get_price函数"""
        from xqdata.rq.func_factor import rq_get_price

        factors = ["open", "close", "open_post", "close_post"]
        codes = ["000001.XSHE", "300750.XSHE"]

        df = rq_get_price(
            factors=factors,
            codes=codes,
            start_time="2025-01-01",
            end_time="2025-01-10",
            frequency="D",
        )

        # 验证包含必要的列
        assert df.index.names == ["datetime", "code"]
        # 验证包含请求的因子列
        for factor in factors:
            assert factor in df.columns
        # 平安银行2025-01-10的收盘价11.30（不复权）
        assert df.loc[("2025-01-10 00:00:00", "000001.XSHE"), "close"] == 11.30
        assert df.loc[
            ("2025-01-10 00:00:00", "300750.XSHE"), "close_post"
        ] == pytest.approx(459.11964)
        assert len(df) == 14  # 7交易日 * 2只股票

    def test_rq_get_price_1m(self):
        """测试内部的rq_get_price函数"""
        from xqdata.rq.func_factor import rq_get_price

        factors = ["open_post", "close_post"]
        codes = ["000001.XSHE", "300750.XSHE"]

        df = rq_get_price(
            factors=factors,
            codes=codes,
            start_time="2025-01-02 09:30:00",
            end_time="2025-01-02 15:00:00",
            frequency="min",
        )

        # 验证包含必要的列
        assert df.index.names == ["datetime", "code"]
        # 验证包含请求的因子列
        for factor in factors:
            assert factor in df.columns

        assert len(df) == 480  # 240分钟 * 2只股票

    def test_rq_get_price_tick(self):
        """测试内部的rq_get_price函数"""
        from xqdata.rq.func_factor import rq_get_price

        factors = ["a1", "b1"]
        codes = ["000001.XSHE", "300750.XSHE"]

        df = rq_get_price(
            factors=factors,
            codes=codes,
            start_time="2025-01-02 10:00:03",
            end_time="2025-01-02 10:02:00",
            frequency="tick",
        )

        # 验证包含必要的列
        assert df.index.names == ["datetime", "code"]
        # 验证包含请求的因子列
        for factor in factors:
            assert factor in df.columns

        assert len(df) == 80  # 20tick/min * 2分钟 * 2只股票

    def test_rq_get_factor(self):
        """测试内部的rq_get_price函数"""
        from xqdata.rq.func_factor import rq_get_factor

        factors = ["pe_ratio", "pb_ratio"]
        codes = ["000001.XSHE", "300750.XSHE"]

        df = rq_get_factor(
            factors=factors,
            codes=codes,
            start_time="2025-01-02",
            end_time="2025-01-10",
            frequency="D",
        )

        # 验证包含必要的列
        assert df.index.names == ["datetime", "code"]
        # 验证包含请求的因子列
        for factor in factors:
            assert factor in df.columns

        assert len(df) == 14  # 7交易日 * 2只股票

    def test_rq_is_st_stock(self):
        """测试内部的rq_is_st_stock函数"""
        from xqdata.rq.func_factor import rq_is_st_stock

        factors = ["is_st"]
        codes = ["000001.XSHE", "300750.XSHE"]

        df = rq_is_st_stock(
            factors=factors,
            codes=codes,
            start_time="2025-01-02",
            end_time="2025-01-10",
            frequency="D",
        )

        # 验证包含必要的列
        assert df.index.names == ["datetime", "code"]
        # 验证包含请求的因子列
        for factor in factors:
            assert factor in df.columns

        assert len(df) == 14  # 7交易日 * 2只股票

    def test_rq_is_suspended(self):
        """测试内部的rq_is_suspended函数"""
        from xqdata.rq.func_factor import rq_is_suspended

        factors = ["is_paused"]
        codes = ["000001.XSHE", "300750.XSHE"]

        df = rq_is_suspended(
            factors=factors,
            codes=codes,
            start_time="2025-01-02",
            end_time="2025-01-10",
            frequency="D",
        )

        # 验证包含必要的列
        assert df.index.names == ["datetime", "code"]
        # 验证包含请求的因子列
        for factor in factors:
            assert factor in df.columns

        assert len(df) == 14  # 7交易日 * 2只股票

    def test_rq_get_instrument_industry(self):
        """测试内部的rq_get_instrument_industry函数"""
        from xqdata.rq.func_factor import rq_get_instrument_industry

        factors = ["citics_2019_l1", "citics_2019_l2_name", "sws_l1"]
        codes = ["000001.XSHE", "300750.XSHE"]

        df = rq_get_instrument_industry(
            factors=factors,
            codes=codes,
            start_time="2025-01-02",
            end_time="2025-01-10",
            frequency="D",
        )

        # 验证包含必要的列
        assert df.index.names == ["datetime", "code"]
        # 验证包含请求的因子列
        for factor in factors:
            assert factor in df.columns
        assert len(df.columns) == 3
        assert len(df) == 14  # 7交易日 * 2只股票

    def test_rq_get_factor_exposure(self):
        """测试内部的rq_get_instrument_industry函数"""
        from xqdata.rq.func_factor import rq_get_factor_exposure

        factors = ["momentum", "beta", "size"]
        codes = ["000001.XSHE", "300750.XSHE"]

        df = rq_get_factor_exposure(
            factors=factors,
            codes=codes,
            start_time="2025-01-02",
            end_time="2025-01-10",
            frequency="D",
        )

        # 验证包含必要的列
        assert df.index.names == ["datetime", "code"]
        # 验证包含请求的因子列
        for factor in factors:
            assert factor in df.columns
        assert len(df.columns) == 3
        assert len(df) == 14  # 7交易日 * 2只股票

    def test_api_get_factor_nopanel(self):
        """测试API的get_factor方法"""
        factors = ["pe_ratio", "pb_ratio", "open_post", "close_post"]
        codes = ["000001.XSHE", "300750.XSHE"]

        df = self.api.get_factor(
            factors=factors,
            codes=codes,
            start_time="2025-01-02",
            end_time="2025-01-10",
            frequency="D",
            panel=False,
        )

        # 验证包含必要的列
        assert df.columns.tolist() == ["attribute", "value"]

        assert len(df) == 56  # 7交易日 * 2只股票 * 4因子

    def test_api_get_factor_panel(self):
        """测试API的get_factor方法"""
        factors = [
            "pe_ratio",
            "pb_ratio",
            "open_post",
            "close_post",
            "cticis_2019_l1",
            "is_st",
            "ebit_lyr",
            "size",
            "total_a"
        ]  # ebit_lyr尚未定义，引用default的rq_get_factor获取
        codes = ["000001.XSHE", "300750.XSHE"]

        df = self.api.get_factor(
            factors=factors,
            codes=codes,
            start_time="2025-01-02",
            end_time="2025-01-10",
            frequency="D",
            panel=True,
        )

        # 验证包含请求的因子列
        for factor in factors:
            assert factor in df.columns

        assert len(df) == 14  # 7交易日 * 2只股票

    def test_api_get_factor_panel_extra_param(self):
        """测试API的get_factor方法设置额外参数"""
        factors = [
            "Liquidity",
            "size",
        ]  # ebit_lyr尚未定义，引用default的rq_get_factor获取
        codes = ["000001.XSHE", "300750.XSHE"]
        default_param_df = self.api.get_factor(
            factors=factors,
            codes=codes,
            start_time="2025-01-02",
            end_time="2025-01-10",
            frequency="D",
            panel=True,
        )
        self.api.set_extra_param("rq_get_factor_exposure", "model", "v2")
        extra_param_df = self.api.get_factor(
            factors=factors,
            codes=codes,
            start_time="2025-01-02",
            end_time="2025-01-10",
            frequency="D",
            panel=True,
        )

        assert extra_param_df.equals(default_param_df) is False
