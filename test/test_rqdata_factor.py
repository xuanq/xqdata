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

    def test_rq_get_factor_post(self):
        """测试内部的rq_get_price函数"""
        from xqdata.rq.func_factor import rq_get_price

        factors = ["open_post", "close_post"]
        codes = ["000001.XSHE", "000002.XSHE"]

        df = rq_get_price(
            factors=factors,
            codes=codes,
            start_time="2023-01-01",
            end_time="2023-01-10",
            frequency="D",
        )

        # 验证包含必要的列
        assert df.index.names == ["datetime", "code"]
        # 验证包含请求的因子列
        for factor in factors:
            assert factor in df.columns
