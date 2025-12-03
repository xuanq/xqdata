from typing import List, Optional, Union

import pandas as pd

from xqdata.dataapi import DataApi


class MockDataApi(DataApi):
    """
    模拟数据API的实现
    """

    def __init__(self):
        self._authenticated = True
        # 初始化模拟数据API连接等

    def auth(self, *args, **kwargs) -> None:
        # 实现模拟数据API的认证逻辑
        self._authenticated = True

    def get_info(self, types: Optional[str] = None, **kwargs) -> Optional[pd.DataFrame]:
        # 实现从模拟数据API获取基础信息的逻辑
        pass

    def get_factor(
        self,
        factors: Union[str, List[str]],
        codes: Union[str, List[str]],
        start_time=None,
        end_time=None,
        frequency="d",
        panel=True,
    ) -> pd.DataFrame:
        # 实现从RQ获取因子数据的逻辑
        pass

    def get_dualkey_factor(
        self,
        factors: Union[str, List[str]],
        codes: Union[str, List[str]],
        objects: Union[str, List[str]],
        start_time=None,
        end_time=None,
        frequency="d",
        panel=True,
    ) -> pd.DataFrame:
        # 实现从RQ获取双键因子数据的逻辑
        pass


# 创建单例实例
instance = MockDataApi()
