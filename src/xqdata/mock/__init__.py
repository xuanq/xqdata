import random
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

import pandas as pd

from xqdata.dataapi import DataApi


class MockDataApi(DataApi):
    """
    模拟数据API的实现
    """

    def __init__(self):
        self._authenticated = True
        # 初始化模拟数据API连接等
        self._mock_info_schemas: Dict[str, Dict[str, str]] = {}

    def auth(self, *args, **kwargs) -> None:
        # 实现模拟数据API的认证逻辑
        self._authenticated = True

    def set_mock_info(self, name: str, schema: Dict[str, str]) -> None:
        """
        设置模拟数据的schema信息

        Args:
            name: 数据名称，例如"stock"
            schema: 字段名到数据类型的映射，例如{"code":"str","listed_date":"datetime","name":"str","market_value":"float64"}
        """
        self._mock_info_schemas[name] = schema

    def _generate_mock_data(self, schema: Dict[str, str], length: int) -> pd.DataFrame:
        """
        根据schema生成模拟数据

        Args:
            schema: 字段名到数据类型的映射
            length: 数据长度

        Returns:
            生成的DataFrame
        """
        data = {}

        for column, dtype in schema.items():
            if dtype == "str":
                # 生成随机字符串
                data[column] = [f"{column}_{i}" for i in range(length)]
            elif dtype == "int64" or dtype == "int":
                # 生成随机整数
                data[column] = [random.randint(1, 1000) for _ in range(length)]
            elif dtype == "float64" or dtype == "float":
                # 生成随机浮点数
                data[column] = [random.uniform(1.0, 1000.0) for _ in range(length)]
            elif dtype == "datetime":
                # 生成随机日期
                base_date = datetime(2020, 1, 1)
                data[column] = [
                    base_date + timedelta(days=random.randint(0, 1000))
                    for _ in range(length)
                ]
            elif dtype == "bool":
                # 生成随机布尔值
                data[column] = [random.choice([True, False]) for _ in range(length)]
            else:
                # 默认生成字符串
                data[column] = [f"{column}_{i}" for i in range(length)]

        return pd.DataFrame(data)

    def get_info(self, types: Optional[str] = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        获取模拟的基础信息数据

        Args:
            types: 信息类型名称
            **kwargs: 过滤条件，例如 listed_date>="2024-01-01"

        Returns:
            符合要求的DataFrame
        """
        # 检查是否设置了该类型的schema
        if types not in self._mock_info_schemas:
            # raise warning and return empty DataFrame
            warnings.warn(
                f"No mock schema set for type '{types}'. Returning empty DataFrame."
            )
            return pd.DataFrame()

        # 获取schema
        schema = self._mock_info_schemas[types]

        # 生成随机长度的模拟数据 (30-100)
        length = random.randint(30, 100)
        df = self._generate_mock_data(schema, length)

        return df.reset_index(drop=True)

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
