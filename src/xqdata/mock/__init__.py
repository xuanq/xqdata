import random
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Union

import numpy as np
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

    def get_info(self, type: str, **kwargs) -> pd.DataFrame:
        """
        获取模拟的基础信息数据

        Args:
            type: 信息类型名称
            **kwargs: 过滤条件，例如 listed_date>="2024-01-01"

        Returns:
            符合要求的DataFrame
        """
        # 检查是否设置了该类型的schema
        if type not in self._mock_info_schemas:
            # raise warning and return empty DataFrame
            warnings.warn(
                f"No mock schema set for type '{type}'. Returning empty DataFrame."
            )
            return pd.DataFrame()

        # 获取schema
        schema = self._mock_info_schemas[type]

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
        frequency="D",
        panel=True,
    ) -> pd.DataFrame:
        """
        获取模拟的因子数据

        Args:
            factors: 因子名称或名称列表
            codes: 股票代码或代码列表
            start_time: 开始时间
            end_time: 结束时间
            frequency: 频率 pandas Offset aliases ('B'=工作日, 'D'=日(默认), 'W'=周, 'ME'=月)
            panel: 是否返回面板数据格式

        Returns:
            包含因子数据的DataFrame
        """

        # 确保factors和codes都是列表
        if isinstance(factors, str):
            factors = [factors]
        if isinstance(codes, str):
            codes = [codes]

        # 设置默认时间范围
        if start_time is None:
            start_time = pd.Timestamp.now() - pd.Timedelta(days=365)
        else:
            start_time = pd.Timestamp(start_time)

        if end_time is None:
            end_time = pd.Timestamp.now()
        else:
            end_time = pd.Timestamp(end_time)

        # 根据频率生成日期范围
        dates = pd.date_range(start=start_time, end=end_time, freq=frequency)

        # 创建所有组合
        index = pd.MultiIndex.from_product(
            [dates, codes, factors], names=["datetime", "code", "attribute"]
        )

        # 生成数据, 随机确定均值和方差后，从正态分布中抽样
        mean = random.uniform(-10, 10)
        std = random.uniform(1, 5)
        data = np.random.normal(mean, std, size=len(index))
        # 创建DataFrame
        df = pd.DataFrame(data, index=index, columns=["value"])
        df = df.reset_index()

        # 如果需要面板数据格式，则进行透视
        if panel:
            # 使用pivot_table将attribute列展开成面板数据
            df_pivot = df.pivot_table(
                index=["datetime", "code"], columns="attribute", values="value"
            )
            # 重置列名层级
            df_pivot.columns.name = None
            return df_pivot
        else:
            # 设置多级索引
            df_result = df.set_index(["datetime", "code"])
            return df_result

    def get_dualkey_factor(
        self,
        factors: Union[str, List[str]],
        codes: Union[str, List[str]],
        objects: Union[str, List[str]] = None,
        start_time=None,
        end_time=None,
        frequency="D",
        panel=True,
    ) -> pd.DataFrame:
        """
        获取模拟的双键因子数据

        Args:
            factors: 因子名称或名称列表
            codes: 股票代码或代码列表
            objects: 对象代码或代码列表
            start_time: 开始时间
            end_time: 结束时间
            frequency: 频率 pandas Offset aliases ('B'=工作日, 'D'=日(默认), 'W'=周, 'ME'=月)
            panel: 是否返回面板数据格式

        Returns:
            包含双键因子数据的DataFrame
        """

        # 确保factors、codes和objects都是列表
        if isinstance(factors, str):
            factors = [factors]
        if isinstance(codes, str):
            codes = [codes]
        if isinstance(objects, str):
            objects = [objects]

        # 设置默认时间范围
        if start_time is None:
            start_time = pd.Timestamp.now() - pd.Timedelta(days=365)
        else:
            start_time = pd.Timestamp(start_time)

        if end_time is None:
            end_time = pd.Timestamp.now()
        else:
            end_time = pd.Timestamp(end_time)

        # 根据频率生成日期范围
        dates = pd.date_range(start=start_time, end=end_time, freq=frequency)

        # 创建所有组合
        index = pd.MultiIndex.from_product(
            [dates, codes, objects, factors],
            names=["datetime", "code", "object", "attribute"],
        )

        # 生成数据, 随机确定均值和方差后，从正态分布中抽样
        mean = random.uniform(-10, 10)
        std = random.uniform(1, 5)
        data = np.random.normal(mean, std, size=len(index))
        # 创建DataFrame
        df = pd.DataFrame(data, index=index, columns=["value"])
        df = df.reset_index()

        # 如果需要面板数据格式，则进行透视
        if panel:
            # 使用pivot_table将attribute列展开成面板数据
            df_pivot = df.pivot_table(
                index=["datetime", "code", "object"],
                columns="attribute",
                values="value",
            )
            # 重置列名层级
            df_pivot.columns.name = None
            return df_pivot
        else:
            # 设置多级索引
            df_result = df.set_index(["datetime", "code", "object"])
            return df_result


# 创建单例实例
instance = MockDataApi()
