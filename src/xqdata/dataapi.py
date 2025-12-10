# coding=utf-8
from __future__ import annotations

import datetime
from abc import ABCMeta, abstractmethod
from importlib import import_module
from typing import Any, List, Optional, Union

import pandas as pd


class DataApi(metaclass=ABCMeta):
    """
    数据API抽象基类，定义了数据访问接口的标准方法。
    所有具体的数据API实现都应继承此类并实现所有抽象方法。
    """

    @abstractmethod
    def auth(self, *args: Any, **kwargs: Any) -> None:
        """
        认证方法，用于初始化API连接和验证身份。

        Args:
            *args: 可变位置参数，用于传递认证所需的信息
            **kwargs: 关键字参数，用于传递认证所需的信息（如token、username、password等）
        """
        pass

    @abstractmethod
    def get_info(self, type: str, **kwargs: Any) -> pd.DataFrame:
        """
        获取基础信息数据

        Args:
            type: 信息类型，字符串
            **kwargs: 查询参数,必须在type返回的df的columns中

        Returns:
            包含所需信息的DataFrame，如果获取失败则返回空DataFrame
        """
        pass

    @abstractmethod
    def get_factor(
        self,
        factors: Union[str, List[str]],
        codes: Union[str, List[str]],
        start_time: Optional[Union[str, datetime.datetime, datetime.date]] = None,
        end_time: Optional[Union[str, datetime.datetime, datetime.date]] = None,
        frequency: str = "D",
        panel: bool = True,
    ) -> pd.DataFrame:
        """
        获取因子数据

        Args:
            factors: 因子名称，可以是单个字符串或字符串列表
            codes: 证券代码，可以是单个字符串或字符串列表
            start_time: 开始时间
            end_time: 结束时间
            frequency: 数据频率，默认为日频
            panel: 是否返回面板数据格式

        Returns:
            包含因子数据的DataFrame
        """
        pass

    @abstractmethod
    def get_dualkey_factor(
        self,
        factors: Union[str, List[str]],
        codes: Union[str, List[str]],
        objects: Union[str, List[str]] = None,
        start_time: Optional[Union[str, datetime.datetime, datetime.date]] = None,
        end_time: Optional[Union[str, datetime.datetime, datetime.date]] = None,
        frequency: str = "D",
        panel: bool = True,
    ) -> pd.DataFrame:
        """
        获取双键因子数据（例如持仓、基差等）

        Args:
            factors: 因子名称，可以是单个字符串或字符串列表
            codes: 主键代码，可以是单个字符串或字符串列表，（如客户号）
            objects: 副键（如产品代码）
            start_time: 开始时间
            end_time: 结束时间
            frequency: 数据频率，默认为日频("D")
            panel: 是否返回面板数据格式

        Returns:
            包含双键因子数据的DataFrame
        """
        pass

    # 可以考虑实现一些通用的辅助方法作为非抽象方法
    def _parse_time_param(
        self, time_param: Optional[Union[str, datetime.datetime, datetime.date]]
    ) -> Optional[datetime.datetime]:
        """
        解析时间参数，将不同格式的时间转换为datetime对象

        Args:
            time_param: 时间参数

        Returns:
            解析后的datetime对象，如果输入为None则返回None
        """
        if time_param is None:
            return None
        if isinstance(time_param, (datetime.datetime, datetime.date)):
            return pd.to_datetime(time_param)
        return pd.to_datetime(time_param)


# TODO: 实现get_bars和get_tradedays等快捷方法
# 当这些方法需要被所有实现类支持时，可以取消注释并添加到抽象接口中


def get_dataapi(api: str = "mock") -> DataApi:
    """
    获取数据API实例的工厂函数

    Args:
        api: API名称，默认为"mock"

    Returns:
        DataApi实例
    """
    try:
        dataapi: DataApi = import_module(f"xqdata.{api}").instance
    except ModuleNotFoundError:
        dataapi: DataApi = import_module("xqdata.mock").instance

    return dataapi
