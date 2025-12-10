import warnings
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
import rqdatac as rq

from xqdata.dataapi import DataApi

from .config import FACTOR_CONFIG, FACTOR_EXTRA_PARAMS, INFO_CONFIG


class RQDataApi(DataApi):
    def __init__(self):
        # 配置管理不同类型的信息查询
        self.info_config = INFO_CONFIG.copy()
        # 配置管理不同因子的查询
        self.factor_config = FACTOR_CONFIG.copy()
        # 存储额外参数的字典
        self._extra_params = {}

    def auth(self, username=None, password=None):
        return rq.init(username=username, password=password)

    def get_info(self, type: str, **kwargs) -> pd.DataFrame:
        """
        获取基础信息数据

        Args:
            type: 信息类型，字符串
            **kwargs: 查询参数

        Returns:
            包含所需信息的DataFrame，如果获取失败则返回空DataFrame
        """
        # 检查是否有对应类型的配置
        if type not in self.info_config:
            # 如果没有配置，warning并返回空DataFrame
            warnings.warn(
                f"No configuration for info type '{type}'. Return empty DataFrame."
            )
            return pd.DataFrame()

        # 获取配置
        config = self.info_config[type]

        # 合并参数
        params = config["params"].copy()
        params.update(kwargs)

        # 调用对应的RQData接口
        try:
            result = config["func"](**params)

            return result
        except Exception:
            # 如果出现异常，返回空DataFrame
            warnings.warn(f"Error fetching info type '{type}'. Return empty DataFrame.")
            return pd.DataFrame()

    def register_info_type(
        self,
        type_name: str,
        func: Callable,
        params: Dict[str, Any],
    ):
        """
        注册新的信息类型查询配置

        Args:
            type_name: 类型名称
            func: 调用的函数(RQData接口或包装后的函数)
            params: 传递给函数的参数
        """
        self.info_config[type_name] = {
            "func": func,
            "params": params,
        }

    def set_extra_param(self, func_name: str, param_name: str, param_value: Any):
        """
        设置函数的额外参数

        Args:
            func_name: 函数名称
            param_name: 参数名称
            param_value: 参数值
        """
        if func_name not in self._extra_params:
            self._extra_params[func_name] = {}
        self._extra_params[func_name][param_name] = param_value

    def get_factor(
        self,
        factors: Union[str, List[str]],
        codes: Union[str, List[str]],
        start_time: Optional[Union[str, datetime, date]] = None,
        end_time: Optional[Union[str, datetime, date]] = None,
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
        # 确保factors和codes都是列表
        if isinstance(factors, str):
            factors = [factors]
        if isinstance(codes, str):
            codes = [codes]

        # 初始化结果DataFrame
        data = pd.DataFrame()

        # 按照配置对因子进行分组，具有相同配置的因子合并查询以节约查询次数
        # 创建一个字典来存储每个查询函数对应的因子列表
        func_factor_map = {}

        # 遍历所有请求的因子
        for factor in factors:
            # 查找因子对应的配置
            if factor in self.factor_config:
                func = self.factor_config[factor]
                # 将因子添加到对应的查询函数列表中
                if func not in func_factor_map:
                    func_factor_map[func] = []
                func_factor_map[func].append(factor)
            else:
                # 如果因子没有配置，使用默认配置
                func = self.factor_config.get("default", None)
                if func:
                    if func not in func_factor_map:
                        func_factor_map[func] = []
                    func_factor_map[func].append(factor)

        # 对每组因子执行查询并将结果合并
        for func, factor_group in func_factor_map.items():
            try:
                # 准备调用参数
                kwargs = {
                    "factors": factor_group,
                    "codes": codes,
                    "start_time": start_time,
                    "end_time": end_time,
                    "frequency": frequency,
                }

                # 如果该函数有额外参数配置，则添加这些参数
                func_name = func.__name__
                if func_name in FACTOR_EXTRA_PARAMS:
                    # 获取允许的额外参数列表
                    allowed_params = FACTOR_EXTRA_PARAMS[func_name]
                    # 添加已设置的额外参数
                    if func_name in self._extra_params:
                        for param_name, param_value in self._extra_params[
                            func_name
                        ].items():
                            # 只添加允许的参数
                            if param_name in allowed_params:
                                kwargs[param_name] = param_value
                # 调用对应的查询函数
                result = func(**kwargs)
                # 如果返回了数据，则合并到主DataFrame中
                if result is not None and not result.empty:
                    if data.empty:
                        data = result
                    else:
                        # 合并数据，基于索引进行合并
                        data = data.merge(
                            result, left_index=True, right_index=True, how="outer"
                        )
            except Exception as e:
                # 如果某个查询出错，记录警告但继续处理其他因子
                warnings.warn(f"Error fetching factors {factor_group}: {str(e)}")

        # 如果没有数据，返回空的DataFrame
        if data.empty:
            return data

        # 根据panel参数决定返回的数据格式
        if not panel:
            # 转换为长格式
            data = data.stack().reset_index(level=-1)
            data.columns = ["attribute", "value"]
        return data

    def get_dualkey_factor(
        self,
        factors: Union[str, List[str]],
        codes: Union[str, List[str]],
        objects: Union[str, List[str]] = None,
        start_time: Optional[Union[str, datetime, date]] = None,
        end_time: Optional[Union[str, datetime, date]] = None,
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
        # 确保参数都是列表
        if isinstance(factors, str):
            factors = [factors]
        if isinstance(codes, str):
            codes = [codes]
        if isinstance(objects, str):
            objects = [objects]
        elif objects is None:
            objects = []

        # 初始化结果DataFrame
        data = pd.DataFrame()

        # 按照配置对因子进行分组，具有相同配置的因子合并查询以节约查询次数
        # 创建一个字典来存储每个查询函数对应的因子列表
        func_factor_map = {}

        # 遍历所有请求的因子
        for factor in factors:
            # 查找因子对应的配置
            if factor in self.factor_config:
                func = self.factor_config[factor]
                # 将因子添加到对应的查询函数列表中
                if func not in func_factor_map:
                    func_factor_map[func] = []
                func_factor_map[func].append(factor)
            else:
                # 如果因子没有配置，使用默认配置
                func = self.factor_config.get("default", None)
                if func:
                    if func not in func_factor_map:
                        func_factor_map[func] = []
                    func_factor_map[func].append(factor)

        # 对每组因子执行查询并将结果合并
        for func, factor_group in func_factor_map.items():
            try:
                # 准备调用参数
                kwargs = {
                    "factors": factor_group,
                    "codes": codes,
                    "objects": objects,
                    "start_time": start_time,
                    "end_time": end_time,
                    "frequency": frequency,
                }

                # 如果该函数有额外参数配置，则添加这些参数
                func_name = func.__name__
                if func_name in FACTOR_EXTRA_PARAMS:
                    # 获取允许的额外参数列表
                    allowed_params = FACTOR_EXTRA_PARAMS[func_name]
                    # 添加已设置的额外参数
                    if func_name in self._extra_params:
                        for param_name, param_value in self._extra_params[
                            func_name
                        ].items():
                            # 只添加允许的参数
                            if param_name in allowed_params:
                                kwargs[param_name] = param_value

                # 特殊处理：对于双键因子，我们需要确保传入objects参数
                # 检查函数是否接受objects参数
                import inspect

                sig = inspect.signature(func)
                if "objects" not in sig.parameters:
                    # 如果函数不接受objects参数，则从kwargs中移除
                    kwargs.pop("objects", None)

                # 调用对应的查询函数
                result = func(**kwargs)

                # 如果返回了数据，则合并到主DataFrame中
                if result is not None and not result.empty:
                    if data.empty:
                        data = result
                    else:
                        # 合并数据，基于索引进行合并
                        data = data.merge(
                            result, left_index=True, right_index=True, how="outer"
                        )
            except Exception as e:
                # 如果某个查询出错，记录警告但继续处理其他因子
                warnings.warn(f"Error fetching factors {factor_group}: {str(e)}")

        # 如果没有数据，返回空的DataFrame
        if data.empty:
            return data

        # 根据panel参数决定返回的数据格式
        if not panel:
            # 转换为长格式
            data = data.stack().reset_index(level=-1)
            data.columns = ["attribute", "value"]

        return data
