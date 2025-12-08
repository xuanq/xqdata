import warnings
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
import rqdatac as rq

from xqdata.dataapi import DataApi

from .config import FACTOR_CONFIG, INFO_CONFIG


class RQDataApi(DataApi):
    def __init__(self):
        # 配置管理不同类型的信息查询
        self.info_config = INFO_CONFIG.copy()
        # 配置管理不同因子的查询
        self.factor_config = FACTOR_CONFIG.copy()

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
        params = config["rq_params"].copy()
        params.update(kwargs)

        # 调用对应的RQData接口
        try:
            result = config["rq_func"](**params)

            # 如果有后处理函数，则应用后处理
            if config["post_process"] and callable(config["post_process"]):
                result = config["post_process"](result, **config["post_process_args"])

            return result
        except Exception:
            # 如果出现异常，返回空DataFrame
            warnings.warn(f"Error fetching info type '{type}'. Return empty DataFrame.")
            return pd.DataFrame()

    def register_info_type(
        self,
        type_name: str,
        rq_func: Callable,
        rq_params: Dict[str, Any],
        post_process: Callable = None,
        post_process_args: Dict[str, Any] = None,
    ):
        """
        注册新的信息类型查询配置

        Args:
            type_name: 类型名称
            rq_func: 调用的RQData函数
            rq_params: 传递给RQData函数的参数
            post_process: 后处理函数
            post_process_args: 传递给后处理函数的参数
        """
        self.info_config[type_name] = {
            "rq_func": rq_func,
            "rq_params": rq_params,
            "post_process": post_process,
            "post_process_args": post_process_args or {},
        }

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

        # 按照配置对因子进行分组，具有相同配置的因子合并查询以节约查询次数
        pass

    # if "is_paused" in factors:
    #     paused_data = _is_suspended(codes, start_time, end_time)
    #     data["is_paused"] = paused_data

    # if "is_st" in factors:
    #     st_data = _is_st_stock(codes, start_time, end_time)
    #     data["is_st"] = st_data

    # price_factors = [
    #     f
    #     for f in factors
    #     if f
    #     in (
    #         "open",
    #         "high",
    #         "low",
    #         "close",
    #         "volume",
    #         "amount",
    #         "trading_date",
    #         "last",
    #         "prev_close",
    #         "total_turnover",
    #         "limit_up",
    #         "limit_down",
    #         "a1",
    #         "a2",
    #         "a3",
    #         "a4",
    #         "a5",
    #         "b1",
    #         "b2",
    #         "b3",
    #         "b4",
    #         "b5",
    #         "a1_v",
    #         "a2_v",
    #         "a3_v",
    #         "a4_v",
    #         "a5_v",
    #         "b1_v",
    #         "b2_v",
    #         "b3_v",
    #         "b4_v",
    #         "b5_v",
    #         "change_rate",
    #         "num_trades",
    #         "open_interest",
    #         "prev_settlement",
    #     )
    # ]

    # if price_factors:
    #     price_data = _get_price(
    #         codes, start_time, end_time, frequency=frequency, fields=price_factors
    #     )
    #     data[price_factors] = price_data[price_factors]

    # for adjust_type in ("post", "pre"):
    #     adjust_factors = [f for f in factors if f.endswith(f"_{adjust_type}")]
    #     fields = [f.split("_")[0] for f in adjust_factors]
    #     if adjust_factors:
    #         price_data = _get_price(
    #             codes,
    #             start_time,
    #             end_time,
    #             frequency=frequency,
    #             fields=fields,
    #             adjust=adjust_type,
    #         )
    #         price_data = price_data.add_suffix(f"_{adjust_type}")
    #         data[adjust_factors] = price_data[adjust_factors]

    # risk_factors = [
    #     "momentum",
    #     "beta",
    #     "book_to_price",
    #     "earnings_yield",
    #     "liquidity",
    #     "size",
    #     "residual_volatility",
    #     "non_linear_size",
    #     "leverage",
    #     "growth",
    # ]
    # queryed_risk_factors = [f for f in factors if f in risk_factors]
    # if queryed_risk_factors:
    #     risk_factor_data = _get_factor_exposure(
    #         codes, start_time, end_time, queryed_risk_factors
    #     )
    #     data[queryed_risk_factors] = risk_factor_data[queryed_risk_factors]

    # pattern = r"^(citics|gildata|citics_2019)_l(\d)"
    # query_industry = [
    #     (re.findall(pattern, f)[0][0], int(re.findall(pattern, f)[0][1]))
    #     for f in factors
    #     if len(re.findall(pattern, f)) > 0
    # ]
    # if len(query_industry) > 0:
    #     temp_df = []
    #     for industry, level in query_industry:
    #         temp_df.append(
    #             pd.concat(
    #                 [
    #                     _get_instrument_industry(
    #                         codes=codes, source=industry, level=level, date=date
    #                     )
    #                     for date in pd.date_range(start_time, end_time)
    #                 ]
    #             )
    #         )
    #     industry_df = pd.concat(temp_df, axis=1)

    #     cols = industry_df.columns.intersection(factors)
    #     data[cols] = industry_df[cols]

    # if data.empty:
    #     return data

    # if panel:
    #     return data
    # else:
    #     data.columns.name = "factorname"
    #     data = data.stack().to_frame()
    #     data.columns = ["value"]
    #     data = Factordf(data)
    #     if df:
    #         return data
    #     else:
    #         return data.to_datacls()

    def get_dualkey_factor(
        self,
        factors: Union[str, List[str]],
        codes: Union[str, List[str]],
        objects: Union[str, List[str]],
        start_time: Optional[Union[str, datetime, date]] = None,
        end_time: Optional[Union[str, datetime, date]] = None,
        frequency: str = "D",
        panel: bool = True,
    ) -> pd.DataFrame:
        # 确保参数都是列表
        if isinstance(factors, str):
            factors = [factors]
        if isinstance(codes, str):
            codes = [codes]
        if isinstance(objects, str):
            objects = [objects]

        # 目前只实现了简单的占位符实现
        # 在实际应用中，这里会根据具体的双键因子类型调用相应的RQData接口
        data = pd.DataFrame()

        return data
