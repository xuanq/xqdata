from datetime import date, datetime
from typing import List, Optional, Union

import pandas as pd
import rqdatac as rq

from .utils import rename_columns


def rq_get_price(
    factors: Union[str, List[str]],
    codes: Union[str, List[str]],
    start_time: Optional[Union[str, datetime, date]] = None,
    end_time: Optional[Union[str, datetime, date]] = None,
    frequency: str = "D",
) -> pd.DataFrame:
    # args map
    if "post" in factors[0]:
        adjust = "post"
    if "pre" in factors[0]:
        adjust = "pre"
    else:
        adjust = "none"

    frequency = "1" + frequency.lower()
    if isinstance(start_time, str):
        start_time = pd.Timestamp(start_time)
    if isinstance(end_time, str):
        end_time = pd.Timestamp(end_time)
    # 期货分钟频以上夜盘算下一日，需要特殊处理
    true_end_date = end_time
    if any(unit in frequency.lower() for unit in {"ms", "s", "min", "h"}):
        if end_time.hour >= 21:
            true_end_date = rq.get_next_trading_date(end_time)

    # get_data
    data: pd.DataFrame = rq.get_price(
        order_book_ids=codes,
        start_date=start_time,
        end_date=true_end_date,
        frequency=frequency,
        fields=factors,
        adjust_type=adjust,
        skip_suspended=False,
        market="cn",
        expect_df=True,
        time_slice=None,
    )
    if data is None:
        return pd.DataFrame()
    if data.empty:
        return data
    else:
        data = data.reset_index()
    # map column names
    rename_columns(data)
    data = data[(data.datetime >= start_time) & (data.datetime <= end_time)]
    return data.set_index(["datetime", "code"])


def rq_get_factor(
    codes: str | List[str],
    factors: str | List[str],
    start_date=None,
    end_date=None,
    df=True,
):
    pass


#     # args map
#     if isinstance(codes, str):
#         codes = [codes]
#     code_mapper = get_code_mapper(codes)
#     rq_codes = [code_mapper.get(code, code) for code in codes]
#     if isinstance(factors, str):
#         factors = [factors]
#     rq_factor = [FIELDS_XQ2RQ.get(factor, factor) for factor in factors]
#     # get_data
#     data: pd.DataFrame = rq.get_factor(
#         order_book_ids=rq_codes,
#         factor=rq_factor,
#         start_date=start_date,
#         end_date=end_date,
#         universe=None,
#         expect_df=df,
#     )
#     if data is None:
#         return None
#     if data.empty:
#         return None
#     else:
#         data = data.reset_index()
#     # map column names
#     data.rename(columns=FIELDS_RQ2XQ, inplace=True)
#     # map codes
#     reversed_code_mapper = {v: k for k, v in code_mapper.items()}
#     data.code = data.code.map(reversed_code_mapper)
#     return data.set_index(["datetime", "code"])


# def _is_suspended(codes, start_date=None, end_date=None):
#     # args map
#     if isinstance(codes, str):
#         codes = [codes]
#     code_mapper = get_code_mapper(codes)
#     rq_codes = [code_mapper.get(code, code) for code in codes]
#     # get_data
#     data: pd.DataFrame = rq.is_suspended(
#         order_book_ids=rq_codes, start_date=start_date, end_date=end_date, market="cn"
#     )
#     data = data.stack()
#     data.index.names = ["datetime", "code"]
#     data.name = "is_paused"
#     data = data.reset_index()
#     # map codes
#     reversed_code_mapper = {v: k for k, v in code_mapper.items()}
#     data.code = data.code.map(reversed_code_mapper)
#     return data.set_index(["datetime", "code"])


# def _is_st_stock(codes, start_date=None, end_date=None):
#     # args map
#     if isinstance(codes, str):
#         codes = [codes]
#     code_mapper = get_code_mapper(codes)
#     rq_codes = [code_mapper.get(code, code) for code in codes]
#     data: pd.DataFrame = rq.is_st_stock(rq_codes, start_date, end_date)
#     data = data.stack()
#     data.index.names = ["datetime", "code"]
#     data.name = "is_st"
#     data = data.reset_index()
#     # map codes
#     reversed_code_mapper = {v: k for k, v in code_mapper.items()}
#     data.code = data.code.map(reversed_code_mapper)
#     return data.set_index(["datetime", "code"])


# def _get_instrument_industry(
#     codes: str | List[str], source="citics_2019", level=0, date=None
# ):
#     # args map
#     if isinstance(codes, str):
#         codes = [codes]
#     if date is not None:
#         date = pd.Timestamp(pd.Timestamp(date).date())
#     code_mapper = get_code_mapper(codes)
#     rq_codes = [code_mapper.get(code, code) for code in codes]
#     # get_data
#     data: pd.DataFrame = rq.get_instrument_industry(
#         rq_codes, source=source, level=level, date=date
#     ).reset_index()
#     # map column names
#     MAPPER = {
#         "first_industry_code": f"{source}_l1",
#         "second_industry_code": f"{source}_l2",
#         "third_industry_code": f"{source}_l3",
#         "first_industry_name": f"{source}_l1_name",
#         "second_industry_name": f"{source}_l2_name",
#         "third_industry_name": f"{source}_l3_name",
#     }
#     MAPPER.update(FIELDS_RQ2XQ)
#     data.rename(columns=MAPPER, inplace=True)
#     # map codes
#     reversed_code_mapper = {v: k for k, v in code_mapper.items()}
#     data.code = data.code.map(reversed_code_mapper)
#     # fomat data
#     if date is None:
#         date = pd.Timestamp(pd.Timestamp.today().date())
#     data["datetime"] = date
#     return data.set_index(["datetime", "code"])


# def _get_shares(codes: str | List[str], start_date, end_date, fields=None):
#     # args map
#     if isinstance(codes, str):
#         codes = [codes]
#     code_mapper = get_code_mapper(codes)
#     rq_codes = [code_mapper.get(code, code) for code in codes]
#     FIELDS_MAPPER = {
#         "total": "total_shares",
#         "circulation_a": "circulating_shares",
#         "free_circulation": "free_circulating_shares",
#         "non_circulation_a": "non_circulating_shares",
#         "total_a": "total_a_shares",
#         "date": "datetime",
#     }
#     FIELDS_MAPPER.update(FIELDS_RQ2XQ)
#     FIELDS_MAPPER2RQ = {v: k for k, v in FIELDS_MAPPER.items()}
#     if fields is not None:
#         if isinstance(fields, str):
#             fields = [fields]
#         rq_feields = [FIELDS_MAPPER2RQ.get(field, field) for field in fields]
#     else:
#         rq_feields = None
#     # get_data
#     data: pd.DataFrame = rq.get_shares(
#         rq_codes, start_date, end_date, fields=rq_feields
#     ).reset_index()
#     # map column names
#     data.rename(columns=FIELDS_MAPPER, inplace=True)
#     # map codes
#     reversed_code_mapper = {v: k for k, v in code_mapper.items()}
#     data.code = data.code.map(reversed_code_mapper)
#     # fomat data
#     return data.set_index(["datetime", "code"])


# def _get_factor_exposure(
#     codes: str | List[str],
#     start_date,
#     end_date,
#     factors=None,
#     industry_mapping="citics2019",
#     model="v1",
# ):
#     # args map
#     if isinstance(codes, str):
#         codes = [codes]
#     code_mapper = get_code_mapper(codes)
#     rq_codes = [code_mapper.get(code, code) for code in codes]
#     # get_data
#     data: pd.DataFrame = rq.get_factor_exposure(
#         rq_codes,
#         start_date,
#         end_date,
#         factors=factors,
#         industry_mapping=industry_mapping,
#         model=model,
#     )
#     if data is None:
#         return None
#     if data.empty:
#         return None
#     else:
#         data = data.reset_index()
#     # map column names
#     data.rename(columns=FIELDS_RQ2XQ, inplace=True)
#     # map codes
#     reversed_code_mapper = {v: k for k, v in code_mapper.items()}
#     data.code = data.code.map(reversed_code_mapper)
#     # fomat data
#     return data.set_index(["datetime", "code"])


# def _index_weights_ex(code: str, start_date=None, end_date=None):
#     # args map
#     code_mapper = get_code_mapper(code)
#     rq_code = code_mapper.get(code, code)

#     # get_data
#     data: pd.DataFrame = rq.index_weights_ex(
#         order_book_id=rq_code, start_date=start_date, end_date=end_date
#     )
#     if data is None:
#         return None
#     if data.empty:
#         return None
#     else:
#         data = data.reset_index()
#     # map column names
#     data.rename(columns=FIELDS_RQ2XQ, inplace=True)
#     # map constituent codes
#     data.code = convert_code(data.code)
#     # fomat data
#     return data.set_index(["datetime", "code"])
