import re
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
    **kwargs,
) -> pd.DataFrame:
    # Ensure factors is a list
    if isinstance(factors, str):
        factors = [factors]
    if isinstance(codes, str):
        codes = [codes]

    # Define all valid price fields
    valid_price_fields = (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trading_date",
        "last",
        "prev_close",
        "total_turnover",
        "limit_up",
        "limit_down",
        "a1",
        "a2",
        "a3",
        "a4",
        "a5",
        "b1",
        "b2",
        "b3",
        "b4",
        "b5",
        "a1_v",
        "a2_v",
        "a3_v",
        "a4_v",
        "a5_v",
        "b1_v",
        "b2_v",
        "b3_v",
        "b4_v",
        "b5_v",
        "change_rate",
        "num_trades",
        "open_interest",
        "prev_settlement",
    )

    # Initialize result dataframe
    data = pd.DataFrame()

    # Process regular price factors (without adjustment)
    price_factors = [f for f in factors if f in valid_price_fields]

    if price_factors:
        # Get data for regular price factors
        price_data = _get_price_internal(
            codes,
            start_time,
            end_time,
            frequency=frequency,
            fields=price_factors,
            adjust_type="none",
            **kwargs,
        )
        if not price_data.empty:
            # Merge with result data
            if data.empty:
                data = price_data
            else:
                data = data.merge(price_data, on=["datetime", "code"], how="outer")

    # Process adjusted factors (post and pre)
    for adjust_type in ("post", "pre"):
        adjust_factors = [f for f in factors if f.endswith(f"_{adjust_type}")]
        # Extract base field names (e.g., "open" from "open_post")
        fields = [f.split(f"_{adjust_type}")[0] for f in adjust_factors]

        if adjust_factors:
            # Get data for adjusted price factors
            price_data = _get_price_internal(
                codes,
                start_time,
                end_time,
                frequency=frequency,
                fields=fields,
                adjust_type=adjust_type,
                **kwargs,
            )

            if not price_data.empty:
                # Add suffix to match requested factor names
                price_data = price_data.add_suffix(f"_{adjust_type}")
                # Rename datetime and code columns back (they shouldn't have suffix)
                price_data.rename(
                    columns={
                        f"datetime_{adjust_type}": "datetime",
                        f"code_{adjust_type}": "code",
                    },
                    inplace=True,
                )

                # Select only the requested adjusted factors
                price_data = price_data[["datetime", "code"] + adjust_factors]

                # Merge with result data
                if data.empty:
                    data = price_data
                else:
                    data = data.merge(price_data, on=["datetime", "code"], how="outer")

    if data.empty:
        return data

    return data.set_index(["datetime", "code"])


def _get_price_internal(
    codes: Union[str, List[str]],
    start_time: Optional[Union[str, datetime, date]],
    end_time: Optional[Union[str, datetime, date]],
    frequency: str,
    fields: List[str],
    adjust_type: str,
    **kwargs,
) -> pd.DataFrame:
    """Internal helper function to get price data from RQData"""
    if frequency != "tick":
        if frequency.lower() == "min":
            frequency = "m"
        frequency = "1" + frequency.lower()
    if isinstance(start_time, str):
        start_time = pd.Timestamp(start_time)
    if isinstance(end_time, str):
        end_time = pd.Timestamp(end_time)

    # Special handling for futures night trading
    true_end_date = end_time
    if any(unit in frequency.lower() for unit in {"ms", "s", "min", "h"}):
        if end_time and end_time.hour >= 21:
            true_end_date = rq.get_next_trading_date(end_time)

    # get_data
    data: pd.DataFrame = rq.get_price(
        order_book_ids=codes,
        start_date=start_time,
        end_date=true_end_date,
        frequency=frequency,
        fields=fields,
        adjust_type=adjust_type,
        **kwargs,
    )

    if data is None or data.empty:
        return pd.DataFrame()

    data = data.reset_index()
    # map column names
    rename_columns(data)

    # Filter by actual date range
    if start_time and end_time:
        data = data[(data.datetime >= start_time) & (data.datetime <= end_time)]

    return data


def rq_get_factor(
    factors: Union[str, List[str]],
    codes: Union[str, List[str]],
    start_time: Optional[Union[str, datetime, date]] = None,
    end_time: Optional[Union[str, datetime, date]] = None,
    frequency: str = "D",
    **kwargs,
):
    # args map
    if isinstance(codes, str):
        codes = [codes]
    if isinstance(factors, str):
        factors = [factors]

    # Convert datetime objects to strings if needed
    if isinstance(start_time, (datetime, date)):
        start_time = start_time.strftime("%Y-%m-%d")
    if isinstance(end_time, (datetime, date)):
        end_time = end_time.strftime("%Y-%m-%d")

    # get_data
    data: pd.DataFrame = rq.get_factor(
        order_book_ids=codes,
        factor=factors,
        start_date=start_time,
        end_date=end_time,
        expect_df=True,
        **kwargs,
    )

    if data is None or data.empty:
        return pd.DataFrame()
    else:
        data = data.reset_index()

    # map column names
    rename_columns(data)

    return data.set_index(["datetime", "code"])


def rq_is_suspended(
    factors: Union[str, List[str]],
    codes: Union[str, List[str]],
    start_time: Optional[Union[str, datetime, date]] = None,
    end_time: Optional[Union[str, datetime, date]] = None,
    frequency: str = "D",
    **kwargs,
):
    # args map
    if isinstance(codes, str):
        codes = [codes]
    # get_data
    data: pd.DataFrame = rq.is_suspended(
        order_book_ids=codes, start_date=start_time, end_date=end_time, **kwargs
    )
    data = data.stack()
    data.index.names = ["datetime", "code"]
    data.name = "is_paused"
    data = data.reset_index()
    return data.set_index(["datetime", "code"])


def rq_is_st_stock(
    factors: Union[str, List[str]],
    codes: Union[str, List[str]],
    start_time: Optional[Union[str, datetime, date]] = None,
    end_time: Optional[Union[str, datetime, date]] = None,
    frequency: str = "D",
    **kwargs,
):
    # args map
    if isinstance(codes, str):
        codes = [codes]
    data: pd.DataFrame = rq.is_st_stock(
        codes,
        start_time,
        end_time,
        **kwargs,
    )
    data = data.stack()
    data.index.names = ["datetime", "code"]
    data.name = "is_st"
    data = data.reset_index()
    return data.set_index(["datetime", "code"])


def rq_get_instrument_industry(
    factors: Union[str, List[str]],
    codes: Union[str, List[str]],
    start_time: Optional[Union[str, datetime, date]] = None,
    end_time: Optional[Union[str, datetime, date]] = None,
    frequency: str = "D",
    **kwargs,
):
    # args map
    if isinstance(codes, str):
        codes = [codes]
    if isinstance(factors, str):
        factors = [factors]

    # Pattern to match industry factors: (citics|gildata|citics_2019)_l(level)
    pattern = r"^(citics|citics_2019|sws|hsi)_l(\d)"

    # Extract industry and level from factors
    query_industry = [
        (re.findall(pattern, f)[0][0], int(re.findall(pattern, f)[0][1]))
        for f in factors
        if len(re.findall(pattern, f)) > 0
    ]

    # If no valid industry factors found, return empty DataFrame
    if len(query_industry) == 0:
        return pd.DataFrame()

    query_industry = set(query_industry)  # Remove duplicates
    # Collect data for each industry and level combination
    temp_df = []
    for industry, level in query_industry:
        temp_df_per_day = []
        # For each date in the date range, get industry data
        date_range = rq.get_trading_dates(start_time, end_time)
        for d in date_range:
            try:
                # get_data
                data: pd.DataFrame = rq.get_instrument_industry(
                    codes,
                    source=industry,
                    level=level,
                    date=d,
                    **kwargs,
                ).reset_index()
                # Add datetime column
                data["datetime"] = d
                temp_df_per_day.append(data)
            except Exception:
                # Skip dates where data is not available
                continue
        # map column names
        MAPPER = {
            "first_industry_code": f"{industry}_l1",
            "second_industry_code": f"{industry}_l2",
            "third_industry_code": f"{industry}_l3",
            "first_industry_name": f"{industry}_l1_name",
            "second_industry_name": f"{industry}_l2_name",
            "third_industry_name": f"{industry}_l3_name",
            "order_book_id": "code",
        }
        temp_df.append(
            pd.concat(temp_df_per_day, axis=0)  # 每天的数据合并
            .rename(
                columns=MAPPER
            )  # 重命名order_book_id->code和first_industry_code->citics_l1等
            .set_index(["datetime", "code"])
        )

    # If no data collected, return empty DataFrame
    if len(temp_df) == 0:
        return pd.DataFrame()

    # Concatenate all data
    result_df = pd.concat(temp_df, axis=1)
    cols = result_df.columns.intersection(factors)
    return result_df[cols]


def rq_get_factor_exposure(
    factors: Union[str, List[str]],
    codes: Union[str, List[str]],
    start_time: Optional[Union[str, datetime, date]] = None,
    end_time: Optional[Union[str, datetime, date]] = None,
    frequency: str = "D",
    **kwargs,
):
    # args map
    if isinstance(codes, str):
        codes = [codes]
    # get_data
    data: pd.DataFrame = rq.get_factor_exposure(
        order_book_ids=codes,
        start_date=start_time,
        end_date=end_time,
        factors=factors,
        **kwargs,
    )
    if data is None or data.empty:
        return pd.DataFrame()
    else:
        data = data.reset_index()
    # map column names
    rename_columns(data)
    # fomat data
    return data.set_index(["datetime", "code"])


def rq_get_shares(
    factors: Union[str, List[str]],
    codes: Union[str, List[str]],
    start_time: Optional[Union[str, datetime, date]] = None,
    end_time: Optional[Union[str, datetime, date]] = None,
    frequency: str = "D",
    **kwargs,
):
    # args map
    if isinstance(codes, str):
        codes = [codes]
    # get_data
    data: pd.DataFrame = rq.get_shares(
        codes, start_date=start_time, end_date=end_time, fields=factors, **kwargs
    ).reset_index()
    # map column names
    rename_columns(data)
    # fomat data
    return data.set_index(["datetime", "code"])


def rq_index_weights_ex(
    factors: Union[str, List[str]],
    codes: Union[str, List[str]],
    objects: Union[str, List[str]] = None,
    start_time: Optional[Union[str, datetime, date]] = None,
    end_time: Optional[Union[str, datetime, date]] = None,
    frequency: str = "D",
    **kwargs,
):
    if isinstance(codes, str):
        codes = [codes]

    # get_data
    def get_single_data(code):
        data: pd.DataFrame = rq.index_weights_ex(
            order_book_id=code, start_date=start_time, end_date=end_time, **kwargs
        ).reset_index()
        data["code"] = code
        return data

    data: pd.DataFrame = pd.concat([get_single_data(code) for code in codes], axis=0)
    data.rename(
        columns={
            "order_book_id": "object",
            "date": "datetime",
            "weight": "constituent_weight",
        },
        inplace=True,
    )

    # fomat data
    return data.set_index(["datetime", "code", "object"])
