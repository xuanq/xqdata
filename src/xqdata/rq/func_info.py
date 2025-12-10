import pandas as pd
import rqdatac as rq

from .utils import rename_columns


def rq_get_trading_dates(start_date=None, end_date=None, market="cn"):
    if start_date is None:
        start_date = "1990-01-01"
    if end_date is None:
        end_date = "2100-01-01"
    data = rq.get_trading_dates(start_date, end_date, market=market)
    data = pd.to_datetime(data)
    data.name = "datetime"
    tradeday_df = (
        pd.DataFrame(data=True, index=data, columns=["is_tradeday"])
        .resample("D")
        .last()
        .fillna(0)
    )
    return tradeday_df


def rq_all_instruments(
    type: str,
    market: str = "cn",
    date=None,
):
    data = rq.all_instruments(type=type, date=date, market=market)
    # map column names
    rename_columns(data)
    # map date
    data["listed_date"] = data["listed_date"].replace("0000-00-00", "1990-01-01")
    data["listed_date"] = data["listed_date"].replace("2999-12-31", "2100-12-31")
    data["de_listed_date"] = data["de_listed_date"].replace("0000-00-00", "2100-01-01")
    # change dtype
    data["listed_date"] = pd.to_datetime(data["listed_date"])
    data["de_listed_date"] = pd.to_datetime(data["de_listed_date"])
    return data
