import rqdatac as rq

from .func_factor import rq_get_factor, rq_get_price
from .utils import rename_columns

# RQData API配置
INFO_CONFIG = {
    "stock": {
        "rq_func": rq.all_instruments,
        "rq_params": {"type": "CS"},
        "post_process": rename_columns,
        "post_process_args": {},
    },
    "fund": {
        "rq_func": rq.all_instruments,
        "rq_params": {"type": "FUND"},
        "post_process": rename_columns,
        "post_process_args": {},
    },
    "futures": {
        "rq_func": rq.all_instruments,
        "rq_params": {"type": "Future"},
        "post_process": rename_columns,
        "post_process_args": {},
    },
    "option": {
        "rq_func": rq.all_instruments,
        "rq_params": {"type": "Option"},
        "post_process": rename_columns,
        "post_process_args": {},
    },
    "convertible": {
        "rq_func": rq.all_instruments,
        "rq_params": {"type": "Convertible"},
        "post_process": rename_columns,
        "post_process_args": {},
    },
    "etf": {
        "rq_func": rq.all_instruments,
        "rq_params": {"type": "ETF"},
        "post_process": rename_columns,
        "post_process_args": {},
    },
    "lof": {
        "rq_func": rq.all_instruments,
        "rq_params": {"type": "LOF"},
        "post_process": rename_columns,
        "post_process_args": {},
    },
}

# Factor配置
FACTOR_CONFIG = {
    "default": rq_get_factor,
    "pe_ratio": rq_get_factor,
    "pb_ratio": rq_get_factor,
    "ps_ratio": rq_get_factor,
    # 行情因子
    "open": rq_get_price,
    "high": rq_get_price,
    "low": rq_get_price,
    "close": rq_get_price,
    "volume": rq_get_price,
    "amount": rq_get_price,
    "open_post": rq_get_price,
    "high_post": rq_get_price,
    "low_post": rq_get_price,
    "close_post": rq_get_price,
    "volume_post": rq_get_price,
    "amount_post": rq_get_price,
}
