import rqdatac as rq
from .func import rename_columns

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
