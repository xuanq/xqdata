import pandas as pd


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    重命名DataFrame的列，将order_book_id重命名为code
    """
    if "order_book_id" in df.columns:
        df.rename(columns={"order_book_id": "code"}, inplace=True)
    if "date" in df.columns:
        df.rename(columns={"date": "datetime"}, inplace=True)
    return df
