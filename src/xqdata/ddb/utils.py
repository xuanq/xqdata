from functools import lru_cache
from typing import List

import pandas as pd
from ddbtools import BaseCRUD, DbColumn, DBDf, create_db, create_table
from dolphindb import Session

from xqdata.dataapi import DataApi


def init_db(session: Session):
    if not session.existsDatabase("dfs://infos"):
        create_db(session, "dfs://infos", "RANGE(2000.01M + (0..30)*12)", "OLAP")

    if not session.existsTable("dfs://infos", "attribute"):
        attribute_info_cols = [
            DbColumn("type", "SYMBOL", "类别"),
            DbColumn("frequency", "SYMBOL", "数据频率"),
            DbColumn("attribute", "SYMBOL", "属性名"),
            DbColumn("name", "SYMBOL", "属性注释"),
            DbColumn("dtype", "SYMBOL", "数据类型"),
            DbColumn("code_dtype", "SYMBOL", "索引列类型"),
            DbColumn("obj_dtype", "SYMBOL", "对象列类型"),
            DbColumn("db_name", "SYMBOL", "数据库名"),
            DbColumn("table_name", "SYMBOL", "表名"),
        ]
        create_table(
            session=session,
            db_name="dfs://infos",
            table_name="attribute",
            columns=attribute_info_cols,
        )


@lru_cache(maxsize=1)
def load_attributes_config(session: Session):
    """
    加载db中attributes配置
    """
    crud = BaseCRUD("dfs://infos", "attribute")
    return crud.get(session)


def add_object(
    session: Session,
    obj_name: str,
    attribute_frequency: str | List[str] | None = None,
    code_dtype: str = "SYMBOL",
    atrribute_dtypes: str | List[str] = ["SYMBOL", "STRING", "DOUBLE", "INT", "BOOL"],
    obj_dtype: str = None,
):
    """
    在dolphindb中创建对象
    """
    if isinstance(atrribute_dtypes, str):
        atrribute_dtypes = [atrribute_dtypes]

    if not session.existsDatabase("dfs://infos"):
        create_db(session, "dfs://infos", "RANGE(2000.01M + (0..30)*12)", "OLAP")

    if attribute_frequency is not None:
        if isinstance(attribute_frequency, str):
            attribute_frequency = [attribute_frequency]

        for frequency in attribute_frequency:
            db_path = f"dfs://{obj_name}_{frequency.lower()}"
            if not session.existsDatabase(db_path):
                if frequency.lower() == "d":
                    partition_plan = (
                        "RANGE(date(datetimeAdd(1990.01M,0..80*12,'M'))), VALUE(`f1`f2)"
                    )
                elif frequency.lower() == "min":
                    partition_plan = "VALUE(2020.01.01..2021.01.01), VALUE(`f1`f2)"
                else:
                    raise ValueError(f"unsupported frequency: {frequency}")
                create_db(session, db_path, partition_plan, "TSDB")

            for dtype in atrribute_dtypes:
                if not session.existsTable(db_path, f"attr_{dtype.lower()}"):
                    _create_attr_tables(
                        session=session,
                        db_path=db_path,
                        dtype=dtype,
                        code_dtype=code_dtype,
                        frequency=frequency,
                        obj_dtype=obj_dtype,
                    )


def _create_attr_tables(
    session: Session,
    db_path: str,
    dtype: str,
    code_dtype: str = "SYMBOL",
    frequency: str = "D",
    obj_dtype=None,
):
    table_name = "attr_" + dtype.lower()

    if frequency.lower() == "d":
        dt_dtype = "DATE"
    elif frequency.lower() == "min":
        dt_dtype = "DATETIME"
    else:
        raise ValueError(f"unsupported frequency: {frequency}")

    cols = [
        DbColumn(name="datetime", dtype=dt_dtype, compress="delta"),
        DbColumn(name="code", dtype=code_dtype),
        DbColumn(name="attribute", dtype="SYMBOL"),
        DbColumn(name="value", dtype=dtype),
    ]

    sortColumns = "`code,`datetime"
    sortKeyMappingFunction = "hashBucket{, 500}"

    if obj_dtype is not None:
        cols.insert(2, DbColumn(name="object", dtype=obj_dtype))
        sortColumns = "`code,`object,`datetime"
        sortKeyMappingFunction = "hashBucket{, 25},hashBucket{, 25}"

    create_table(
        session,
        db_path,
        table_name,
        cols,
        partition_by="datetime, attribute",
        sortColumns=sortColumns,
        keepDuplicates="ALL",
        sortKeyMappingFunction=sortKeyMappingFunction,
    )


def add_attribute(
    session: Session,
    type: str,
    attribute: str,
    name: str = None,
    frequency="D",
    dtype="DOUBLE",
    code_dtype="SYMBOL",
    obj_dtype=None,
    db_name=None,
    table_name=None,
):
    if db_name is None:
        db_name = f"dfs://{type}_{frequency.lower()}"
    if table_name is None:
        table_name = "attr_" + dtype.lower()

    crud = BaseCRUD("dfs://infos", "attribute")
    crud.key_cols = ["type", "frequency", "attribute"]
    crud.upsert(
        session=session,
        data=pd.Series(
            {
                "type": type,
                "frequency": frequency.lower(),
                "attribute": attribute,
                "name": name,
                "dtype": dtype,
                "code_dtype": code_dtype,
                "obj_dtype": obj_dtype,
                "db_name": db_name,
                "table_name": table_name,
            }
        )
        .to_frame()
        .T,
    )


def upsert_attribute(
    session: Session,
    type: str,
    data: pd.DataFrame,
    attributes: List[str] | None = None,
    frequency: str = "D",
):
    # assuming data is a panel-dataframe with index must be [datetime,code] or [datetime,code,object]
    attribute_info = load_attributes_config(session)
    if attributes is None:
        attributes = data.columns

    for attribute in attributes:
        attrinfo = attribute_info.query(
            f"type == '{type}' and attribute == '{attribute}' and frequency == '{frequency.lower()}'"
        )
        if attrinfo.empty:
            Warning(f"attribute {attribute} not found")
            continue
        elif len(attrinfo) > 1:
            Warning(
                f"attribute {attribute} is not unique,please check dfs://infos attribute table"
            )
            continue

        db_name = attrinfo.iloc[0]["db_name"]
        table_name = attrinfo.iloc[0]["table_name"]
        atttribute_data = data[[attribute]].stack().reset_index(level=-1)
        atttribute_data.columns = ["attribute", "value"]

        crud = BaseCRUD(db_name, table_name)
        crud.key_cols = data.index.names
        d = DBDf(
            session=session,
            db_path=db_name,
            table_name=table_name,
            data=atttribute_data,
        )
        crud.upsert(session=session, data=d)


def sync_data(
    session: Session,
    external_api: DataApi,
    type: str,
    factors: str | List[str],
    codes: str | List[str],
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    freq: str = "D",
    objects: str | List[str] = None,
):
    if isinstance(factors, str):
        factors = [factors]
    if isinstance(codes, str):
        codes = [codes]
    if objects is not None:
        if isinstance(objects, str):
            objects = [objects]
        factor_df = external_api.get_dualkey_factor(
            codes=codes,
            factors=factors,
            objects=objects,
            start_time=start_time,
            end_time=end_time,
            frequency=freq,
        )
    else:
        factor_df = external_api.get_factor(
            codes=codes,
            factors=factors,
            start_time=start_time,
            end_time=end_time,
            frequency=freq,
        )
    upsert_attribute(
        session=session, type=type, data=factor_df, attributes=factors, frequency=freq
    )
    return True
