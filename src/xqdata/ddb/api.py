from contextlib import contextmanager
from typing import Generator, List

import dolphindb as ddb
import pandas as pd
from ddbtools import BaseCRUD, Comparator, DBDf, Filter

from xqdata.dataapi import DataApi

from .utils import (
    add_attribute as _add_attribute,
)
from .utils import (
    load_attributes_config,
)
from .utils import (
    sync_data as _sync_data,
)
from .utils import (
    upsert_attribute as _upsert_attribute,
)


class DDBDataApi(DataApi):
    def auth(self, host, port, userid, password) -> None:
        self.host = host
        self.port = port
        self.userid = userid
        self.password = password
        try:
            session = ddb.Session(
                host=host, port=port, userid=userid, password=password
            )
            session.close()
        except Exception:
            raise Exception("auth failed")

    @property
    @contextmanager
    def session(self) -> Generator[ddb.Session, None, None]:
        session = ddb.Session(self.host, self.port, self.userid, self.password)
        try:
            yield session
        finally:
            session.close()

    def get_info(self, type, **kwargs):
        crud = BaseCRUD("dfs://infos", type.lower())
        with self.session as session:
            return crud.get(session=session)

    def get_attributes(
        self,
        attributes: str | List[str],
        codes: List = None,
        dual_key=False,
        objects: List = None,
        start_time=None,
        end_time=None,
        types: str | List[str] = None,
        frequency: str = "D",
        value_filter: Filter = None,
        panel=True,
    ) -> pd.DataFrame:
        with self.session as session:
            attr_info = load_attributes_config(session)

            if isinstance(attributes, str):
                attributes = [attributes]
            f_attr_info = attr_info[attr_info["attribute"].isin(attributes)]

            if dual_key:
                f_attr_info = f_attr_info[f_attr_info["obj_dtype"] != ""]
            else:
                f_attr_info = f_attr_info[f_attr_info["obj_dtype"] == ""]

            if types is not None:
                if isinstance(types, str):
                    types = [types]
                types = [t.lower() for t in types]
                f_attr_info = f_attr_info[f_attr_info["type"].isin(types)]

            f_attr_info = f_attr_info[f_attr_info["frequency"] == frequency.lower()]

            start_time = pd.Timestamp(start_time) if start_time else None
            end_time = pd.Timestamp(end_time) if end_time else None

            general_conds = []
            if codes is not None:
                if len(codes) == 1:
                    general_conds.append(Filter("code", Comparator.eq, codes[0]))
                else:
                    general_conds.append(Filter("code", Comparator.isin, codes))
            if objects is not None and dual_key:
                general_conds.append(Filter("object", Comparator.isin, objects))
            if start_time:
                general_conds.append(Filter("datetime", Comparator.gt, start_time))
            if end_time:
                general_conds.append(Filter("datetime", Comparator.lt, end_time))
            if value_filter:
                general_conds.append(value_filter)

            results = []
            for key, attrsubdf in f_attr_info.groupby(["db_name", "table_name"]):
                db_name = key[0]
                table_name = key[1]
                conds = [
                    Filter("attribute", Comparator.isin, attrsubdf.attribute.to_list()),
                ]
                conds.extend(general_conds)
                crud = BaseCRUD(db_path=db_name, table_name=table_name)
                res = crud.get(session=session, conds=conds, panel=False)
                if not res.empty:
                    results.append(res)
            if len(results) > 0:
                result_df = pd.concat(results)
                if not panel:
                    return result_df
                else:
                    index = ["datetime", "code"]
                    if dual_key:
                        index.append("object")
                    return result_df.pivot(
                        index=index, columns="attribute", values="value"
                    )
            else:
                return pd.DataFrame()

    def get_factor(
        self, factors, codes, start_time=None, end_time=None, frequency="D", panel=True
    ):
        if isinstance(codes, str):
            codes = [codes]
        data = self.get_attributes(
            attributes=factors,
            codes=codes,
            start_time=start_time,
            end_time=end_time,
            types=None,
            frequency=frequency,
            panel=panel,
        )
        if not panel:
            # data.columns.name = "factorname"
            # data = data.stack().to_frame()
            # data.columns = ["value"]
            data.rename(columns={"attribute": "factorname"}, inplace=True)
        return data

    def get_dualkey_factor(
        self,
        factors,
        codes,
        objects=None,
        start_time=None,
        end_time=None,
        frequency="D",
        panel=True,
    ):
        if isinstance(codes, (str, int)):
            codes = [codes]
        if isinstance(objects, (str, int)):
            objects = [objects]
        data = self.get_attributes(
            attributes=factors,
            codes=codes,
            dual_key=True,
            objects=objects,
            start_time=start_time,
            end_time=end_time,
            types=None,
            frequency=frequency,
            panel=panel,
        )
        if not panel:
            data.rename(columns={"attribute": "factorname"}, inplace=True)
        return data

    def upsert_info(
        self, table_name: str, key_cols: List[str], new_info_data: pd.DataFrame
    ):
        """
        插入或更新info表
        """
        if isinstance(key_cols, str):
            key_cols = [key_cols]
        crud = BaseCRUD("dfs://infos", table_name)
        crud.key_cols = key_cols
        with self.session as session:
            data = DBDf(session, "dfs://infos", table_name, new_info_data)
            crud.upsert(session=session, data=data)
            return True

    def sync_data(
        self,
        external_api: DataApi,
        type: str,
        factors: str | List[str],
        codes: str | List[str],
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        freq: str = "D",
        objects: List[str] = None,
    ):
        with self.session as session:
            _sync_data(
                session=session,
                external_api=external_api,
                type=type,
                factors=factors,
                codes=codes,
                start_time=start_time,
                end_time=end_time,
                freq=freq,
                objects=objects,
            )

    def upsert_attribute(
        self,
        type: str,
        data: pd.DataFrame,
        attributes: List[str] | None = None,
        frequency: str = "D",
    ):
        with self.session as session:
            _upsert_attribute(
                session=session,
                type=type,
                data=data,
                attributes=attributes,
                frequency=frequency,
            )

    def add_attribute(
        self,
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
        with self.session as session:
            _add_attribute(
                session=session,
                type=type,
                attribute=attribute,
                name=name,
                frequency=frequency,
                dtype=dtype,
                code_dtype=code_dtype,
                obj_dtype=obj_dtype,
                db_name=db_name,
                table_name=table_name,
            )


instance = DDBDataApi()
