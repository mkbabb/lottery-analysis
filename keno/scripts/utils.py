import os
import tempfile
from typing import *

import pandas as pd
import sqlalchemy as sqla


def create_sqla_engine_str(
    username: str, password: str, host: str, port: str, database: Optional[str] = None
) -> sqla.engine.base.Engine:
    s = f"mysql+pymysql://{username}:{password}@{host}:{port}"
    s += f"/{database}" if database is not None else ""
    return s


def read_sql_tmpfile(
    sql: str, con: sqla.engine.Connection, *args, **kwargs
) -> pd.DataFrame:
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "mycsv")

        curr = con.execute(sql + " limit 0")
        cols = ",".join((f"'{i}'" for i in curr.keys()))

        to_csv = f"""
            SELECT * FROM (SELECT {cols} UNION ALL {sql}) as tmp
            INTO OUTFILE '{path}'
            FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\n';
            """
        con.execute(to_csv)

        return pd.read_csv(path, *args, **kwargs)


def read_sql_table_tmpfile(
    table_name: str, con: sqla.engine.Connection, *args, **kwargs
) -> pd.DataFrame:
    return read_sql_tmpfile(f"select * from {table_name}", con=con, *args, **kwargs)
