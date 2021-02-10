from typing import *

import sqlalchemy as sqla


def create_sqla_engine_str(
    username: str, password: str, host: str, port: str, database: Optional[str] = None
) -> sqla.engine.base.Engine:
    s = f"mysql+pymysql://{username}:{password}@{host}:{port}"
    s += f"/{database}" if database is not None else ""
    return s