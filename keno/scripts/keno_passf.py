import pathlib
from typing import *

import pandas as pd


def concat_csv(
    filepaths: List[str],
    sep: str,
    names: Optional[List[str]] = None,
) -> pd.DataFrame:
    has_header = None if names is not None else True

    dfs = (
        pd.read_csv(filepath, sep=sep, names=names, header=has_header)
        for filepath in filepaths
    )
    return pd.concat(dfs)


def process_keno_split_data(dirpath: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    get_paths = lambda glob: list(
        sorted(map(lambda x: str(x), pathlib.Path(dirpath).glob(glob)))
    )

    wagers_paths = get_paths("split/*wager*")
    wagers_names = "begin_draw;end_draw;qp;ticket_cost;numbers_wagered".split(";")
    wagers = concat_csv(
        wagers_paths,
        sep=";",
        names=wagers_names,
    )

    drawings_paths = get_paths("split/*draw*")
    drawings_names = "Draw Nbr;Draw Date;Winning Number String".split(";")
    drawings = concat_csv(
        drawings_paths,
        sep=";",
        names=drawings_names,
    )

    return wagers, drawings


wagers, drawings = process_keno_split_data("keno/data/keno_2017_2019")
