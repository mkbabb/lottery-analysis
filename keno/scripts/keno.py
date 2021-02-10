import argparse
import bisect
import contextlib
import json
import pathlib
from datetime import datetime, timedelta
from typing import *
from numpy import number
from functools import reduce

import pandas as pd
import sqlalchemy as sqla

from bit_manipulations import bits_to_nums, nums_to_bits, popcount64d
from schemas import *
from utils import create_sqla_engine_str

MAX_BITS = 63
MAX_NUMBERS = 80 + 1

PRIZE_DICT: Dict[int, Dict[int, int]] = {
    10: {10: 100000, 9: 4250, 8: 450, 7: 40, 6: 15, 5: 2, 0: 5},
    9: {9: 30000, 8: 3000, 7: 150, 6: 25, 5: 6, 4: 1},
    8: {8: 10000, 7: 750, 6: 50, 5: 12, 4: 2},
    7: {7: 4500, 6: 100, 5: 17, 4: 3, 3: 1},
    6: {6: 1100, 5: 50, 4: 8, 3: 1},
    5: {5: 420, 4: 18, 3: 2},
    4: {4: 75, 3: 5, 2: 1},
    3: {3: 27, 2: 2},
    2: {2: 11},
    1: {1: 2},
}


class KenoTime:
    def __init__(self, start_date: datetime, end_date: datetime, delta: timedelta):
        self.start_date = start_date
        self.end_date = end_date
        self.delta = delta
        # Shift by one day, and add (inclusive range).
        self.intervals = ((end_date + timedelta(days=1)) - start_date) // delta + 1


def normalize_draw_dates(dates: pd.Series) -> Callable[[int], str]:
    KENO_TIMES = [
        KenoTime(
            datetime.fromisoformat("1970-01-01T05:05"),
            datetime.fromisoformat("1970-01-01T01:45"),
            timedelta(minutes=5),
        ),
        KenoTime(
            datetime.fromisoformat("2020-01-01T05:05"),
            datetime.fromisoformat("2020-01-01T01:45"),
            timedelta(minutes=4),
        ),
    ]
    START_DATES = list(map(lambda x: x.start_date, KENO_TIMES))
    # Each day's worth of draws should equal exactly
    # to the number of KenoTime intervals (249 up until 2020).
    # If this isn't the case, then that day is malformed in some way.
    offsets = dates.value_counts().to_dict()

    prev_date: Optional[datetime] = None

    def calc_dates(time: int) -> str:
        nonlocal prev_date
        date = datetime.strptime(str(time), "%Y%m%d")

        ix = bisect.bisect_left(START_DATES, date) - 1
        keno_time = KENO_TIMES[ix]

        offset = keno_time.intervals - offsets[time]

        if offset > 0:
            prev_date = None
            return date.isoformat()
        else:
            if prev_date is None or prev_date.time() == keno_time.end_date.time():
                start_date = keno_time.start_date
                prev_date = date.replace(hour=start_date.hour, minute=start_date.minute)
            else:
                prev_date += keno_time.delta

            return prev_date.isoformat()

    return calc_dates


def get_bit_info(number_string: str) -> Tuple[int, int]:
    bit_info = nums_to_bits(
        number_string,
        bit_length=MAX_BITS,
        max_num=MAX_NUMBERS,
        num_length=2,
    )
    return (bit_info[0], bit_info[1])


def get_number_string(bit_info: List[int]) -> str:
    return bits_to_nums(bit_info, delim=",", bit_length=MAX_BITS)


def process_drawings(drawings: pd.DataFrame) -> pd.DataFrame:
    """Initial pre-processing of the drawings DataFrame.

    Processes the lottery numbers into their corresponding
    bit and integer array counterparts.
    Thereinafter, the dates are formatted (by accumulation)
    into evenly spaced intervals of 5, 24.

    @param drawings: DataFrame containing keno drawings data.

    @returns drawings: modified 'drawings' DataFrame.
    """

    drawings = (
        drawings.rename(
            columns={
                "Draw Nbr": "id",
                "Draw Date": "date",
                "Winning Number String": "number_string",
            }
        )
        .set_index("id")
        .assign(low_bits=0, high_bits=0)
    )

    normalize_draw_dates_func = normalize_draw_dates(drawings["date"])

    def process(row: pd.Series) -> pd.Series:
        low_bits, high_bits = get_bit_info(row["number_string"])

        row["number_string"] = get_number_string([low_bits, high_bits])
        row["low_bits"] = low_bits
        row["high_bits"] = high_bits

        row["date"] = normalize_draw_dates_func(row["date"])

        return row

    return drawings.apply(process, axis=1)


def process_wagers(wagers: pd.DataFrame) -> pd.DataFrame:
    def process(row: pd.Series) -> pd.Series:
        low_bits, high_bits = get_bit_info(row["numbers_wagered"])

        row["low_bits"] = low_bits
        row["high_bits"] = high_bits

        row["qp"] = row["qp"] == "T"

        return row

    return (
        wagers.assign(low_bits=0, high_bits=0)
        .apply(process, 1)
        .drop("numbers_wagered", axis=1)
    )


def create_numbers_wagered(
    wagers: pd.DataFrame, numbers_wagered: pd.DataFrame
) -> pd.DataFrame:
    """For the creation of the secondary foreign key table 'numbers_wagered'.
    Allows for once-over preprocessing of unique ticket lottery numbers.
    Equates to a roughly 80% size reduction of the wagers DataFrame.

    Utilizes the same process by which 'process_drawings' processes
    the lottery numbers.

    @param wagers: DataFrame containing keno wagers data.

    @returns number_wagered: new 'number_wagered' DataFrame wherewith the
                    subsequent ticket lottery numbers are stored.
    """
    pk = ["low_bits", "high_bits"]

    def get_number_strings(row: pd.Series) -> pd.Series:
        bit_info = [row[i] for i in pk]
        row["numbers_played"] = sum(map(popcount64d, bit_info))
        row["number_string"] = get_number_string(bit_info)

        return row

    # A number string is the normalized number list;
    # numbers_wagered is what the user selected.
    t_numbers_wagered = (
        wagers[pk]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(numbers_played=0, number_string="")
        .apply(get_number_strings, axis=1)
    )

    if numbers_wagered.empty:
        t_numbers_wagered["id"] = t_numbers_wagered.index
        return t_numbers_wagered
    else:
        dups = reduce(
            lambda x, y: x & y,
            (t_numbers_wagered[i].isin(numbers_wagered[i]) for i in pk),
        )
        t_numbers_wagered = t_numbers_wagered[~dups].reset_index(drop=True)

        if not t_numbers_wagered.empty:
            max_id = numbers_wagered["id"].max()
            t_numbers_wagered["id"] = t_numbers_wagered.index + max_id + 1
            return pd.concat([numbers_wagered, t_numbers_wagered], axis=0)
        else:
            return numbers_wagered


def map_wagers(wagers: pd.DataFrame, numbers_wagered: pd.DataFrame) -> pd.DataFrame:
    pk = ["low_bits", "high_bits"]

    mapped_index = wagers[pk].merge(numbers_wagered, on=pk)
    wagers["numbers_wagered_id"] = mapped_index["id"]

    return wagers.drop(pk, axis=1).reset_index(drop=True)


def find_and_set_winnings(
    wagers: pd.DataFrame, numbers_wagered: pd.DataFrame, drawings: pd.DataFrame
) -> pd.DataFrame:
    """
    Function to find the prize amount of each item in the
    'wagers' DataFrame.

    Most of the work is done within 'calculate_prize' function.

    @param wagers: DataFrame containing keno wagers data.
    @param numbers_wagered: DataFrame containing numbers_wagered data.
    @param drawings: DataFrame containing keno drawings data.

    @returns wagers: modified 'wagers' DataFrame.

    """

    def calculate_prize(
        row: pd.Series, numbers_wagered: pd.DataFrame, drawings: pd.DataFrame
    ) -> pd.Series:
        """
        Function applied to all rows in the 'wagers' DataFrame.
        Utilized normally via pd.apply.

        Principally, this function is responsible for the bit-wise AND'ing of
        two lottery numbers, allowing for fast matching of theretofore
        mentioned numbers.

        @param x: 'numbers_wagered_id' and 'draw_number_id' element of the 'wagers' DataFrame
        @param spots: DataFrame containing spots data.
        @param drawings: DataFrame containing keno drawings data.

        @returns match_mask: array of high and low bits of the match,
                            hamming weight (number of spots played),
                            and date.
        """

        numbers_wagered_id = row["numbers_wagered_id"]
        draw_number_id = row["draw_number_id"]

        high_bits1 = numbers_wagered.loc[numbers_wagered_id, "high_bits"]
        low_bits1 = numbers_wagered.loc[numbers_wagered_id, "low_bits"]
        number_played = numbers_wagered.loc[numbers_wagered_id, "numbers_played"]

        try:
            high_bits2 = drawings.loc[draw_number_id, "high_bits"]
            low_bits2 = drawings.loc[draw_number_id, "low_bits"]

            date = drawings.loc[draw_number_id, "date"]

            match_mask = list(
                map(
                    lambda x: x[0] & x[1],
                    zip([low_bits1, high_bits1], [low_bits2, high_bits2]),
                )
            )
            numbers_matched = sum(map(popcount64d, match_mask))

            try:
                prize = PRIZE_DICT[number_played][numbers_matched]
                match_mask += [numbers_matched, prize, date]
            except KeyError:
                match_mask += [numbers_matched, 0, date]

        except KeyError:
            match_mask = [0, 0, 0, 0, 0]

        d = pd.Series(
            dict(
                zip(
                    [
                        "high_match_mask",
                        "low_match_mask",
                        "numbers_matched",
                        "prize",
                        "date",
                    ],
                    match_mask,
                )
            )
        )
        row = row.append(d)
        return row

    wagers = wagers.apply(
        lambda x: calculate_prize(x, numbers_wagered, drawings),
        axis=1,
        result_type="expand",
    )

    return wagers


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


def apply_multi_index(
    df: pd.DataFrame,
    levels: Union[str, List[str]],
    func: Callable[[pd.MultiIndex], pd.MultiIndex],
) -> pd.MultiIndex:
    return df.index.set_levels(func(df.index.get_level_values(levels)), levels)


def insert_on_duplicate_key_update(
    table: sqla.Table, values: dict, conn: sqla.engine.Connection
) -> Any:
    insert_stmt = table.insert().values(**values)
    # Only supported in SQLA 1.4...
    # on_conflict_stmt = insert_stmt.on_duplicate_key_update(
    #     data=insert_stmt.inserted.data, status="U"
    # )
    try:
        return conn.execute(insert_stmt)
    except sqla.exc.IntegrityError:
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--dirpath", required=True)

    args = parser.parse_args()

    CONFIG = json.load(open(args.config, "r"))
    MYSQL = CONFIG["mysql"]

    def open_mysql_conn() -> sqla.engine.Connection:
        engine_str = create_sqla_engine_str(
            username=MYSQL["username"],
            password=MYSQL["password"],
            host=MYSQL["host"],
            port=MYSQL["port"],
            database=MYSQL["database"],
        )
        engine = sqla.create_engine(engine_str)
        return engine.connect()

    with contextlib.closing(open_mysql_conn()) as conn:
        metadata = sqla.MetaData(bind=conn)
        get_table = lambda x: sqla.Table(x, metadata, autoload=True)

        numbers_wagered_table = get_table("numbers_wagered")
        wagers_table = get_table("wagers")
        drawings_table = get_table("drawings")

        wagers, drawings = process_keno_split_data(dirpath=args.dirpath)
        numbers_wagered = pd.read_sql_table("numbers_wagered", con=conn)

        wagers = wagers[:100]
        drawings = drawings[:100]

        wagers = process_wagers(wagers)
        drawings = process_drawings(drawings)

        numbers_wagered = create_numbers_wagered(wagers, numbers_wagered)

        wagers = map_wagers(wagers, numbers_wagered)

        for n, row in numbers_wagered.iterrows():
            insert_on_duplicate_key_update(numbers_wagered_table, row, conn)
        # numbers_wagered.apply(lambda row: print(row))

        # numbers_wagered.to_sql(
        #     "numbers_wagered", con=conn, if_exists="append", index=False
        # )
        # drawings.to_sql("drawings", con=conn, if_exists="append", index=False)

        # wagers.to_sql("wagers", con=conn, if_exists="append", index=False)

        # wagers = find_and_set_winnings(
        #     wagers=wagers, numbers_wagered=numbers_wagered, drawings=drawings
        # )


if __name__ == "__main__":
    main()
