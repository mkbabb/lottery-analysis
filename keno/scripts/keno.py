import argparse
import bisect
import contextlib
import json
import os
from datetime import datetime, timedelta
from typing import *

import pandas as pd
import sqlalchemy as sqla
from sqlalchemy import func

from bit_manipulations import bits_to_nums, nums_to_bits, popcount64d
from schemas import *
from utils import create_sqla_engine_str, read_sql_table_tmpfile

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
        # Shift by one day and add (inclusive range).
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
    wagers: pd.DataFrame, conn: sqla.engine.Connection
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
    table_name = "numbers_wagered"
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

    numbers_wagered = pd.read_sql_table(table_name, con=conn, index_col="id")

    if not numbers_wagered.empty:
        dups = t_numbers_wagered.set_index(pk).index.isin(
            numbers_wagered.set_index(pk).index
        )
        t_numbers_wagered = t_numbers_wagered.loc[~dups]

    t_numbers_wagered.to_sql(
        table_name, con=conn, if_exists="append", index=False, method="multi"
    )

    return pd.read_sql_table(table_name, con=conn, index_col="id")


def map_wagers(wagers: pd.DataFrame, numbers_wagered: pd.DataFrame) -> pd.DataFrame:
    pk = ["low_bits", "high_bits"]

    mapped_index = wagers[pk].merge(numbers_wagered.reset_index(), on=pk, how="left")
    wagers["numbers_wagered_id"] = mapped_index["id"]

    wagers = wagers.drop(pk, axis=1).reset_index(drop=True)

    return wagers


def explode_wagers(wagers: pd.DataFrame, conn: sqla.engine.Connection) -> pd.DataFrame:
    tmp_table_name = "tmp_wagers"
    wagers.to_sql(
        tmp_table_name,
        con=conn,
        index_label="wager_id",
        if_exists="replace",
        method="multi",
    )
    sql = f"""SELECT
    {tmp_table_name}. *,
    drawings.id AS tmp_draw_number_id
FROM
    {tmp_table_name}
    LEFT JOIN drawings ON drawings.id BETWEEN {tmp_table_name}.begin_draw
    AND {tmp_table_name}.end_draw"""

    tmp_table = pd.read_sql(sql, con=conn)
    tmp_table["draw_number_id"] = tmp_table["tmp_draw_number_id"]
    tmp_table.drop("tmp_draw_number_id", axis=1, inplace=True)

    return tmp_table


def trim_to_max_pk(
    table_name: str,
    pk: str,
    conn: sqla.engine.Connection,
) -> int:
    metadata = sqla.MetaData(bind=conn)
    table = sqla.Table(table_name, metadata, autoload=True)

    start_id = conn.execute(func.max(table.c[pk])).scalar()

    if start_id is not None:
        conn.execute(table.delete().where(table.c[pk] == start_id))
        return start_id
    else:
        return -1


def find_and_set_winnings(
    wagers: pd.DataFrame,
    numbers_wagered: pd.DataFrame,
    drawings: pd.DataFrame,
    wagers_table_name: str,
    conn: sqla.engine.Connection,
) -> pd.DataFrame:
    """
    Function to find the prize amount of each item in the
    'wagers' DataFrame.

    @param wagers: DataFrame containing keno wagers data.
    @param numbers_wagered: DataFrame containing numbers_wagered data.
    @param drawings: DataFrame containing keno drawings data.

    @returns wagers: modified 'wagers' DataFrame.

    """
    metadata = sqla.MetaData(bind=conn)
    wagers_table = sqla.Table(wagers_table_name, metadata, autoload=True)

    def calculate_prize(row: pd.Series) -> pd.Series:
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
        try:
            numbers_wagered_id = row["numbers_wagered_id"]
            draw_number_id = row["draw_number_id"]

            high_bits1 = numbers_wagered.at[numbers_wagered_id, "high_bits"]
            low_bits1 = numbers_wagered.at[numbers_wagered_id, "low_bits"]
            number_played = numbers_wagered.at[numbers_wagered_id, "numbers_played"]

            high_bits2 = drawings.at[draw_number_id, "high_bits"]
            low_bits2 = drawings.at[draw_number_id, "low_bits"]

            match_mask = [low_bits1 & low_bits2, high_bits1 & high_bits2]
            numbers_matched = sum(map(popcount64d, match_mask))

            row["low_match_mask"] = match_mask[0]
            row["high_match_mask"] = match_mask[1]

            row["numbers_matched"] = numbers_matched
            row["prize"] = PRIZE_DICT.get(number_played, {}).get(numbers_matched, 0)

            conn.execute(wagers_table.insert(), **row)

        except Exception as e:
            print(e)

        return row

    return wagers.assign(
        low_match_mask=0, high_match_mask=0, numbers_matched=0, prize=0
    ).apply(calculate_prize, axis=1)


def trim_imported_wagers(
    wagers: pd.DataFrame, wagers_table_name: str, conn: sqla.engine.Connection
) -> pd.DataFrame:
    pk = "wager_id"
    start_id = trim_to_max_pk(wagers_table_name, pk=pk, conn=conn)
    return wagers[wagers[pk] >= start_id]


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

    wagers_table_name = "wagers"
    numbers_wagered_table_name = "numbers_wagered"
    drawings_table_name = "drawings"

    with contextlib.closing(open_mysql_conn()) as conn:
        # numbers_wagered = pd.read_sql_table(numbers_wagered_table_name, con=conn)

        numbers_wagered = read_sql_table_tmpfile(numbers_wagered_table_name, con=conn)

        # drawings = process_drawings(drawings)
        # drawings.to_sql("drawings", con=conn, if_exists="append", index=False, method="multi")
        drawings = read_sql_table_tmpfile(drawings_table_name, con=conn, index_col="id")

        # wagers = process_wagers(wagers)
        # numbers_wagered = create_numbers_wagered(wagers, conn)

        # wagers = map_wagers(wagers, numbers_wagered)
        # wagers.to_csv(os.path.join(args.dirpath, "wagers.csv"), index=False)
        # del wagers
        # input("Explode the wagers.")

        wagers = pd.read_csv(os.path.join(args.dirpath, "exploded_wagers.csv"))
        wagers = trim_imported_wagers(wagers, wagers_table_name, conn)

        wagers = find_and_set_winnings(
            wagers, numbers_wagered, drawings, wagers_table_name, conn
        )
        # wagers.to_sql(
        #     "wagers", con=conn, if_exists="append", index=False, method="multi"
        # )


if __name__ == "__main__":
    main()
