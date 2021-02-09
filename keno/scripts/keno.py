import datetime
import os
import pathlib
import sqlite3
from typing import *

import pandas as pd

from bit_manipulations import bits_to_nums, nums_to_bits, popcount64d

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

DRAWINGS_SCHEMA = """
CREATE TABLE "drawings" (
	"id"	INTEGER UNIQUE,
	"date"	INTEGER,
	"high_bits"	UNSIGNED SMALL INTEGER NOT NULL,
	"low_bits"	UNSIGNED INTEGER NOT NULL,
	"number_string"	TEXT,
	PRIMARY KEY("id")
);
"""


WAGERS_SCHEMA = """
CREATE TABLE "wagers" (
	"id"	INTEGER PRIMARY KEY AUTOINCREMENT,
    "date" INTEGER,
	"draw_number_id"	INTEGER,
    "begin_draw"  INTEGER,
    "end_draw"  INTEGER,
    "qp"    UNSIGNED INTEGER,
    "ticket_cost" INTEGER,
	"numbers_wagered_id"	INTEGER,
	"numbers_matched"	TINY INT,
	"high_match_mask"	UNSIGNED SMALL INTEGER,
	"low_match_mask"	UNSIGNED INTEGER,
	"prize"	UNSIGNED INT,
	FOREIGN KEY("draw_number_id") REFERENCES "drawings"("id"),
	FOREIGN KEY("date") REFERENCES "drawings"("date"),
	FOREIGN KEY("numbers_wagered") REFERENCES "numbers_wagered"("id")
);
"""

NUMBERS_WAGERED_SCHEMA = """
CREATE TABLE "numbers_wagered" (
	"id"	INTEGER PRIMARY KEY AUTOINCREMENT,
	"number_string"	TEXT,
	"high_bits"	UNSIGNED SMALL INTEGER,
	"low_bits"	UNSIGNED INTEGER,
	"numbers_played"	TINY INTEGER
);
"""

epoch = datetime.datetime.utcfromtimestamp(0)


def totimestamp(dt: datetime.datetime) -> int:
    return int((dt - epoch).total_seconds())


def process_drawings(drawings: pd.DataFrame) -> pd.DataFrame:
    """
    Initial pre-processing of the drawings DataFrame.

    Processes the lottery numbers into their corresponding
    bit and integer array counterparts.
    Thereinafter, the dates are formatted (by accumulation)
    into evenly spaced intervals of 5, 24.

    @param drawings: DataFrame containing keno drawings data.

    @returns drawings: modified 'drawings' DataFrame.
    """

    drawings = drawings.rename(
        columns={
            "Draw Nbr": "id",
            "Draw Date": "date",
            "Winning Number String": "number_string",
        }
    )

    def to_bits(df: pd.Series) -> pd.Series:
        bit_info = nums_to_bits(
            df["number_string"],
            bit_length=MAX_BITS,
            max_num=MAX_NUMBERS,
            num_length=2,
            delim=" ",
        )

        df["number_string"] = bits_to_nums(bit_info, delim=",", bit_length=MAX_BITS)

        d = pd.Series(dict(zip(["low_bits", "high_bits"], bit_info)))
        df = df.append(d)

        return df

    drawings = drawings.apply(to_bits, axis=1, result_type="expand")

    drawings["date"] = 0

    drawings.set_index("id", inplace=True)

    return drawings


def create_numbers_wagered(
    wagers: pd.DataFrame, numbers_wagered: Optional[pd.DataFrame] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    For the creation of the secondary foreign key table 'number_wagered'.
    Allows for once-over preprocessing of unique ticket lottery numbers.
    Equates to a roughly 80% size reduction of the wagers DataFrame.

    Utilizes the same process by which 'process_drawings' processes
    the lottery numbers.

    @param wagers: DataFrame containing keno wagers data.

    @returns number_wagered: new 'number_wagered' DataFrame wherewith the
                    subsequent ticket lottery numbers are stored.
    """

    def get_bit_info(nums: str) -> Tuple[list, str]:
        bit_info = nums_to_bits(
            nums, bit_length=MAX_BITS, max_num=MAX_NUMBERS, num_length=2
        )

        return bit_info, bits_to_nums(bit_info, delim=",", bit_length=MAX_BITS)

    def to_bits(row: pd.Series) -> pd.Series:
        bit_info, number_string = get_bit_info(row["number_string"])

        row["number_string"] = number_string
        bit_info.append(sum(map(popcount64d, bit_info)))

        d = pd.Series(dict(zip(["low_bits", "high_bits", "numbers_played"], bit_info)))
        row = row.append(d)

        return row

    if numbers_wagered is None:
        numbers_wagered = (
            wagers["numbers_wagered"]
            .drop_duplicates()
            .reset_index(drop=True)
            .to_frame("number_string")
        )
        numbers_wagered = numbers_wagered.apply(to_bits, axis=1, result_type="expand")

    numbers_wagered_dict = dict(
        zip(
            numbers_wagered["number_string"],
            numbers_wagered.index,
        )
    )
    wagers["numbers_wagered_id"] = wagers["numbers_wagered"].map(
        lambda x: numbers_wagered_dict.get(get_bit_info(x)[1])
    )

    wagers.drop("numbers_wagered", axis=1, inplace=True)

    return wagers, numbers_wagered


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
        df: pd.Series, numbers_wagered: pd.DataFrame, drawings: pd.DataFrame
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

        number_wagered_id = df["numbers_wagered_id"]
        draw_number_id = df["draw_number_id"]

        high_bits1 = numbers_wagered.loc[number_wagered_id, "high_bits"]
        low_bits1 = numbers_wagered.loc[number_wagered_id, "low_bits"]
        number_played = numbers_wagered.loc[number_wagered_id, "numbers_played"]

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
            numbers_matched = sum(map(lambda x: popcount64d(x), match_mask))

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

        df = df.append(d)
        return df

    wagers = wagers.apply(
        lambda x: calculate_prize(x, numbers_wagered, drawings),
        axis=1,
        result_type="expand",
    )

    return wagers


def concat_csv(
    filepaths: List[str],
    sep: str,
    out_filepath: Optional[str] = None,
    names: Optional[List[str]] = None,
) -> pd.DataFrame:
    has_header = None if names is not None else True

    dfs = (
        pd.read_csv(filepath, sep=sep, names=names, header=has_header)
        for filepath in filepaths
    )

    df = pd.concat(dfs)

    if out_filepath is not None:
        df.to_csv(out_filepath, index=False)

    return df


def process_keno_split_data(data_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    wagers_out_path = os.path.join(data_dir, "wagers.csv")
    drawings_out_path = os.path.join(data_dir, "drawings.csv")

    get_paths = lambda glob: list(
        sorted(map(lambda x: str(x), pathlib.Path(data_dir).glob(glob)))
    )

    def read_or_process(file_path: str, func) -> pd.DataFrame:
        if not os.path.exists(file_path):
            df = func()
            df.to_csv(file_path, index=False)
            return df
        else:
            return pd.read_csv(file_path)

    def get_wagers() -> pd.DataFrame:
        wagers_paths = get_paths("split/*wager*")
        wagers_names = "begin_draw;end_draw;qp;ticket_cost;numbers_wagered".split(";")
        return concat_csv(
            wagers_paths,
            sep=";",
            out_filepath=wagers_out_path,
            names=wagers_names,
        )

    def get_drawings() -> pd.DataFrame:
        drawings_paths = get_paths("split/*draw*")
        drawings_names = "Draw Nbr;Draw Date;Winning Number String".split(";")
        return concat_csv(
            drawings_paths,
            sep=";",
            out_filepath=drawings_out_path,
            names=drawings_names,
        )

    wagers, drawings = read_or_process(wagers_out_path, get_wagers), read_or_process(
        drawings_out_path, get_drawings
    )
    return wagers[:100], drawings


conn = sqlite3.connect("keno/data/keno_2017_2019/keno_v3.db")

data_dir = "keno/data/keno_2017_2019/"


wagers, drawings = process_keno_split_data(data_dir=data_dir)

drawings = process_drawings(drawings)

wagers, numbers_wagered = create_numbers_wagered(wagers)

# wagers = find_and_set_winnings(
#     wagers=wagers, numbers_wagered=numbers_wagered, drawings=drawings
# )


drawings.to_sql(
    name="drawings",
    con=conn,
    schema=DRAWINGS_SCHEMA,
    if_exists="replace",
    index_label="id",
)
numbers_wagered.to_sql(
    name="numbers_wagered",
    con=conn,
    schema=NUMBERS_WAGERED_SCHEMA,
    if_exists="replace",
    index_label="id",
)
wagers.to_sql(
    name="wagers", con=conn, schema=WAGERS_SCHEMA, if_exists="replace", index_label="id"
)
