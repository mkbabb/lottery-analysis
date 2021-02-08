import datetime
import math
import os
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Union, Tuple

import pandas as pd
import numpy as np
import pathlib

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
    1: {1: 2}
}

DRAWINGS_SCHEMA = '''
CREATE TABLE "drawings" (
	"id"	INTEGER UNIQUE,
	"date"	INTEGER,
	"high_bits"	UNSIGNED SMALL INTEGER NOT NULL,
	"low_bits"	UNSIGNED INTEGER NOT NULL,
	"number_string"	TEXT,
	PRIMARY KEY("id")
);
'''


TICKETS_SCHEMA = '''
CREATE TABLE "tickets" (
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
'''

NUMBERS_WAGERED_SCHEMA = '''
CREATE TABLE "numbers_wagered" (
	"id"	INTEGER PRIMARY KEY AUTOINCREMENT,
	"number_string"	TEXT,
	"high_bits"	UNSIGNED SMALL INTEGER,
	"low_bits"	UNSIGNED INTEGER,
	"numbers_played"	TINY INTEGER
);
'''

epoch = datetime.datetime.utcfromtimestamp(0)


def totimestamp(dt: datetime.datetime) -> int:
    return int((dt - epoch).total_seconds())


def process_wagers(wagers: pd.DataFrame) -> pd.DataFrame:
    '''
    Initial pre-processing of the wagers DataFrame.

    @param wagers: DataFrame containing keno wagers data.

    @returns wagers: modified wagers DataFrame.
    '''

    def bool_coalesce(b: str) -> int:
        b = b.lower().strip()
        if (b == "t" or b == "true"):
            return 0
        else:
            return 1

    dfs: List[pd.DataFrame] = []

    def func(x):
        begin_draw = x[0]
        end_draw = x[1]

        delta = end_draw - begin_draw + 1

        x[2] = bool_coalesce(x[2])
        x[3] //= delta

        if (delta > 1):
            x = x.reshape((1, -1))
            x = x.repeat(delta, axis=0)
            t = (x[..., 0] + np.arange(delta)).reshape((-1, 1))
            x = np.append(x, t, axis=1)
        return x

    tmp = np.apply_along_axis(func, -1, wagers.values)
    print(tmp)

    # def expand_draw_number(df: pd.Series) -> None:
    #     begin_draw = df["begin_draw"]
    #     end_draw = df["end_draw"]

    #     delta = end_draw - begin_draw + 1

    #     df["qp"] = bool_coalesce(df["qp"])
    #     df["ticket_cost"] //= delta

    #     tmp = []

    #     for i in range(begin_draw, end_draw + 1):
    #         t_df = df.copy()
    #         t_df["draw_number_id"] = i
    #         tmp.append(t_df)

    #     dfs.append(pd.DataFrame(tmp).reset_index(drop=True))

    # wagers.apply(
    #     expand_draw_number, axis=1)

    # wagers = pd.concat(dfs, axis=0, ignore_index=True)

    return wagers


def process_drawings(drawings: pd.DataFrame) -> pd.DataFrame:
    '''
    Initial pre-processing of the drawings DataFrame.

    Processes the lottery numbers into their corresponding
    bit and integer array counterparts.
    Thereinafter, the dates are formatted (by accumulation)
    into evenly spaced intervals of 5, 24.

    @param drawings: DataFrame containing keno drawings data.

    @returns drawings: modified 'drawings' DataFrame.
    '''

    drawings = drawings\
        .rename(columns={"Draw Nbr": "id",
                         "Draw Date": "date",
                         "Winning Number String": "number_string"})

    def to_bits(df: pd.Series) -> pd.Series:
        l = nums_to_bits(df["number_string"],
                         bit_length=MAX_BITS,
                         max_num=MAX_NUMBERS,
                         num_length=2,
                         delim=" ")

        df["number_string"] = bits_to_nums(l,
                                           delim=",",
                                           bit_length=MAX_BITS)

        d = pd.Series(dict(zip(["low_bits", "high_bits"], l)))
        df = df.append(d)

        return df

    drawings = drawings\
        .apply(to_bits, axis=1, result_type="expand")

    # The below will need to be modified to accommodate the new v2 formatting.
    # shape = [249, 1]
    # strides = [5 * 60, 24 * 60 * 60]

    # # Initialize the starting date to be at 5:05 AM on the day thence.
    # start_time = (totimestamp(
    #     datetime.datetime.strptime(
    #         drawings.loc[0, "date"], "%m/%d/%Y")) - 24 * 60 * 60) + 5 * 60 * (60 + 1)
    # # Start the accumulation at the 228th interval of 5 on the hereinbefore mentioned day.
    # offset = 228 * 60 * 5
    # times = [start_time] * 10002
    # times[0] += offset
    # accumulate_by(times, shape, strides, [offset, 0])

    # drawings["date"] = pd.DataFrame(times)

    # def f(x):
    #     print(datetime.datetime.utcfromtimestamp(x).strftime(
    #         '%Y-%m-%d %H:%M:%S'))
    # drawings["date"].apply(f, 1)
    drawings["date"] = 0

    return drawings.set_index("id")


def process_numbers_wagered(wagers: pd.DataFrame) -> pd.DataFrame:
    '''
    For the creation of the secondary foreign key table 'number_wagered'.
    Allows for once-over preprocessing of unique ticket lottery numbers.
    Equates to a roughly 80% size reduction of the wagers DataFrame.

    Utilizes the same process by which 'process_drawings' processes
    the lottery numbers.

    @param wagers: DataFrame containing keno wagers data.

    @returns number_wagered: new 'number_wagered' DataFrame wherewith the
                    subsequent ticket lottery numbers are stored.
    '''
    numbers_wagered = wagers["numbers_wagered"]\
        .drop_duplicates()\
        .reset_index(drop=True)\
        .to_frame("number_string")

    ratio = numbers_wagered["number_string"].shape[0] / \
        wagers["numbers_wagered"].shape[0]

    def to_bits(df: pd.Series) -> pd.Series:
        l = nums_to_bits(df["number_string"],
                         bit_length=MAX_BITS,
                         max_num=MAX_NUMBERS,
                         num_length=2)

        df["number_string"] = bits_to_nums(l,
                                           delim=",",
                                           bit_length=MAX_BITS)
        l.append(sum(map(popcount64d, l)))

        d = pd.Series(dict(zip(["low_bits",
                                "high_bits",
                                "numbers_played"], l)))
        df = df.append(d)

        return df

    numbers_wagered = numbers_wagered\
        .apply(to_bits,
               axis=1, result_type="expand")

    numbers_wagered_dict = dict(zip(
        numbers_wagered["number_string"],
        numbers_wagered.index,)
    )

    wagers["numbers_wagered"] = wagers["numbers_wagered"]\
        .map(numbers_wagered_dict.get)
    wagers.rename(
        columns={"numbers_wagered": "numbers_wagered_id"}, inplace=True)

    return numbers_wagered


def calculate_prize(df: pd.Series,
                    numbers_wagered: pd.DataFrame,
                    drawings: pd.DataFrame) -> pd.Series:
    '''
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
    '''

    number_wagered_id = df["numbers_wagered_id"]
    draw_number_id = df["draw_number_id"]

    high_bits1 = numbers_wagered.loc[number_wagered_id, "high_bits"]
    low_bits1 = numbers_wagered.loc[number_wagered_id, "low_bits"]
    number_played = numbers_wagered.loc[number_wagered_id, "numbers_played"]

    try:
        high_bits2 = drawings.loc[draw_number_id, "high_bits"]
        low_bits2 = drawings.loc[draw_number_id, "low_bits"]

        date = drawings.loc[draw_number_id, "date"]

        match_mask = list(map(lambda x: x[0] & x[1],
                              zip([low_bits1, high_bits1],
                                  [low_bits2, high_bits2])))
        numbers_matched = sum(
            map(lambda x: popcount64d(x), match_mask))

        try:
            prize = PRIZE_DICT[number_played][numbers_matched]
            match_mask += [numbers_matched, prize, date]

        except KeyError:
            match_mask += [numbers_matched, 0, date]

    except KeyError:
        match_mask = [0, 0, 0, 0, 0]

    d = pd.Series(dict(zip(["high_match_mask",
                            "low_match_mask",
                            "numbers_matched",
                            "prize",
                            "date"], match_mask)))

    df = df.append(d)

    return df


def find_winnings(wagers: pd.DataFrame,
                  numbers_wagered: pd.DataFrame,
                  drawings: pd.DataFrame) -> pd.DataFrame:
    '''
    Function to find the prize amount of each item in the
    'wagers' DataFrame.

    Most of the work is done within 'calculate_prize' function.

    @param wagers: DataFrame containing keno wagers data.
    @param numbers_wagered: DataFrame containing numbers_wagered data.
    @param drawings: DataFrame containing keno drawings data.

    @returns wagers: modified 'wagers' DataFrame.

    '''
    wagers = wagers\
        .apply(lambda x: calculate_prize(x, numbers_wagered, drawings), axis=1, result_type="expand")

    return wagers


def concat_csv(filepaths: List[str], sep: str, out_filepath: Optional[str], names: Optional[List[str]]) -> pd.DataFrame:
    has_header = None if names is not None else True

    dfs = (pd.read_csv(filepath, sep=sep, names=names, header=has_header)
           for filepath in filepaths)

    df = pd.concat(dfs, axis=1)

    if (out_filepath is not None):
        df.to_csv(out_filepath, index=False)

    return df


data_dir = "keno/data/keno_2017_2019/"


conn = sqlite3.connect(
    "keno/data/keno_2017_2019/keno_v2.db")

wagers_paths = list(pathlib.Path(data_dir).glob("*wager*"))
wagers = concat_csv(wagers_paths)

drawings_paths = list(pathlib.Path(data_dir).glob("*drawing*"))
drawings = concat_csv(drawings_paths)


# t_drawings.to_sql(name="drawings",
#                   con=conn,
#                   schema=DRAWINGS_SCHEMA,
#                   if_exists="replace",
#                   index_label="id")
# t_numbers_wagered.to_sql(name="numbers_wagered",
#                          con=conn,
#                          schema=NUMBERS_WAGERED_SCHEMA,
#                          if_exists="replace",
#                          index_label="id")
# t_tickets.to_sql(name="tickets",
#                  con=conn,
#                  schema=TICKETS_SCHEMA,
#                  if_exists="replace",
#                  index_label="id")
