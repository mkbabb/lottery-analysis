import datetime
import os
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

from bit_array import (binary_operate_int_array, bit_array_to_ints,
                       bit_array_to_lotto, ints_to_bit_array,
                       lotto_to_bit_array, popcount64d,
                       unary_operate_int_array)

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

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
	"round"	INTEGER UNIQUE,
	"date"	INTEGER,
	"high_bits"	UNSIGNED SMALL INTEGER NOT NULL,
	"low_bits"	UNSIGNED INTEGER NOT NULL,
	"drawn_numbers"	TEXT,
	PRIMARY KEY("round")
);
'''


TICKETS_SCHEMA = '''
CREATE TABLE "tickets" (
	"index"	INTEGER PRIMARY KEY AUTOINCREMENT,
    "date" INTEGER,
	"round"	INTEGER,
	"spots"	INTEGER,
	"number_spots_matched"	TINY INT,
	"high_match_mask"	UNSIGNED SMALL INTEGER,
	"low_match_mask"	UNSIGNED INTEGER,
	"prize"	UNSIGNED INT,
	FOREIGN KEY("round") REFERENCES "drawings"("round"),
	FOREIGN KEY("date") REFERENCES "drawings"("date"),
	FOREIGN KEY("spots") REFERENCES "spots"("id")
);
'''

SPOTS_SCHEMA = '''
CREATE TABLE "spots" (
	"index"	INTEGER PRIMARY KEY AUTOINCREMENT,
	"spots"	TEXT,
	"high_bits"	UNSIGNED SMALL INTEGER,
	"low_bits"	UNSIGNED INTEGER,
	"number_spots_played"	TINY INTEGER
);
'''

epoch = datetime.datetime.utcfromtimestamp(0)


def totimestamp(dt: datetime.datetime) -> int:
    return int((dt - epoch).total_seconds())


def accumulate_by(arr: list,
                  shape: List[int],
                  strides: List[int],
                  axis_counter: List[int] = None) -> List[int]:
    '''
    Accumulates strides[i] by shape[i] into arr.
    The routine below is essentially a multi-dimensional array indexing routine,
    hence the utilization of variable names 'shape' and 'strides'.

    @param arr: array wherewith the data is accumulated into.
    @param shape: array wherewith the interval of accumulation is therein described.
    @param strides: array wherewith the amount of accumulation is therein described.
    @param axis_counter: array utilized for initializing the indexing.

    @returns: arr
    '''
    i = 1
    mdim = len(strides)
    if (axis_counter is None):
        axis_counter = [0] * mdim

    stride_shape = list(map(lambda x: x[0] * x[1],
                            zip(shape, strides)))
    while (i < len(arr)):
        axis_counter[0] += strides[0]

        for j in range(1, mdim):
            if axis_counter[j - 1] >= stride_shape[j - 1]:
                axis_counter[j - 1] = 0
                axis_counter[j] += strides[j]

        ix = sum(axis_counter)
        arr[i] += ix
        # print(datetime.datetime.utcfromtimestamp(arr[i]).strftime(
        #     '%Y-%m-%d %H:%M:%S'), axis_counter, i)
        i += 1
    return arr


def process_tickets(tickets: pd.DataFrame) -> pd.DataFrame:
    '''
    Initial pre-processing of the tickets DataFrame.

    @param tickets: DataFrame containing keno tickets data.

    @returns tickets: modified tickets DataFrame.
    '''
    tickets = tickets.drop("TRANSACTION_DATE", 1)\
        .rename(columns={"Draw_Number": "round",
                         "NUMBER_WAGERED": "spots"})
    return tickets


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
    bit_array = drawings["Winning Number String"].apply(
        lambda x: lotto_to_bit_array(x, delim=" "),
        1)
    int_array = bit_array.apply(bit_array_to_ints, 1)

    drawings["high_bits"] = int_array.apply(
        lambda x: x[0], 1)
    drawings["low_bits"] = int_array.apply(
        lambda x: x[1], 1)

    drawings = drawings\
        .rename(columns={"Draw Nbr": "round",
                         "Draw Date": "date",
                         "Winning Number String": "drawn_numbers"})

    drawings["drawn_numbers"] = bit_array.apply(
        lambda x: bit_array_to_lotto(x, delim=","))

    shape = [249, 1]
    strides = [5 * 60, 24 * 60 * 60]

    start_time = (totimestamp(
        datetime.datetime.strptime(
            drawings.loc[0, "date"], "%m/%d/%Y")) - 24 * 60 * 60) + 5 * 60 * (60 + 1)

    offset = 228 * 60 * 5

    times = [start_time] * 10002
    times[0] += offset
    accumulate_by(times, shape, strides, [offset, 0])

    drawings["date"] = pd.DataFrame(times)

    return drawings.set_index("round")


def process_spots(tickets: pd.DataFrame) -> pd.DataFrame:
    '''
    For the creation of the secondary foreign key table 'spots'.
    Allows for once-over preprocessing of unique ticket lottery numbers.
    Equates to a roughly 80% size reduction of the tickets DataFrame.

    Utilizes the same process by which 'process_drawings' processes
    the lottery numbers.

    @param tickets: DataFrame containing keno tickets data.

    @returns spots: new 'spots' DataFrame wherewith the
                    subsequent ticket lottery numbers are stored.
    '''
    spots = tickets["spots"]\
        .drop_duplicates()\
        .reset_index(drop=True)\
        .to_frame("spots")

    ratio = spots["spots"].shape[0] / tickets["spots"].shape[0]
    print(ratio)

    bit_array = spots["spots"]\
        .apply(lambda x: lotto_to_bit_array(x, num_length=2),
               1)
    int_array = bit_array.apply(bit_array_to_ints, 1)

    spots.loc[:, "high_bits"] = int_array.apply(
        lambda x: x[0],
        1)
    spots.loc[:, "low_bits"] = int_array.apply(
        lambda x: x[1],
        1)
    spots.loc[:, "number_spots_played"] = int_array.apply(
        lambda x: sum(map(lambda y: popcount64d(y) - 1, x)))

    spots_dict = dict(zip(
        spots["spots"],
        spots.index,)
    )
    tickets["spots"] = tickets["spots"].map(spots_dict.get)
    tickets.rename(columns={"spots": "spots_id"},
                   inplace=True)
    spots.loc[:, "spots"] = bit_array.apply(
        lambda x: bit_array_to_lotto(x, delim=","))

    return spots


def calculate_prize(x: list,
                    spots: pd.DataFrame,
                    drawings: pd.DataFrame) -> List[int]:
    '''
    Function applied to all rows in the 'tickets' DataFrame.
    Utilized normally via a pd.apply method.

    Principally, this function is responsible for the bit-wise AND'ing of
    two lottery numbers, allowing for fast matching of the theretofore
    mentioned numbers.

    @param x: 'spots_id' and 'round' element of the 'tickets' DataFrame
    @param spots: DataFrame containing spots data.
    @param drawings: DataFrame containing keno drawings data.

    @returns match_mask: array of high and low bits of the match,
                         hamming weight (number of spots played),
                         and date.
    '''
    spot = x[0]
    round_ = x[1]

    high_bits1 = spots.loc[spot, "high_bits"]
    low_bits1 = spots.loc[spot, "low_bits"]

    number_spots_played = spots.loc[spot, "number_spots_played"]

    try:
        high_bits2 = drawings.loc[round_, "high_bits"]
        low_bits2 = drawings.loc[round_, "low_bits"]
        date = drawings.loc[round_, "date"]

        match_mask = list(map(lambda x: x[0] & x[1],
                              zip([high_bits1, low_bits1],
                                  [high_bits2, low_bits2])))
        try:
            number_spots_matched = sum(
                map(lambda x: popcount64d(x) - 1, match_mask))

            prize = PRIZE_DICT[number_spots_played][number_spots_matched]
            match_mask += [number_spots_matched, prize, date]
        except KeyError:
            match_mask += [0, 0, date]
    except KeyError:
        match_mask = [0, 0, 0, 0, 0]

    return match_mask


def find_winnings(tickets: pd.DataFrame,
                  spots: pd.DataFrame,
                  drawings: pd.DataFrame) -> pd.DataFrame:
    '''
    Function to find the prize amount of each item in the
    'tickets' DataFrame.

    Most of the work is done within 'calculate_prize' function.

    @param tickets: DataFrame containing keno tickets data.
    @param spots: DataFrame containing spots data.
    @param drawings: DataFrame containing keno drawings data.

    @returns tickets: modified 'tickets' DataFrame.

    '''
    match_mask = tickets[["spots_id", "round"]]\
        .apply(lambda x: calculate_prize(x, spots, drawings), 1)

    tickets.loc[:, "high_match_mask"] = match_mask.apply(
        lambda x: x[0], 1)
    tickets.loc[:, "low_match_mask"] = match_mask.apply(
        lambda x: x[1], 1)
    tickets.loc[:, "number_spots_matched"] = match_mask.apply(
        lambda x: x[2], 1)
    tickets.loc[:, "prize"] = match_mask.apply(
        lambda x: x[3], 1)
    # tickets.loc[:, "date"] = match_mask.apply(
    #     lambda x: x[4], 1)

    return tickets


conn = sqlite3.connect(os.path.join(DIR_PATH, "keno.db"))




drawings = pd.read_csv(os.path.join(DIR_PATH, "Keno_Draw_Results.csv"))
tickets = pd.read_csv(os.path.join(
    DIR_PATH, "Keno_Transactions.txt"),
    sep=";")


# times = t_drawings.pivot_table(index=["date"], aggfunc="size")
# print(times)

t_tickets = process_tickets(tickets)
t_drawings = process_drawings(drawings)
t_spots = process_spots(t_tickets)

t_tickets = find_winnings(t_tickets, t_spots, t_drawings)


t_drawings.to_sql(name="drawings",
                  con=conn,
                  schema=DRAWINGS_SCHEMA,
                  if_exists="replace",
                  index_label="id")
t_spots.to_sql(name="spots",
               con=conn,
               schema=SPOTS_SCHEMA,
               if_exists="replace",
               index_label="id")
t_tickets.to_sql(name="tickets",
                 con=conn,
                 schema=TICKETS_SCHEMA,
                 if_exists="replace",
                 index_label="id")
