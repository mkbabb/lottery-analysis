import datetime
import math
import os
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Union, Tuple

import pandas as pd

from bit_manipulations import bits_to_nums, nums_to_bits, popcount64d

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

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
	"numbers_winning"	TEXT,
	PRIMARY KEY("id")
);
'''


TICKETS_SCHEMA = '''
CREATE TABLE "tickets" (
	"id"	INTEGER PRIMARY KEY AUTOINCREMENT,
    "date" INTEGER,
	"draw_number_id"	INTEGER,
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
    tickets = tickets\
        .drop("TRANSACTION_DATE", 1)\
        .rename(columns={"Draw_Number": "draw_number_id",
                         "NUMBER_WAGERED": "numbers_wagered"})
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
    bits = drawings["Winning Number String"].apply(
        lambda x: nums_to_bits(x,
                               bit_length=MAX_BITS,
                               max_num=MAX_NUMBERS,
                               delim=" "),
        1)

    drawings["low_bits"] = bits.apply(
        lambda x: x[0], 1)
    drawings["high_bits"] = bits.apply(
        lambda x: x[1], 1)

    drawings = drawings\
        .rename(columns={"Draw Nbr": "id",
                         "Draw Date": "date",
                         "Winning Number String": "numbers_winning"})

    drawings["numbers_winning"] = bits.apply(
        lambda x: bits_to_nums(x,
                               delim=",",
                               bit_length=MAX_BITS))

    shape = [249, 1]
    strides = [5 * 60, 24 * 60 * 60]

    # Initialize the starting date to be at 5:05 AM on the day thence.
    start_time = (totimestamp(
        datetime.datetime.strptime(
            drawings.loc[0, "date"], "%m/%d/%Y")) - 24 * 60 * 60) + 5 * 60 * (60 + 1)
    # Start the accumulation at the 228th interval of 5 on the hereinbefore mentioned day.
    offset = 228 * 60 * 5
    times = [start_time] * 10002
    times[0] += offset
    accumulate_by(times, shape, strides, [offset, 0])

    drawings["date"] = pd.DataFrame(times)

    # def f(x):
    #     print(datetime.datetime.utcfromtimestamp(x).strftime(
    #         '%Y-%m-%d %H:%M:%S'))
    # drawings["date"].apply(f, 1)

    return drawings.set_index("id")


def process_numbers_wagered(tickets: pd.DataFrame) -> pd.DataFrame:
    '''
    For the creation of the secondary foreign key table 'number_wagered'.
    Allows for once-over preprocessing of unique ticket lottery numbers.
    Equates to a roughly 80% size reduction of the tickets DataFrame.

    Utilizes the same process by which 'process_drawings' processes
    the lottery numbers.

    @param tickets: DataFrame containing keno tickets data.

    @returns number_wagered: new 'number_wagered' DataFrame wherewith the
                    subsequent ticket lottery numbers are stored.
    '''
    numbers_wagered = tickets["numbers_wagered"]\
        .drop_duplicates()\
        .reset_index(drop=True)\
        .to_frame("number_string")

    ratio = numbers_wagered["number_string"].shape[0] / \
        tickets["numbers_wagered"].shape[0]
    print(f"savings ratio: {ratio}")

    bits = numbers_wagered["number_string"]\
        .apply(lambda x: nums_to_bits(x,
                                      bit_length=MAX_BITS,
                                      max_num=MAX_NUMBERS,
                                      num_length=2),
               1)

    numbers_wagered.loc[:, "low_bits"] = bits.apply(
        lambda x: x[0],
        1)
    numbers_wagered.loc[:, "high_bits"] = bits.apply(
        lambda x: x[1],
        1)
    numbers_wagered.loc[:, "numbers_played"] = bits.apply(
        lambda x: sum(map(lambda y: popcount64d(y), x)))

    numbers_wagered_dict = dict(zip(
        numbers_wagered["number_string"],
        numbers_wagered.index,)
    )
    tickets["numbers_wagered"] = tickets["numbers_wagered"]\
        .map(numbers_wagered_dict.get)
    tickets.rename(
        columns={"numbers_wagered": "numbers_wagered_id"}, inplace=True)

    numbers_wagered.loc[:, "number_string"] = bits.apply(
        lambda x: bits_to_nums(x,
                               delim=",",
                               bit_length=MAX_BITS))

    return numbers_wagered


def calculate_prize(x: list,
                    numbers_wagered: pd.DataFrame,
                    drawings: pd.DataFrame) -> List[int]:
    '''
    Function applied to all rows in the 'tickets' DataFrame.
    Utilized normally via pd.apply.

    Principally, this function is responsible for the bit-wise AND'ing of
    two lottery numbers, allowing for fast matching of theretofore
    mentioned numbers.

    @param x: 'numbers_wagered_id' and 'draw_number_id' element of the 'tickets' DataFrame
    @param spots: DataFrame containing spots data.
    @param drawings: DataFrame containing keno drawings data.

    @returns match_mask: array of high and low bits of the match,
                         hamming weight (number of spots played),
                         and date.
    '''
    number_wagered_id = x[0]
    draw_number_id = x[1]

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
        number_matched = sum(
            map(lambda x: popcount64d(x), match_mask))
        try:
            prize = PRIZE_DICT[number_played][number_matched]
            match_mask += [number_matched, prize, date]
        except KeyError:
            match_mask += [number_matched, 0, date]
    except KeyError:
        match_mask = [0, 0, 0, 0, 0]

    return match_mask


def find_winnings(tickets: pd.DataFrame,
                  numbers_wagered: pd.DataFrame,
                  drawings: pd.DataFrame) -> pd.DataFrame:
    '''
    Function to find the prize amount of each item in the
    'tickets' DataFrame.

    Most of the work is done within 'calculate_prize' function.

    @param tickets: DataFrame containing keno tickets data.
    @param numbers_wagered: DataFrame containing numbers_wagered data.
    @param drawings: DataFrame containing keno drawings data.

    @returns tickets: modified 'tickets' DataFrame.

    '''
    match_mask = tickets[["numbers_wagered_id", "draw_number_id"]]\
        .apply(lambda x: calculate_prize(x, numbers_wagered, drawings), 1)

    tickets.loc[:, "high_match_mask"] = match_mask.apply(
        lambda x: x[0], 1)
    tickets.loc[:, "low_match_mask"] = match_mask.apply(
        lambda x: x[1], 1)
    tickets.loc[:, "numbers_matched"] = match_mask.apply(
        lambda x: x[2], 1)
    tickets.loc[:, "prize"] = match_mask.apply(
        lambda x: x[3], 1)
    # tickets.loc[:, "date"] = match_mask.apply(
    #     lambda x: x[4], 1)

    return tickets


conn = sqlite3.connect(os.path.join(
    DIR_PATH,
    "keno.db"))
drawings = pd.read_csv(os.path.join(
    DIR_PATH,
    "Keno_Draw_Results.csv"))
tickets = pd.read_csv(os.path.join(
    DIR_PATH,
    "Keno_Transactions.txt"),
    sep=";")


t_tickets = process_tickets(tickets)
t_drawings = process_drawings(drawings)
t_numbers_wagered = process_numbers_wagered(t_tickets)


t_tickets = find_winnings(t_tickets, t_numbers_wagered, t_drawings)

# print(t_tickets)
# print(t_drawings)
# print(t_numbers_wagered)


t_drawings.to_sql(name="drawings",
                  con=conn,
                  schema=DRAWINGS_SCHEMA,
                  if_exists="replace",
                  index_label="id")
t_numbers_wagered.to_sql(name="numbers_wagered",
                         con=conn,
                         schema=NUMBERS_WAGERED_SCHEMA,
                         if_exists="replace",
                         index_label="id")
t_tickets.to_sql(name="tickets",
                 con=conn,
                 schema=TICKETS_SCHEMA,
                 if_exists="replace",
                 index_label="id")


# 'ix_i' table generation

# N = math.ceil(MAX_NUMBERS / MAX_BITS)
# ixs: List[List[List[int]]] = [[] for i in range(N)]

# for i in range(MAX_NUMBERS):
#     ix = i // MAX_BITS
#     ixs[ix].append([i, 1 << (i % MAX_BITS)])

# for n, j in enumerate(ixs):
#     t_ix = pd.DataFrame(j, columns=["id", "number"])
#     t_ix.to_sql(name=f"ix_{n}",
#                 con=conn,
#                 if_exists="replace",
#                 index=False)
