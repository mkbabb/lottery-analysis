import datetime
import math
import os
from typing import Any, Callable, Dict, List, Optional, Union, Tuple
import csv
import pandas as pd
from utils import choose
from bit_manipulations import nums_to_bits, bits_to_nums, popcount64d


CASH5_FIELD_COUNT = 43
CASH5_PICKED_COUNT = 5
CASH5_PRIZE = 100000.0

MAX_BITS = 63

CASH5_ODDS_DICT = {CASH5_PICKED_COUNT - i:
                   choose(CASH5_PICKED_COUNT, CASH5_PICKED_COUNT - i) *
                   choose(CASH5_FIELD_COUNT - CASH5_PICKED_COUNT, i)
                   / choose(CASH5_FIELD_COUNT, CASH5_PICKED_COUNT)
                   for i in range(CASH5_PICKED_COUNT + 1)}


def detect_cash_type(cash_df: pd.DataFrame) -> int:
    return int(
        max(
            filter(lambda x: x.find("Number") != -1, cash5_df.columns)
        ).split(" ")[1]
    )


def process_cash_n(cash_df: pd.DataFrame) -> pd.DataFrame:
    cash_type = detect_cash_type(cash_df)

    if (cash_type == 5):
        max_num = CASH5_FIELD_COUNT
    else:
        max_num = 9

    def _nums_to_bits(x: pd.Series):
        nums = ",".join([str(x[f"Number {i}"])
                         for i in range(1, cash_type + 1)])

        bits = nums_to_bits(nums=nums,
                            bit_length=MAX_BITS,
                            max_num=max_num + 1,
                            delim=",")

        date = datetime.datetime.strptime(x["Date"], "%m/%d/%Y")

        d = {"epoch": int(date.strftime("%s")),
             "day": date.day,
             "month": date.month,
             "year": date.year,
             "bits": bits[0]}

        x = x.append(pd.Series(d))
        return x

    cash_df = cash_df.apply(_nums_to_bits, 1)

    return cash_df


def calc_tickets(cash_df: pd.DataFrame) -> pd.DataFrame:
    cash_type = detect_cash_type(cash_df)

    if (cash_type == 5):
        cash_df["total_tickets"] = (
            cash_df["winners_2"] / CASH5_ODDS_DICT[2] + cash_df["winners_3"] / CASH5_ODDS_DICT[3]) / 2

        cash_df["winners"] = cash_df["winners_5"] + \
            cash_df["winners_4"] + \
            cash_df["winners_3"] + cash_df["winners_2"]

        cash_df["losers"] = cash_df["total_tickets"] - cash_df["winners"]

        cash_df["total_prizes"] = \
            cash_df["winners_2"] + 5 * cash_df["winners_3"] + \
            cash_df["prize_4"] * cash_df["winners_4"] + \
            cash_df["prize_5"].str.replace(
                "^[A-Za-z]+$", "0").astype(float) * cash_df["winners_5"]

        cash_df["profit"] = cash_df["total_tickets"] - \
            cash_df["total_prizes"]

        cash_df["winners_1"] = cash_df["total_tickets"] * CASH5_ODDS_DICT[1]
        cash_df["winners_0"] = cash_df["total_tickets"] * CASH5_ODDS_DICT[0]
    else:
        pass

    return cash_df


# Be sure to process the original csv into a usable format.
cash5_df = pd.read_csv("cash345/data/cash5_winnings.csv")
t = detect_cash_type(cash5_df)
print(t)
# cash5_df = calc_tickets(cash5_df)
# cash5_df.to_csv("cash345/data/cash5_winnings_1.csv", index=False)
