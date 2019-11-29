import datetime
import math
import os
from typing import Any, Callable, Dict, List, Optional, Union, Tuple
import csv
import pandas as pd
from utils import choose
from bit_manipulations import nums_to_bits, bits_to_nums, popcount64d
# Need to figure out how to have one file for bit manip.


CASH5_FIELD_COUNT = 43
CASH5_PICKED_COUNT = 5
CASH5_PRIZE = 100000.0
MAX_BITS = 63

ODDS_DICT = {CASH5_PICKED_COUNT - i:
             choose(CASH5_PICKED_COUNT, CASH5_PICKED_COUNT - i) *
             choose(CASH5_FIELD_COUNT - CASH5_PICKED_COUNT, i)
             / choose(CASH5_FIELD_COUNT, CASH5_PICKED_COUNT)
             for i in range(CASH5_PICKED_COUNT)}


PRIZE_DICT = {5: 100000, 4: 250, 3: 5, 2: 1, 1: 0}


def process_cash5(cash5_df: pd.DataFrame) -> pd.DataFrame:

    def _nums_to_bits(x: pd.Series):
        nums = ",".join([str(x[f"Number {i}"])
                         for i in range(1, CASH5_PICKED_COUNT + 1)])
        bits = nums_to_bits(nums=nums,
                            bit_length=MAX_BITS,
                            max_num=CASH5_FIELD_COUNT + 1,
                            delim=",")

        date = datetime.datetime.strptime(x["Date"], "%m/%d/%Y")

        d = {"epoch": int(date.strftime("%s")),
             "day": date.day,
             "month": date.month,
             "year": date.year,
             "bits": bits[0]}

        x = x.append(pd.Series(d))
        return x

    cash5_df = cash5_df.apply(_nums_to_bits, 1)

    return cash5_df


def back_test(nums: str,
              cash5_df: pd.DataFrame,
              date: str = "") -> pd.DataFrame:
    bits = nums_to_bits(nums=nums,
                        bit_length=MAX_BITS,
                        max_num=CASH5_FIELD_COUNT + 1,
                        delim=", ")[0]
    if (date != ""):
        _date = datetime.datetime.strptime(date, "%m/%d/%Y")
        epoch = int(_date.strftime("%s"))
        pos = (cash5_df["epoch"] == epoch).argmax()
        print(pos)
    else:
        pos = 0

    winnings = []

    def propagate_win(n: int, cash5_df: pd.DataFrame) -> float:
        propagated = True
        prize = CASH5_PRIZE

        n0 = n

        while (cash5_df.loc[n, "prize_5"] == "Rollover"):
            prize += CASH5_PRIZE / 10
            n += 1

        cash5_df.loc[n0, "prize_5"] = prize
        cash5_df.loc[n, "prize_5"] = CASH5_PRIZE

        return prize

    for n, x in cash5_df.iloc[pos:, ].iterrows():
        match = x["bits"] & bits

        if (match):
            count = popcount64d(match)
            prize = 0.0

            if (count == CASH5_PICKED_COUNT):
                print("g")
                if (x["prize_5"] == "Rollover"):
                    prize = propagate_win(n, cash5_df)
                else:
                    prize = float(x["prize_5"]) * x["winners_5"]
                    x["winners_5"] += 1
                    prize /= x["winners_5"]
            else:
                prize = PRIZE_DICT[count]

            x = x.append(
                pd.Series({"count": count, "prize": prize}))

            winnings.append(x)

    winnings_df = pd.DataFrame(winnings)
    # print(cash5_df.iloc[pos:, ])
    # print(winnings_df)
    return winnings_df


cash5_path = "cash345/data/cash5_winnings.csv"

cash5_df = pd.read_csv(cash5_path)

# Be sure to process the original csv into a usable format.
# cash5_df = process_cash5(cash5_df)
# cash5_df.to_csv("cash345/data/NCELCash5_bits.csv", index=False)

nums = "1, 2, 3, 4, 5"
date = "10/08/2007"

winnings = back_test(nums, cash5_df, date)
winnings.to_csv("cash345/data/tmp.csv")
