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

        won = cash5_df.loc[n0, "jackpot"]

        while (cash5_df.loc[n, "prize_5"] == "Rollover"):
            prize += CASH5_PRIZE / 10
            n += 1

        cash5_df.loc[n0, "prize_5"] = won
        cash5_df.loc[n, "prize_5"] = prize

        return won

    for n, x in cash5_df.iloc[pos:, ].iterrows():
        match = x["bits"] & bits

        if (match):
            count = popcount64d(match)
            prize = 0.0

            if (count == CASH5_PICKED_COUNT):
                if (x["prize_5"] == "Rollover"):
                    prize = propagate_win(n, cash5_df)
                else:
                    prize = float(x["prize_5"]) * x["winners_5"]
                    x["winners_5"] += 1
                    prize /= x["winners_5"]
            else:
                prize = PRIZE_DICT[count]

            x = x.append(
                pd.Series({"count": count,
                           "prize": prize}))

            winnings.append(x)

    winnings_df = pd.DataFrame(winnings)
    return winnings_df


cash5_path = "cash345/data/cash5_winnings.csv"

cash5_df = pd.read_csv(cash5_path)


nums = "1, 2, 3, 4, 5"
date = "10/08/2007"

winnings = back_test(nums, cash5_df, date)
winnings.to_csv("cash345/data/tmp.csv")
