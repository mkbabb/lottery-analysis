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
MAX_BITS = 63

ODDS_DICT = {CASH5_PICKED_COUNT - i:
             choose(CASH5_PICKED_COUNT, CASH5_PICKED_COUNT - i) *
             choose(CASH5_FIELD_COUNT - CASH5_PICKED_COUNT, i)
             / choose(CASH5_FIELD_COUNT, CASH5_PICKED_COUNT)
             for i in range(CASH5_PICKED_COUNT)}

PRIZE_DICT = {5: 100000, 4: 250, 3: 5, 2: 1, 1: 0}


def back_test(nums: List[str], cash5_df: pd.DataFrame):
    bits = nums_to_bits(nums=",".join(map(str, nums)),
                        bit_length=MAX_BITS,
                        max_num=CASH5_FIELD_COUNT + 1,
                        delim=",")[0]

    winnings = []

    def _back_test(x: pd.Series):
        match = x["bits"] & bits
        if (match):
            count = popcount64d(match)

            x = x.append(
                pd.Series({"count": count, "prize": PRIZE_DICT[count]}))

            winnings.append(x)

    cash5_df["bits"] = cash5_df.apply(_back_test, 1)

    return pd.DataFrame(winnings)


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


cash5_path = "cash345/data/NCELCash5_bits.csv"

cash5_df = pd.read_csv(cash5_path, dtype={
                       f"Number {i}": int for i in range(1, CASH5_PICKED_COUNT + 1)})
# cash5_df = process_cash5(cash5_df)

nums = [5, 6, 7, 8, 9]

winnings = back_test(nums, cash5_df)


# cash5_df.to_csv("cash345/data/NCELCash5_bits.csv", index=False)
