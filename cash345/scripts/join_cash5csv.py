import pandas as pd
import datetime


def get_weekday(x: pd.Series) -> int:
    date = datetime.datetime.fromtimestamp(x["epoch"])
    return date.weekday()


df1 = pd.read_csv("cash345/data/cash5_winnings.csv")

df1["weekday"] = df1.apply(get_weekday, 1)
df1.to_csv("cash345/data/cash5_winnings.csv")

# df2 = pd.read_csv("cash345/data/NCELCash5_bits.csv").iloc[::-1].reset_index()

# df3 = df1.join(df2)

# df3.to_csv("cash345/data/cash5_winnings.csv", index=False)
