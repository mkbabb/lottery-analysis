import pandas as pd

df1 = pd.read_csv("cash345/data/cash5_winnings.csv")
df2 = pd.read_csv("cash345/data/NCELCash5_bits.csv").iloc[::-1].reset_index()

df3 = df1.join(df2)

df3.to_csv("cash345/data/cash5_winnings.csv", index=False)
