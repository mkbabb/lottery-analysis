import pandas as pd
import datetime


scraped_cash5_path = "cash345/data/cash5_scraped.csv"
ncel_cash5_bits_path = "cash345/data/NCELCash5_bits.csv"
out_path = "cash345/data/joined.csv"

scraped_df = pd.read_csv(scraped_cash5_path)
ncel_cash5_df_processed = pd.read_csv(ncel_cash5_bits_path)

out_df = scraped_df\
    .merge(ncel_cash5_df_processed,
           left_on="date",
           right_on="Date",
           how="left")\
    .drop("Date", axis=1)

out_df.to_csv(out_path, index=False)
