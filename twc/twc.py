import pandas as pd
import sqlite3

conn = sqlite3.connect("twc.db")


def xl_to_df(file):
    xl = pd.ExcelFile(file)
    return pd.concat([pd.read_excel(xl, i) for i in xl.sheet_names], 0)


df = pd.read_csv("test.csv")\
    .fillna(-1)
df = df.drop(df.columns[0], axis=1)
df = df.astype({i: int for i in df.columns[2:]})

df.to_sql("data", conn, if_exists="replace")

cols = [i[0] for i in conn.execute("select * from data").description]

print(df.shape)

for i in cols:
    conn.execute(f'''
    UPDATE data
        SET "{i}" = NULL
            WHERE "{i}" = -1;
    ''')

conn.commit()
conn.close()
