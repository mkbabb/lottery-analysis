from glob import iglob
import shutil
import os


PATH = "keno/data/keno_2017_2019"
OUT = "keno/data/keno_draw_data.csv"

with open(OUT, "w") as out_file:
    files = [i for i in iglob(os.path.join(PATH, '*.csv')) if "draw" in i]
    for i in files:
        with open(i, "r") as file_i:
            shutil.copyfileobj(file_i, out_file)
