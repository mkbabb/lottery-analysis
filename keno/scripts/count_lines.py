import re
import os

expr = re.compile(".*;.*;.*;200;")


for (dirpath, dirnames, filenames) in os.walk("keno/data/keno_2017_2019"):
    files = [os.path.join(dirpath, i) for i in filenames]

n = 0
for i in files:
    with open(i, "r") as f:
        for j in f.readlines():
            if re.match(expr, j):
                n += 1
print(n)

