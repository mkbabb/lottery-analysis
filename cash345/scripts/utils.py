import math
from typing import Dict, List, Callable, Any
import itertools
from functools import reduce
import random
import datetime

import numpy as np
import pandas as pd

import re

RE_WHITESPACE = re.compile("\s+")


def dollar_to_float(s: str) -> float:
    s = s.strip()
    ix = s.find("$")
    if (ix != -1):
        s = s[ix + 1:]

    try:
        return float(s.replace(",", ""))
    except TypeError:
        return 0


def sanitize_string(s: str, white_space: str = "") -> str:
    if (white_space != ""):
        s = re.sub(RE_WHITESPACE, white_space)
    return s.lower().strip()


def clamp(x, v0, v1):
    return v0 if x < v0 else v1 if x > v1 else x


def xlsx_to_df_dict(file: str) -> Dict[str, pd.DataFrame]:
    xl = pd.ExcelFile(file)
    return {i: pd.read_excel(xl, i) for i in xl.sheet_names}


def xlsx_to_df_list(file: str) -> pd.DataFrame:
    xl = pd.ExcelFile(file)
    return [pd.read_excel(xl, i) for i in xl.sheet_names]


def xlsx_to_df_grab(file: str,
                    sheet_names: List[str],
                    sanitize: bool = True) -> Dict[str, pd.DataFrame]:
    xl = pd.ExcelFile(file)

    dfs = {}

    for i in xl.sheet_names:
        sheet_name = i
        if (sanitize):
            sheet_name = sanitize_string(i)

        try:
            sheet_names.index(i)
            dfs[i] = pd.read_excel(xl, i)
        except ValueError:
            pass

    return dfs


def swap(iterable: List, ix1: int, ix2: int) -> None:
    assert(ix1 < len(iterable) and ix2 < len(iterable))
    t = iterable[ix1]
    iterable[ix1] = iterable[ix2]
    iterable[ix2] = t


def roll(iterable: List, axis: int = -1):
    if (axis == 0):
        return iterable
    elif (axis == -1):
        axis = len(iterable) - 1
    i = 0
    while (i < axis):
        swap(iterable, axis, i)
        i += 1
    return iterable


def choose(n, m):
    return math.factorial(n) / (math.factorial(n - m) * math.factorial(m))


def permutation(n, m):
    return math.factorial(n) / math.factorial(n - m)


def combinations(iterable, n):
    ixs = list(range(n))

    while (True):
        yield (tuple(iterable[k] for k in ixs))
        for j in range(n - 1, -1, -1):
            if (ixs[j] < len(iterable) + j - n):
                break
        else:
            return
        ixs[j] += 1
        for k in range(j + 1, n):
            ixs[k] = ixs[k - 1] + 1


def zero_axes_before(rept_counter, axis: int) -> bool:
    for i in range(axis):
        if i != axis:
            if rept_counter[i] != 0:
                return False
    return True


def pair_wise(seq1: list,
              seq2: list,
              func: Callable[[Any, Any], Any]) -> list:
    buff = [0] * len(seq1)
    for n, i in enumerate(seq1):
        t = func(i, seq2[n])
        buff[n] = t
    return buff


def get_strides(shape: List[int]) -> List[int]:
    N = len(shape)
    init = 1
    strides = [0] * N
    strides[0] = init

    for i in range(N - 1):
        init *= shape[i]
        strides[i + 1] = init
    return strides


def md_iter(shape, strides, repeats=None):
    mdim = len(shape)
    N = reduce(lambda x, y: x * y, shape, 1)

    if (repeats is None):
        repeats = [0] * mdim

    axis_counter = [0] * mdim
    rept_counter = [0] * mdim

    i = 0
    while (i < N):
        yield axis_counter

        if (rept_counter[0] < repeats[0]):
            rept_counter[0] += 1
        else:
            rept_counter[0] = 0
            axis_counter[0] += 1

        for j in range(1, mdim):
            if axis_counter[j - 1] >= shape[j - 1]:
                if (rept_counter[j] == repeats[j]):
                    rept_counter[j] = 0
                    axis_counter[j - 1] = 0
                    axis_counter[j] += 1
                else:
                    rept_counter[j] += 1
                    axis_counter[j - 1] = 0
        i += 1
    return


def permutations(iterable, r=0):
    n = len(iterable)
    r = n if r is None else r
    ixs = list(range(n))

    k = 0
    i = 1
    while (True):
        yield tuple(iterable[ixs[n]] for n in range(r))
        for j in range(n - 1, r - 1, -1):
            swap(ixs, j, j - 1)

        if (k == r):
            swap(ixs, 0, i)
            k = 0
            i += 1
        else:
            k += 1

        if (i > n):
            return


def product(*iters, repeats=1):
    mdim = len(iters)
    ixs = [i for i in range(mdim) for j in range(repeats)]
    shape = [len(i) for i in iters for j in range(repeats)]
    strides = get_strides(shape)

    for i in (md_iter(shape, strides)):
        yield tuple(iters[j][i[n]] for n, j in enumerate(ixs))


def pearson(u: np.ndarray, v: np.ndarray) -> float:
    cov_u = u - np.average(u)
    cov_v = v - np.average(v)

    cov_uv = np.dot(cov_u, cov_v)
    std_uv = np.sqrt(np.dot(cov_u, cov_u) * np.dot(cov_v, cov_v))

    if (std_uv != 0 or std_uv != np.nan):
        return cov_uv / std_uv
    else:
        return 0


def partition_function_P(n, k):
    if (n == k):
        return 1 + partition_function_P(n, k - 1)
    elif (k == 0 or n < 0):
        return 0
    elif(n == 0 or k == 1):
        return 1
    else:
        return partition_function_P(n, k - 1) + partition_function_P(n - k, k)


def partition_function_P_d(n, k):
    n += 1
    k += 1

    count = [[0 for i in range(n)] for j in range(k)]
    count[0][0] = 1
    for i in range(k):

        for j in range(n):
            if (j < i):
                count[i][j] = count[i - 1][j]
            else:
                count[i][j] = count[i][j - i] + count[i - 1][j]

    return count[k - 1][n - 1]


def partition_function_Q_d(n, k):
    if (k < 2):
        return 1
    n = n - int(choose(k, 2))
    return partition_function_P_d(n, k)


def partition_function_Q(n, k):
    return partition_function_P(n - int(choose(k, 2)), k)


def q_binom(S, n, k):
    count = [[0 for i in range(n + 1)] for j in range(k + 1)]
    count[0][0] = 1

    for i in range(len(S)):
        for j in range(k, 0, -1):
            for s in range(S[i], n + 1):
                count[j][s] += count[j - 1][s - S[i]]

    return count[k][n]


def sum_of_n(n):
    return int(n * (n + 1) / 2)


def sieve(n):
    if (n <= 1):
        return []
    primes = []
    primes_t = [True] * n
    primes_t[0] = primes_t[1] = False
    sqrt_n = int(math.sqrt(n))

    for p, is_prime in enumerate(primes_t):
        if (is_prime):
            primes.append(p)
            for j in range(p * p, n, p):
                primes_t[j] = False
    return primes


def falling_factorial(x, n):
    s = 1
    for i in range(n):
        s *= (x - i)
    return s


def rising_factorial(x, n):
    s = 1
    for i in range(n):
        s *= (x + i)
    return s


def zeta_2n(n, primes=None):
    if (primes is None):
        primes = sieve(20 + int(math.ceil(n / (math.log(n) - 1))))
    z = 1
    for p in primes:
        z *= (1 - math.pow(p, -2 * n))
    return 1 / z


def bernoulli_n(n, primes=None):
    if (n == 0):
        return 1
    elif (n == 1):
        return 0.5
    elif (n % 2 == 0):
        n -= 1
        if (primes is None):
            primes = sieve(20 + int(math.ceil(n / (math.log(n) - 1))))
        if (n >= 20):
            return (-1)**(n + 1) * zeta_2n(n, primes) * 4 * math.sqrt(math.pi * n) * (((2 * n) / (2 * math.pi * math.e))**(2 * n))
        else:
            return (-1)**(n + 1) * ((2 * math.factorial(2 * n)) / ((2 * math.pi)**(2 * n))) * zeta_2n(n, primes)
    else:
        return 0


def sum_of_nc(n, c):
    primes = sieve(20 + int(math.ceil(n / (math.log(n) - 1))))
    s = ((n**(c + 1)) / (c + 1)) + 0.5 * n**c
    for k in range(2, c + 1):
        s += bernoulli_n(k) / math.factorial(k) *\
            falling_factorial(c, k - 1) * n**(c - k + 1)
    return int(math.ceil(s))
