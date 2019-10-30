import datetime
import math
import os
from typing import Any, Callable, Dict, List, Optional, Union
import textwrap
import pandas as pd
import string

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

# 64-bit OS, max integer size should be 2^63 - 1,
# so MAX_BITS is then: 64 - 1 = 63. However, each interval of bits is
# segmented at MAX_BITS + 1, therefore: 63 - 1 = 62.
# Of course, this is Python, so this is entirely arbitrary.

MAX_BITS = 62


'''
For all things bit and integer array related.
Facilitates easy bitwise operation on arbitrarily sized bit arrays.

An important distinction to be made:

When 'bit array' is used, this refers to an actual array of bits; ones and zeros,
e.g.: [0, 1, 0, 1, 0, ...].

When 'int array' is used, this refers to the integer array representation of a bit array
of length N, wherein the integer array's length is M = ~~(N / MAX_BITS) + 1.
'''


def pfac(N: int,
         fax: List[int]) -> List[int]:
    p = 2
    while p**2 <= N:
        while (N % p == 0):
            fax.append(p)
            N //= p
        p += 1

    if N != 1:
        fax.append(N)
    return fax


def max_uniform_split(N: int,
                      limit: int,
                      limit_threshold: int,
                      saved_numbers: Dict[int, List[int]]) -> List[int]:
    '''
    Wherein a number, N, is split into the maximal equally distributed intervals
    wherein each interval is of size M, where M <= limit.

    A trivial example:
        N = 80
        limit = 64

    By intuit, one may find the answer straight away: 2 intervals of 40. However,
    when dealing with numbers composed of prime factors, f_i, wherein most f_i are
    greater than the limiter value, things become complicated rather quickly.

    A non-trivial example:
        N =  537
        limit = 2

    This factorizes into:
        fax_537 = [3, 179]

    Withal, one can see that the process must continue downward with each f_i,
    resulting in a forwards recursion therein (for 179 and so forth).

    Additionally, with each subsequent recursive step, the output group
    is recursed upon again, attempting to maximize the grouping values.

    For example, if N was split into 10 groups of 2 and 20 groups of 1,
    with limiter value = 2:
        group = [10, 2, 20, 1]

    Quite obviously, this could be coalesced into a maximal variant thereupon:
        group = [30, 2, 1, 0]


    @param N: the number theretofore split.
    @param limit: the limiter value of each interval split in N
    @param limit_threshhold: the multiplier used to determine the need for recursion thereafter the first.

    @returns group: array or four elements, which may be described as thus:
        group = [group_count_1, group_size_1, group_count_2, group_size_2]
        Where N = sum(map(
                        lambda x: x[0] * x[1],
                        zip(group[::2], group[1::2]
                )))

        The atomic size of each group is then four:
        If N is prime, then it may be therefor represented:
            N = f_00
            N + 1 = f_10...f_1i
            N = f_10...f_1i - 1

            n = f_10...f_1(i - 1)
            m = f_1i

            N = nm - 1
            N = (n - 1)*m + (m - 1)
            N = [n - 1, m, 1, m - 1]

        If N is composite:
            n = f_0...f_0(i - 1)
            m = f_0i
            N = nm
            N - [1, n, 1, m]

        If m is of size > limit, this process can be applied recursively thereuntil an
        atomic value is found. The proof of this is left as an exercise to the reader ;).
    '''
    if (not (N % limit)):
        return [N // limit, limit, 1, 0]
    elif (N in saved_numbers):
        return list(saved_numbers.get(N))

    fax: List[int] = []
    pfac(N, fax)

    if (len(fax) == 1):
        N += 1
        fax = []
        pfac(N, fax)
        is_prime = True
    else:
        is_prime = False

    t = 1
    i = len(fax) - 1
    if (fax[i] > limit):
        group = max_uniform_split(fax[i], limit, limit_threshold, saved_numbers)
        i -= 1
        while (i >= 0):
            t *= fax[i]
            i -= 1
        group[0] = group[0] * t - 1 if is_prime else 0
        group[2] = group[2] * t - 1 if is_prime else 0

        if (group[2] > limit * limit_threshold):
            M = group[2] * group[3]
            tmp = max_uniform_split(M, limit, limit_threshold, saved_numbers)
            if (tmp[1] == group[1]):
                group[0] += tmp[0]
                group[2] = tmp[2]
                group[3] = tmp[3]
    else:
        group = [1, 0, 1, 0]
        while (t * fax[i] <= limit and i > 0):
            t *= fax[i]
            i -= 1
        group[1] = t
        t = 1
        while (i >= 0):
            t *= fax[i]
            i -= 1
        group[0] = t - 1 if is_prime else t
        group[3] = group[1] - 1 if is_prime else 0

    N -= 1 if is_prime else 0
    saved_numbers[N] = list(group)
    return group


def nums_to_bits(nums: str,
                 bit_length: int,
                 max_num: int,
                 delim: str = None,
                 num_length: int = None) -> List[int]:
    '''
    Converts a given number or sequence of numbers into N bit_length integers,
    where N = ceil(len(nums) / bit_length).

    The process wherewith the conversion takes place is simple: if a
    number is located within 'nums', set that numbers' bit
    (located at [N - floor(num/bit_length) + 1][num % bit_length])
    to 1, else 0.

    @param nums: string of numbers deliminated by either 'delim' or 'num_length'
    @param max_num: maximum number availed for use within 'nums'
    @param bit_length: the interval therewith the integers are sized.
    @param delim: delimiter used for 'nums'
    @param num_length: if no delimiter is provided, split 'nums' at every 'num_length' interval.

    @returns bits: array of bit flags masquerading as integers.
    '''
    arr = nums.split(delim)\
        if delim is not None\
        else textwrap.wrap(nums, num_length or -1)

    N = math.ceil(max_num / bit_length)
    bits = [0] * N

    for i in arr:
        n = int(i)
        ix = n // bit_length
        bits[ix] |= 1 << (n % bit_length)
    return bits


def nums_to_bits_small(nums: str,
                       bit_length: int,
                       max_num: int,
                       delim: str = None,
                       num_length: int = None) -> List[int]:
    bits = [0] * math.ceil(max_num / bit_length)
    def f(x: int): bits[x // bit_length] |= 1 << (x % bit_length)
    list(map(lambda x: f(int(x)), nums.split(delim) if delim else textwrap.wrap(nums, num_length or -1)))
    return bits


def bits_to_nums_small(bits: List[int],
                       bit_length: int,
                       delim: str = None) -> str:
    nums_l: List[str] = []
    def f(num, i):
        while (i != 0):
            nums_l.append(str(num)) if (i & 1 != 0) else 0
            num += 1
            i >>= 1
    list(map(lambda x: f(x[0] * bit_length, x[1]), enumerate(bits)))
    return "".join(nums_l) if delim is None else f"{delim}".join(nums_l)


def bits_to_nums(bits: List[int],
                 bit_length: int,
                 delim: str = None,
                 num_length: int = None) -> str:
    '''
    Converts an array of integers (therein an array of bit flags), into a
    sequence of numbers deliminated by 'delim' or separated by 'num_length'.

    The function simply bit shifts each number therein 'bits' by 1 until a subsequent AND
    with 1 results in a non-zero value.

    @param bits: array of bit flags masquerading as integers.
    @param delim: delimiter used for 'nums'
    @param bit_length: the interval therewith the integers are sized.
    @param num_length: if no delimiter is provided, split 'nums' at every 'num_length' interval.

    @param return: string of numbers deliminated by either 'delim' or 'num_length'
    '''
    nums = ""
    for n, i in enumerate(bits):
        num = n * bit_length
        while (i != 0):
            if (i & 1 != 0):
                nums += f"{num}" if delim is None else f"{num}{delim}"
            num += 1
            i >>= 1
    return nums if delim is None else nums[:-len(delim)]


def popcount64d(num: int):
    '''
    Computes the Hamming Weight of a given binary number.
    See https://en.wikipedia.org/wiki/Hamming_weight for more information.

    @param num: input integer for which the Hamming Weight is computed.

    @returns i: the corresponding Hamming Weight of num.
    '''
    i = 0
    while (i < num):
        num &= num - 1
        i += 1
    return i


def example1():
    spots = "1,2,3,4,5,6,7,8,9,10,11,12,80,60,63"
    drawings = "1,2,3,4,12,13,14,15,20,30,40,50,60,70,80"

    b1 = nums_to_bits_small(spots, 64, 80, ",")
    b2 = nums_to_bits_small(drawings, 64, 80, ",")
    t = list(map(lambda x: x[0] & x[1], zip(b1, b2)))
    print(b1)
    print(b2)

    bs1 = "|".join(["{:b}".format(i) for i in b1])
    bs2 = "|".join(["{:b}".format(i) for i in b2])
    bs3 = "|".join(["{:b}".format(i) for i in t])

    t1 = sum(map(popcount64d, b1))
    t2 = sum(map(popcount64d, b2))
    t3 = sum(map(popcount64d, t))

    print(t1, t2, t3)

    print(bs1)
    print(bs2)
    print(bs3)
    print(len(bs1))
    print(len(bs2))
    print(len(bs3))
    print(bits_to_nums_small(b1, 64, ","))


def test_splitting():
    for i in range(2, 10):
        for j in range(2, 10000):
            group = max_uniform_split(j, i, 2, {})
            M = sum(map(
                lambda x: x[0] * x[1],
                zip(group[::2], group[1::2])))
            # print(f"original: {j}, interval: {i} | mapped: {M}")
            # print(group)
            assert(j == M)


# example1()
