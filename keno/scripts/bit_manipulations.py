import math
import textwrap
from typing import *

# 64-bit OS, max integer size should be 2^63 - 1,
# so MAX_BITS is then: 64 - 1 = 63. However, each interval of bits is
# segmented at MAX_BITS + 1, therefore: 63 - 1 = 62.
# Of course, this is Python, so this is entirely arbitrary.

MAX_BITS = 62


"""
For all things bit and integer array related.
Facilitates easy bitwise operation on arbitrarily sized bit arrays.

An important distinction to be made:

When 'bit array' is used, this refers to an actual array of bits; ones and zeros,
e.g.: [0, 1, 0, 1, 0, ...].

When 'int array' is used, this refers to the integer array representation of a bit array
of length N, wherein the integer array's length is M = ~~(N / MAX_BITS) + 1.
"""


def nums_to_bits(
    nums: str,
    bit_length: int,
    max_num: int,
    delim: Optional[str] = None,
    num_length: Optional[int] = None,
) -> List[int]:
    """
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
    """
    arr = (
        nums.split(delim)
        if delim is not None
        else textwrap.wrap(nums, num_length or -1)
    )

    N = math.ceil(max_num / bit_length)
    bits = [0] * N

    for i in arr:
        n = int(i)
        ix = n // bit_length
        bits[ix] |= 1 << (n % bit_length)

    return bits


def bits_to_nums(
    bits: List[int],
    bit_length: int,
    delim: Optional[str] = None,
    num_length: Optional[int] = None,
) -> str:
    """
    Converts an array of integers (therein an array of bit flags), into a
    sequence of numbers delimited by 'delim' or separated by 'num_length'.

    The function simply bit shifts each number therein 'bits' by 1 until a subsequent AND
    with 1 results in a non-zero value.

    @param bits: array of bit flags masquerading as integers.
    @param delim: delimiter used for 'nums'
    @param bit_length: the interval therewith the integers are sized.
    @param num_length: if no delimiter is provided, split 'nums' at every 'num_length' interval.

    @param return: string of numbers delimited by either 'delim' or 'num_length'
    """
    nums = ""

    for n, i in enumerate(bits):
        num = n * bit_length

        while i != 0:
            if i & 1 != 0:
                nums += f"{num}" if delim is None else f"{num}{delim}"
            num += 1
            i >>= 1

    return nums if delim is None else nums[: -len(delim)]


def popcount64d(num: int) -> int:
    """
    Computes the Hamming Weight of a given binary number.
    See https://en.wikipedia.org/wiki/Hamming_weight for more information.

    @param num: input integer for which the Hamming Weight is computed.

    @returns i: the corresponding Hamming Weight of num.
    """
    i = 0
    while i < num:
        num &= num - 1
        i += 1
    return i


def example1():
    spots = "1,2,3,4,5,6,7,8,9,10,11,12,80,60,63"
    drawings = "1,2,3,4,12,13,14,15,20,30,40,50,60,70,80"

    b1 = nums_to_bits(spots, 64, 80, ",")
    b2 = nums_to_bits(drawings, 64, 80, ",")
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
    print(bits_to_nums(b1, 64, ","))


# example1()
