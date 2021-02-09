# lottery-analysis

A series of data collecting & processing scripts for performing a set of analyses upon
the various games of the NC lottery.

#### They're in another castle.

The written analysis is located elsewhere (yet to be formalized). Once complete, it will
be linked [here]().

-   [lottery-analysis](#lottery-analysis) - [](#)
    -   [A note on number crunching](#a-note-on-number-crunching)
    -   [Cash 3, 4, 5](#cash-3-4-5)
        -   [Back testing](#back-testing)
    -   [Keno](#keno)
        -   [Initial data format](#initial-data-format)
        -   [Wager compression](#wager-compression)

## A note on number crunching

Many of the games we've analyzed feature a similar structure: the user picks a series of
numbers, and the dealer picks a series of numbers; the prize is then dependent upon how
many of the user's numbers match the dealer's. An important note: typically, the order
of the numbers is irrelevant.

Using the aforesaid information, we developed a relatively clever system to quickly
analyze, and thereupon match, a given number string: encode each number into a `n`-bit
integer, wherein each `n`-th bit encodes the presence of a number of value `n`.

For example, in some arbitrary game:

```
dealer_numbers = "1, 2, 3, 4"
user_numbers = "1, 2"

dealer_number_bits = 30; 0b11110
user_numbers_bits = 6; 0b110
```

It's trivial then to calculate how many numbers matched
([`popcount`](https://en.wikipedia.org/wiki/Hamming_weight) of the bitwise-AND of the
two numbers), and moreover, what numbers lie within a given string (useful for various
analyses done later).

Note that the above is an _`n`_-bit integer: in Python, integers are of an arbitrary
precision, but in most parts of the universe, this isn't true. The solution? Break up
the integer into parts, each of a `BIT_COUNT` size (most often, this is 64).

## Cash 3, 4, 5

As it stands, [cash345](cash345) focuses primarily on the collecting, and thereon
processing, of data from NC's [Cash 5](https://nclottery.com/Cash5). This includes a
scraper for collecting the data, an a series of post-processing scripts for manipulating
it down to a usable form.

### Back testing

Perhaps the most interesting script is [back_test.py](cash345/scripts/back_test.py):
this runs a "what-if" simulation of the following: what if you were to play the same
number combination every day, starting from some arbitrary point in time (clamped
between the start of NC's Cash 5 game and now)? How much would you stand to lose, or to
win?

An example:

```python
cash5_path = "cash345/data/cash5_winnings_1.csv"
cash5_df = pd.read_csv(cash5_path)

nums = "1, 2, 3, 4, 5"
date = "10/08/2007"

winnings = back_test(nums, cash5_df, date)
```

Which would project your winnings, playing only `1, 2, 3, 4, 5`, starting on
`10/08/2007`.

## Keno

Using information collected from the NC state lottery, herein we process and analyze a
group of ~5M wager records of NC's [Keno](https://nclottery.com/Keno) variant. We use
our usual post-processing tricks here: convert the "user" numbers (`wagers`), and the
"dealer" numbers (`drawings`) into their integral representations, & c. & c.

After processing, we store the various facets of the data into a SQLite database (though
the data medium is relatively inconsequential).

### Initial data format

A user can select to play a given number string for up to 20 dealer drawings. In place
of having then 20 rows for each repeated number, the wager data encodes this into one
row with two values demarcating the start and end wager ids (allowing the wager to be
tied back to a unique purchase). This is certainly more efficient, but to make the prize
calculations easier, we explode out these rows: so if a given row has a range from
1-`n`, we'd turn this into `n` rows with a new `wager_id`.

### Wager compression

A rather substantive optimization can be made during the processing of the wager data:
as the order of the numbers is irrelevant, and only numbers within a limited range
(`[1, 80]`) can be played, the wager data for an individual user is probabilistically
similar, with the potential for the played numbers to be duplicated an arbitrarily large
number of times; the only inextricable values are then the `date`, `wager_id`, etc.

Therefore, the numbers played, and the integral versions thereof - which consume the
majority of the row space - may be compressed using an intermediary foreign-key table.
The actual numbers played are separated out, leaving only a integer pointer into the
aforesaid table.
