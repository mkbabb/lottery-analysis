WITH
    toast_low
    AS
    (
        SELECT ix_0.id, ix_0.number, numbers_wagered.number_string, numbers_wagered.high_bits, numbers_wagered.low_bits, numbers_wagered.numbers_played
        FROM ix_0
            INNER JOIN numbers_wagered ON numbers_wagered.low_bits & ix_0.number
    ),
    toast_high
    AS
    (
        SELECT ix_1.id, ix_1.number, numbers_wagered.number_string, numbers_wagered.high_bits, numbers_wagered.low_bits, numbers_wagered.numbers_played
        FROM ix_1
            INNER JOIN numbers_wagered ON numbers_wagered.high_bits & ix_1.number
    ),
    waffle
    AS
    (
                    SELECT *
            FROM toast_low
        UNION
            SELECT *
            FROM toast_high
    )
SELECT *
FROM waffle
GROUP BY id
