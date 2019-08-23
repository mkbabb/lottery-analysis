WITH
    toast
    AS
    (
        SELECT tickets.numbers_wagered_id, numbers_wagered.number_string, tickets.numbers_matched, tickets.prize,
            numbers_wagered.id, numbers_wagered.numbers_played, numbers_wagered.high_bits, numbers_wagered.low_bits,
            tickets.draw_number_id
        FROM numbers_wagered
            INNER JOIN tickets ON tickets.numbers_wagered_id = numbers_wagered.id
    ),
    waffle
    AS
    (
        FROM ix_0
        SELECT ix_0.id, ix_0.number, toast.number_string, toast.prize, toast.high_bits, toast.low_bits, toast.numbers_matched, toast.numbers_played
            INNER JOIN toast ON ix_0.number & toast.low_bits
    ),
    crepe
    AS
    (
        SELECT ix_1.id, ix_1.number, toast.number_string, toast.prize, toast.high_bits, toast.low_bits, toast.numbers_matched, toast.numbers_played
        FROM ix_1
            INNER JOIN toast ON ix_1.number & toast.high_bits
    ),
    pancake
    AS
    (
                    SELECT *
            FROM waffle
        UNION
            SELECT *
            FROM crepe
    )

SELECT pancake.id AS number, count(*) AS cnt
FROM pancake
GROUP BY pancake.id
ORDER BY cnt DESC
