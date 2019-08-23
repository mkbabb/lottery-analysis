WITH
    toast
    AS
    (
        select drawings.numbers_winning as number_string, drawings.high_bits, drawings.low_bits
		from drawings
    ),
    waffle
    AS
    (
        SELECT ix_0.id, ix_0.number, toast.number_string, toast.high_bits, toast.low_bits
        FROM ix_0
            INNER JOIN toast ON ix_0.number & toast.low_bits
    ),
    crepe
    AS
    (
        SELECT ix_1.id, ix_1.number, toast.number_string, toast.high_bits, toast.low_bits
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

SELECT pancake.id as number, count(*) AS cnt
FROM pancake
GROUP BY pancake.id
ORDER BY number
