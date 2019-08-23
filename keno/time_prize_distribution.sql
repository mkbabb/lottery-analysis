WITH
    toast
    AS
    (
        SELECT numbers_wagered.number_string, numbers_wagered.high_bits, numbers_wagered.low_bits, numbers_wagered.numbers_played, tickets.numbers_matched, tickets.prize, tickets.draw_number_id
        FROM numbers_wagered
            INNER JOIN tickets ON tickets.numbers_wagered_id = numbers_wagered.id
    ),
    waffle
    AS
    (
        SELECT toast.draw_number_id, drawings.id, toast.number_string, drawings.numbers_winning, toast.numbers_matched, toast.numbers_played, toast.prize, datetime(drawings.date, 'unixepoch') AS isotime
        FROM drawings
            INNER JOIN toast ON toast.draw_number_id = drawings.id
    ),

    crepe
    AS
    (
        SELECT *, strftime('%Y', waffle.isotime) AS year, strftime('%m', waffle.isotime) AS month, strftime('%d', waffle.isotime) AS day, strftime('%H', waffle.isotime) AS hour, strftime('%M', waffle.isotime) AS minute
        FROM waffle
    )
SELECT crepe.hour, crepe.minute, sum(crepe.prize), count(*),  sum(case when crepe.prize != 0 then 1 else 0 end) as winners, sum(case when crepe.prize = 0 then 1 else 0 end) as losers
FROM crepe
GROUP BY crepe.hour, crepe.minute
ORDER BY crepe.hour