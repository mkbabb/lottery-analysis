WITH
    toast
    AS
    (
        SELECT tickets.numbers_wagered_id, numbers_wagered.number_string, tickets.numbers_matched, tickets.prize, numbers_wagered.id, numbers_wagered.numbers_played
        FROM numbers_wagered
            INNER JOIN tickets ON tickets.numbers_wagered_id = numbers_wagered.id
    )
,
    waffle
    AS
    (
        SELECT toast.numbers_played, toast.numbers_matched, count(*) AS cnt
        FROM toast
        WHERE toast.prize = 0
        GROUP BY toast.numbers_played
    ),
    crepe
    AS
    (
        SELECT toast.numbers_played, toast.numbers_matched, count(*) AS cnt
        FROM toast
        WHERE toast.prize > 0
        GROUP BY toast.numbers_played
    ),
    pancake
    AS
    (
        SELECT waffle.numbers_played, waffle.cnt AS losers, crepe.cnt AS winners
        FROM waffle
            INNER JOIN crepe ON crepe.numbers_played = waffle.numbers_played
    )
SELECT *, pancake.losers+pancake.winners AS total_count
FROM pancake
