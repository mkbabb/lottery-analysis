WITH
    toast
    AS
    (
        SELECT tickets.numbers_wagered_id, numbers_wagered.number_string, tickets.numbers_matched, tickets.prize, numbers_wagered.id, numbers_wagered.numbers_played
        FROM numbers_wagered
            INNER JOIN tickets ON tickets.numbers_wagered_id = numbers_wagered.id
    )

SELECT toast.numbers_played, toast.numbers_matched, count(*) AS cnt
FROM toast
GROUP BY toast.numbers_played, toast.numbers_matched
ORDER BY numbers_played, cnt DESC
