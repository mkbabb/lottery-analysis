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
    )
	select * from waffle