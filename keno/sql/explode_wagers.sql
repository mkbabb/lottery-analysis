WITH t_wagers AS (
    SELECT
        *
    FROM
        wagers
        INNER JOIN numbers_wagered ON wagers.numbers_wagered_id = numbers_wagered.id
)
SELECT
    *
FROM
    t_wagers
    LEFT JOIN drawings ON drawings.id BETWEEN t_wagers.begin_draw
    AND t_wagers.end_draw