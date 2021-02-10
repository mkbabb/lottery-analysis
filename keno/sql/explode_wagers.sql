SELECT
    wagers. *,
    drawings.id AS tmp_draw_number_id
FROM
    wagers
    LEFT JOIN drawings ON drawings.id BETWEEN wagers.begin_draw
    AND wagers.end_draw;