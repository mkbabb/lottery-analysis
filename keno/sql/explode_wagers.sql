SELECT
    tmp_wagers. *,
    drawings.id AS tmp_draw_number_id
FROM
    tmp_wagers
    LEFT JOIN drawings ON drawings.id BETWEEN tmp_wagers.begin_draw
    AND tmp_wagers.end_draw;