CREATE TABLE `wagers` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `draw_number_id` INT UNSIGNED,
    `begin_draw` INT,
    `end_draw` INT,
    `qp` TINYINT(1),
    `ticket_cost` INT,
    `numbers_wagered_id` INT UNSIGNED,
    `numbers_matched` INT,
    `high_match_mask` BIGINT UNSIGNED,
    `low_match_mask` BIGINT UNSIGNED,
    `prize` INT,
    PRIMARY KEY(`id`),
    FOREIGN KEY(`draw_number_id`) REFERENCES `drawings`(`id`),
    FOREIGN KEY(`numbers_wagered_id`) REFERENCES `numbers_wagered`(`id`)
);