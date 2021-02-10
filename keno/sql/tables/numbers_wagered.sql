CREATE TABLE `numbers_wagered` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `number_string` TEXT,
    `high_bits` BIGINT UNSIGNED,
    `low_bits` BIGINT UNSIGNED,
    `numbers_played` INT UNSIGNED,
    PRIMARY KEY(`id`)
);