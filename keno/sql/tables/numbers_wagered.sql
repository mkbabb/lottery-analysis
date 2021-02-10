CREATE TABLE `numbers_wagered` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `numbers_string` TEXT,
    `high_bits` INT UNSIGNED,
    `low_bits` INT UNSIGNED,
    `numbers_played` INT UNSIGNED,
    PRIMARY KEY(`id`)
);