CREATE TABLE `drawings` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `date` DATETIME,
    `high_bits` BIGINT UNSIGNED NOT NULL,
    `low_bits` BIGINT UNSIGNED NOT NULL,
    `number_string` TEXT,
    PRIMARY KEY(`id`)
);