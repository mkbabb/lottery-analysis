CREATE TABLE `numbers_wagered` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `number_string` text,
    `low_bits` bigint UNSIGNED NOT NULL,
    `high_bits` bigint UNSIGNED NOT NULL,
    `numbers_played` INT UNSIGNED NOT NULL,
    PRIMARY KEY (`id`, `low_bits`, `high_bits`)
) ENGINE = InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci