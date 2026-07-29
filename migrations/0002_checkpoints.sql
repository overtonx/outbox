CREATE TABLE IF NOT EXISTS outbox_checkpoints (
    lock_name         VARCHAR(255)  NOT NULL PRIMARY KEY,
    last_confirmed_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at        TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                          ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
