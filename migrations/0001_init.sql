CREATE TABLE IF NOT EXISTS outbox_events (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    event_id        CHAR(36)      NOT NULL,
    event_type      VARCHAR(255)  NOT NULL,
    aggregate_type  VARCHAR(255)  NOT NULL,
    aggregate_id    VARCHAR(255)  NOT NULL,
    topic           VARCHAR(255)  NOT NULL,
    content_type    VARCHAR(100)  NOT NULL DEFAULT 'application/json',
    payload         LONGBLOB      NOT NULL,
    headers         JSON          NULL,
    status          TINYINT UNSIGNED NOT NULL DEFAULT 0
                        COMMENT '0=new,1=sent,2=retry,3=error,4=processing',
    attempt_count   INT UNSIGNED  NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP(6)  NULL,
    last_error      TEXT          NULL,
    created_at      TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                        ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_outbox_events_event_id (event_id),
    KEY idx_outbox_events_claim (status, created_at, id),
    KEY idx_outbox_events_aggregate (aggregate_type, aggregate_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS outbox_deadletters (
    id              BIGINT UNSIGNED PRIMARY KEY,
    event_id        CHAR(36)      NOT NULL,
    event_type      VARCHAR(255)  NOT NULL,
    aggregate_type  VARCHAR(255)  NOT NULL,
    aggregate_id    VARCHAR(255)  NOT NULL,
    topic           VARCHAR(255)  NOT NULL,
    content_type    VARCHAR(100)  NOT NULL DEFAULT 'application/json',
    payload         LONGBLOB      NOT NULL,
    headers         JSON          NULL,
    attempt_count   INT UNSIGNED  NOT NULL,
    last_error      TEXT          NULL,
    created_at      TIMESTAMP(6)  NOT NULL,
    moved_at        TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_outbox_deadletters_event_id (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
