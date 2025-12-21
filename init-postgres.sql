CREATE TABLE IF NOT EXISTS event_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    task_key VARCHAR(100) NOT NULL UNIQUE,          -- из сообщения kafka (partition:offset)
    event_name VARCHAR(100) NOT NULL,               -- из сообщения (Event.Name)
    event_type VARCHAR(50) NOT NULL,                -- из сообщения (Event.Type)
    event_number INTEGER NOT NULL,                  -- из сообщения (Event.Number)
    event_message TEXT NOT NULL,                    -- из сообщения (Event.Msg)
    processing_duration_ms BIGINT NOT NULL,         -- из сообщения (Event.Duration)

    created_at TIMESTAMPTZ NOT NULL,                -- из сообщения (Event.CreatedAt)
    db_created_at TIMESTAMPTZ DEFAULT NOW(),        -- когда записали в БД
    processed_at TIMESTAMPTZ,                       -- когда обработали

    status VARCHAR(20) NOT NULL DEFAULT 'pending'   -- статус обработки: pending, processing, completed, failed
);


-- Индексы
CREATE INDEX IF NOT EXISTS idx_event_logs_status_created        -- для поиска событий и обработки
    ON event_logs(status, created_at);

CREATE INDEX IF NOT EXISTS idx_event_logs_created_at_status     -- для удаления старых, например которым больше 2 дней
    ON event_logs(created_at, status);
