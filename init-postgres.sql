-- Таблица для логирования
CREATE TABLE IF NOT EXISTS event_logs (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL UNIQUE,    -- ID события из Kafka
    producer_id VARCHAR(100) NOT NULL,        -- Кто отправил
    event_type VARCHAR(50) NOT NULL,          -- Тип события
    status VARCHAR(20) NOT NULL,              -- Статус события: created, processing, completed, failed
    expected_duration INTEGER NOT NULL,       -- Ожидаемое время выполнения (мс)
    actual_duration INTEGER,                  -- Фактическое время (мс)
    created_at TIMESTAMP DEFAULT NOW(),       -- Когда создано
    processed_at TIMESTAMP,                   -- Когда обработано
    error_message TEXT                        -- Ошибка если была
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_event_logs_status_created        -- для поиска событий и обработки
    ON event_logs(status, created_at);

CREATE INDEX IF NOT EXISTS idx_event_logs_created_at_status     -- для удаления старых, например которым больше 30 дней
    ON event_logs(created_at, status);
