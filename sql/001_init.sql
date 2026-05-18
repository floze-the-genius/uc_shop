CREATE TABLE IF NOT EXISTS orders (
  id BIGSERIAL PRIMARY KEY,
  category TEXT NOT NULL,
  telegram_id TEXT,
  status TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  result JSONB,
  attempts INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT NOT NULL,
  last_error TEXT,
  processing_owner TEXT,
  processing_started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_idempotency_key
  ON orders (idempotency_key);

CREATE INDEX IF NOT EXISTS idx_orders_status_created
  ON orders (status, created_at);

CREATE INDEX IF NOT EXISTS idx_orders_processing_started
  ON orders (status, processing_started_at);

CREATE TABLE IF NOT EXISTS order_events_outbox (
  id BIGSERIAL PRIMARY KEY,
  event_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  event_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  payload JSONB NOT NULL,
  publish_after TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_by TEXT,
  locked_at TIMESTAMPTZ,
  published_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_event_id
  ON order_events_outbox (event_id);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
  ON order_events_outbox (publish_after, id)
  WHERE published_at IS NULL;
