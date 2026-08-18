CREATE TABLE IF NOT EXISTS outbox_event (
  id UUID PRIMARY KEY,
  aggregate_type VARCHAR(200) NOT NULL,
  aggregate_id VARCHAR(300) NOT NULL,
  event_type VARCHAR(300) NOT NULL,
  payload JSONB NOT NULL,
  headers JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  status VARCHAR(30) NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  locked_by VARCHAR(200),
  locked_until TIMESTAMPTZ,
  published_at TIMESTAMPTZ,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS outbox_event_poll_idx
  ON outbox_event (status, available_at, occurred_at);
CREATE INDEX IF NOT EXISTS outbox_event_cleanup_idx
  ON outbox_event (published_at) WHERE status = 'PUBLISHED';

CREATE TABLE IF NOT EXISTS inbox_message (
  consumer_name VARCHAR(300) NOT NULL,
  message_id VARCHAR(500) NOT NULL,
  status VARCHAR(30) NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  received_at TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ,
  last_error TEXT,
  PRIMARY KEY (consumer_name, message_id)
);

CREATE INDEX IF NOT EXISTS inbox_message_cleanup_idx
  ON inbox_message (completed_at) WHERE status = 'COMPLETED';
