DROP SCHEMA IF EXISTS zq CASCADE;
CREATE SCHEMA zq;

CREATE TABLE zq.queues (
  que_id                  SERIAL PRIMARY KEY,
  que_name                TEXT NOT NULL UNIQUE,
  que_batch_first_id      BIGINT,
  que_batch_after_last_id BIGINT CHECK (que_batch_after_last_id ISNULL OR que_batch_after_last_id > que_batch_first_id)
);

COMMENT ON TABLE zq.queues IS 'Main table holding information about all queues and open batches';
COMMENT ON COLUMN zq.queues.que_id IS 'Primary key and part of the name for corresponding data table';
COMMENT ON COLUMN zq.queues.que_name IS 'Name of the queue';
COMMENT ON COLUMN zq.queues.que_batch_first_id IS 'Id of first event in the current batch';
COMMENT ON COLUMN zq.queues.que_batch_after_last_id IS 'Id of event following after the last in the current batch';
