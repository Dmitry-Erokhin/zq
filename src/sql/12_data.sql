CREATE OR REPLACE FUNCTION zq.enqueue(p_queue_name TEXT, p_data TEXT[]) RETURNS VOID AS
$$
DECLARE
  queue_id  INT;
  line      TEXT;

BEGIN
  PERFORM * FROM zq._assert_queue_exists(p_queue_name);

  SELECT que_id FROM zq.queues WHERE que_name = p_queue_name INTO queue_id;

  FOREACH line IN ARRAY p_data LOOP
    EXECUTE 'INSERT INTO zq.queue_'|| queue_id ||'(data) VALUES (''' || line || ''')';
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zq.dequeue(p_queue_name TEXT) RETURNS TABLE ("timestamp" TIMESTAMP, "data" TEXT) AS
$$
DECLARE
  v_queue_id            INT;
  v_first_batch_id      INT;
  v_after_last_batch_id INT;

BEGIN
  PERFORM * FROM zq._assert_queue_exists(p_queue_name);

  SELECT que_id, que_batch_first_id, que_batch_after_last_id
  FROM zq.queues
  WHERE que_name = p_queue_name AND que_batch_after_last_id IS NOT NULL
  INTO v_queue_id, v_first_batch_id, v_after_last_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Queue "%" does not have open batch.', p_queue_name;
  END IF;

  RETURN QUERY
    EXECUTE 'SELECT "timestamp", "data" FROM zq.queue_' || v_queue_id || ' WHERE id >=$1 AND id < $2'
    USING v_first_batch_id, v_after_last_batch_id;
  RETURN;
END;
$$ LANGUAGE plpgsql;
