CREATE OR REPLACE FUNCTION zq.open_batch(p_queue_name TEXT, p_max_batch_size INT) RETURNS INT AS
$$
DECLARE
  v_last_data_id        INT;
  v_queue_id            INT;
  v_first_batch_id      INT;
  v_after_last_batch_id INT;

BEGIN
  IF NOT p_max_batch_size > 0 THEN
    RAISE EXCEPTION 'Max batch size param should not be negative.';
  END IF;

  PERFORM * FROM zq._assert_queue_exists(p_queue_name);

  SELECT que_id, que_batch_first_id
  FROM zq.queues
  WHERE que_name = p_queue_name AND que_batch_after_last_id IS NULL
  INTO v_queue_id, v_first_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Queue "%" has already open batch.', p_queue_name;
  END IF;

  EXECUTE 'SELECT MAX(id) FROM zq.queue_' || v_queue_id INTO v_last_data_id;

  -- If no new events - return 0 without batch creation
  IF v_first_batch_id = COALESCE(v_last_data_id, v_first_batch_id) THEN
    RETURN 0;
  END IF;

  v_after_last_batch_id := least(v_first_batch_id + p_max_batch_size, v_last_data_id + 1);

  UPDATE zq.queues SET que_batch_after_last_id = v_after_last_batch_id WHERE que_id = v_queue_id;

  RETURN v_after_last_batch_id - v_first_batch_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zq.close_batch(p_queue_name TEXT) RETURNS VOID AS
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

  UPDATE zq.queues
  SET que_batch_first_id = v_after_last_batch_id, que_batch_after_last_id = NULL
  WHERE que_id = v_queue_id;

  EXECUTE 'DELETE FROM zq.queue_' || v_queue_id || ' WHERE id < ' || v_after_last_batch_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zq.cancel_batch(p_queue_name TEXT) RETURNS VOID AS
$$
DECLARE
  v_queue_id INT;

BEGIN
  PERFORM * FROM zq._assert_queue_exists(p_queue_name);

  SELECT que_id
  FROM zq.queues
  WHERE que_name = p_queue_name AND que_batch_after_last_id IS NOT NULL
  INTO v_queue_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Queue "%" does not have open batch.', p_queue_name;
  END IF;

  UPDATE zq.queues SET  que_batch_after_last_id = NULL WHERE que_id = v_queue_id;
END;
$$ LANGUAGE plpgsql;
