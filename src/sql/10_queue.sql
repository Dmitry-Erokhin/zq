CREATE OR REPLACE FUNCTION zq.create_queue(p_queue_name TEXT) RETURNS BOOLEAN AS
$BODY$
DECLARE
  v_queue_id INT;

BEGIN
  BEGIN
    INSERT INTO zq.queues(que_name) VALUES (p_queue_name) RETURNING que_id INTO v_queue_id;
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE 'Attempt of creation existing queue %', p_queue_name;
    RETURN FALSE;
  END;

  EXECUTE $$
    CREATE TABLE zq.queue_$$ || v_queue_id || $$ (
      id          BIGSERIAL PRIMARY KEY,
      "timestamp" TIMESTAMP NOT NULL DEFAULT now(),
      "data"      TEXT
    );
    COMMENT ON TABLE zq.queue_$$ || v_queue_id || $$ IS 'Data table for the queue "$$ || p_queue_name || $$"';
  $$;

  RAISE NOTICE 'Queue % with id % and corresponding table were created ', p_queue_name, v_queue_id;

  RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zq.drop_queue(p_queue_name TEXT) RETURNS BOOLEAN AS
$BODY$
  DECLARE
    v_queue_id INT;

  BEGIN
    DELETE FROM zq.queues WHERE que_name = p_queue_name RETURNING que_id INTO v_queue_id;
    IF v_queue_id IS NULL THEN
      RAISE NOTICE 'Attempt of dropping non existing queue %', p_queue_name;
      RETURN FALSE;
    END IF;

    EXECUTE 'DROP TABLE IF EXISTS zq.queue_' || v_queue_id;

    RAISE NOTICE 'Queue % with id % and corresponding table were deleted ', p_queue_name, v_queue_id;

    RETURN TRUE;
  END;
$BODY$ LANGUAGE plpgsql;
