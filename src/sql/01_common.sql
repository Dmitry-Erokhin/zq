CREATE OR REPLACE FUNCTION zq._assert_queue_exists(p_queue_name TEXT) RETURNS VOID AS
$BODY$
BEGIN
  PERFORM * FROM zq.queues WHERE que_name = p_queue_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Queue "%" is not exists.', p_queue_name;
  END IF;
END
$BODY$ LANGUAGE plpgsql;
