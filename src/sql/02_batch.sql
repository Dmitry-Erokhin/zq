CREATE OR REPLACE FUNCTION zq.open_batch(p_queue_name TEXT, p_max_batch_size INT) RETURNS INT AS
$$
BEGIN
  RAISE EXCEPTION 'Not implemented yet';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zq.close_batch(p_queue_name TEXT) RETURNS VOID AS
$$
BEGIN
  RAISE EXCEPTION 'Not implemented yet';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zq.cancel_batch(p_queue_name TEXT) RETURNS VOID AS
$$
BEGIN
  RAISE EXCEPTION 'Not implemented yet';
END;
$$ LANGUAGE plpgsql;
