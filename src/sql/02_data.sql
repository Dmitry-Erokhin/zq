CREATE OR REPLACE FUNCTION zq.open_batch(p_queue_name TEXT, p_max_batch_size INT) RETURNS INT AS
$$
BEGIN
  RAISE EXCEPTION 'Not implemented yet';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zq.enqueue(p_queue_name TEXT, p_data TEXT[]) RETURNS VOID AS
$$
BEGIN
  RAISE EXCEPTION 'Not implemented yet';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION zq.dequeue(p_queue_name TEXT) RETURNS TABLE ("timestamp" TIMESTAMP, "data" TEXT) AS
$$
BEGIN
  RAISE EXCEPTION 'Not implemented yet';
END;
$$ LANGUAGE plpgsql;