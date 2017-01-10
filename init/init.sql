DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'action') THEN
    CREATE TYPE action AS ENUM ('add', 'delete', 'rename', 'request_drop_event', 'drop_event', 'cancel_drop_event');
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS operation
(
  event varchar,
  action action,
  name varchar,
  action_metadata jsonb,
  version int,
  ordering int,
  ts timestamp without time zone default NOW(),
  user_name varchar, -- will be 'legacy' for operations applied before this column existed. 'unknown' if user auth was disabled (like integration)
  PRIMARY KEY (event, version, ordering)
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'maintenance') THEN
    CREATE TABLE maintenance
    (
      ts timestamp without time zone default NOW(),
      is_maintenance boolean,
      reason varchar
    );
    CREATE INDEX maintenance_ts_index ON maintenance(ts);
    INSERT INTO maintenance (is_maintenance, reason) VALUES
      (false, 'initializing table');
  END IF;
END $$;
