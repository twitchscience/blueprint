DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'action') THEN
    CREATE TYPE action AS ENUM ('add', 'delete');
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS operation
(
  event varchar,
  action action,
  inbound varchar,
  outbound varchar,
  column_type varchar,
  column_options varchar,
  version int,
  ordering int,
  PRIMARY KEY (event, version, ordering)
);
