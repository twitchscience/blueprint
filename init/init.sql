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


DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'stream_type') THEN
    CREATE TYPE stream_type AS ENUM ('stream', 'firehose');
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS kinesis_config
(
  stream_name text,
  stream_type stream_type,
  team text,
  version int,
  contact text,
  usage text,
  aws_account bigint,
  consuming_library text,
  spade_config jsonb,
  last_edited_at timestamp without time zone default NOW(),
  last_changed_by text,
  dropped boolean default false,
  dropped_reason text default '',
  PRIMARY KEY(stream_name, stream_type, aws_account, version)
);

DO $$
  BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'event_metadata_type') THEN
    CREATE TYPE event_metadata_type AS ENUM ('comment', 'edge_type');
  END IF;
END $$;

-- This tables keeps track of the only the current event metadata
CREATE TABLE IF NOT EXISTS event_metadata
(
  event varchar,
  metadata_type event_metadata_type,
  metadata_value varchar,
  ts timestamp without time zone default NOW(),
  user_name varchar,
  version int,
  PRIMARY KEY (event, metadata_type, version)
);
