
/* The results of running this file is safe for both before and after the code change. */
BEGIN;
    LOCK TABLE kinesis_config;
    CREATE TABLE IF NOT EXISTS id_kinesis_config
    (
      id SERIAL,
      stream_name text,
      stream_type stream_type,
      stream_region text,
      aws_account bigint,
      team text,
      version int,
      contact text,
      usage text,
      consuming_library text,
      spade_config jsonb,
      last_edited_at timestamp without time zone default NOW(),
      last_changed_by text,
      dropped boolean default false,
      dropped_reason text default '',
      PRIMARY KEY(stream_name, stream_type, aws_account, version)
    );

    INSERT INTO id_kinesis_config (
      stream_name,
      stream_type,
      aws_account,
      team,
      version,
      contact,
      usage,
      consuming_library,
      spade_config,
      last_edited_at,
      last_changed_by,
      dropped,
      dropped_reason
    ) SELECT
      stream_name,
      stream_type,
      aws_account,
      team,
      version,
      contact,
      usage,
      consuming_library,
      spade_config,
      last_edited_at,
      last_changed_by,
      dropped,
      dropped_reason
    FROM kinesis_config;

    ALTER TABLE kinesis_config RENAME TO kinesis_config_old;
    ALTER TABLE id_kinesis_config RENAME TO kinesis_config;
    ALTER SEQUENCE id_kinesis_config_id_seq RENAME TO kinesis_config_id_seq;

    /* All queries below here should be rerun after the old instance is out of service to ensure
     * configs were updated. */

    /* Update all modifications of each stream to use the same id. */
    WITH latest_id AS (
        SELECT stream_name, stream_type, aws_account, max(id) as max_id
        FROM kinesis_config
        GROUP BY stream_name, stream_type, aws_account
    )
    UPDATE kinesis_config kc
    SET id=li.max_id
    FROM latest_id li
    WHERE kc.stream_name = li.stream_name AND kc.stream_type = li.stream_type AND kc.aws_account = li.aws_account;

    /* Set the region on all non-null spade configs */
    UPDATE kinesis_config SET spade_config=spade_config || '{"StreamRegion": "us-west-2"}' WHERE spade_config IS NOT NULL;

    /* Set the region on all configs */
    UPDATE kinesis_config SET stream_region = 'us-west-2';

COMMIT;
