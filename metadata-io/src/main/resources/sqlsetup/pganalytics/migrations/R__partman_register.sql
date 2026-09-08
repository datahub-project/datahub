-- Register pgAnalytics partitioned parents with pg_partman.
-- Register event with pg_partman. Control column: event_time.
DO $partmanreg$
DECLARE
  partman_schema text;
  parent_qual text := '__PARTMAN_PARENT_EVENT__';
  interval_txt text := '__PARTMAN_INTERVAL__';
  premake_val int := __PARTMAN_PREMAKE__;
  force_overwrite boolean := __PARTMAN_FORCE_OVERWRITE__;
  already_registered boolean;
BEGIN
  SELECT n.nspname INTO partman_schema
  FROM pg_extension e
  JOIN pg_namespace n ON n.oid = e.extnamespace
  WHERE e.extname = 'pg_partman';

  IF partman_schema IS NULL THEN
    RAISE NOTICE 'pg_partman is not installed; skipping partition registration for event';
    RETURN;
  END IF;

  IF to_regclass(parent_qual) IS NULL THEN
    RAISE EXCEPTION 'Table % does not exist', parent_qual;
  END IF;

  EXECUTE format(
    'SELECT EXISTS (SELECT 1 FROM %I.part_config WHERE parent_table = $1)',
    partman_schema
  ) INTO already_registered USING parent_qual;

  IF already_registered THEN
    IF force_overwrite THEN
      EXECUTE format(
        'UPDATE %I.part_config SET partition_interval = $1, premake = $2 WHERE parent_table = $3',
        partman_schema
      ) USING interval_txt, premake_val, parent_qual;
      EXECUTE format(
        'SELECT %I.run_maintenance($1)',
        partman_schema
      ) USING parent_qual;
    END IF;
    RETURN;
  END IF;

  EXECUTE format(
    'SELECT %I.create_parent(p_parent_table := $1, p_control := $2, p_interval := $3, p_premake := $4, p_jobmon := $5)',
    partman_schema
  ) USING parent_qual, 'event_time', interval_txt, premake_val, false;

  EXECUTE format(
    'SELECT %I.run_maintenance($1)',
    partman_schema
  ) USING parent_qual;
END
$partmanreg$;

-- Register rollup with pg_partman. Control column: bucket_start.
DO $partmanreg$
DECLARE
  partman_schema text;
  parent_qual text := '__PARTMAN_PARENT_ROLLUP__';
  interval_txt text := '__PARTMAN_INTERVAL__';
  premake_val int := __PARTMAN_PREMAKE__;
  force_overwrite boolean := __PARTMAN_FORCE_OVERWRITE__;
  already_registered boolean;
BEGIN
  SELECT n.nspname INTO partman_schema
  FROM pg_extension e
  JOIN pg_namespace n ON n.oid = e.extnamespace
  WHERE e.extname = 'pg_partman';

  IF partman_schema IS NULL THEN
    RAISE NOTICE 'pg_partman is not installed; skipping partition registration for rollup';
    RETURN;
  END IF;

  IF to_regclass(parent_qual) IS NULL THEN
    RAISE EXCEPTION 'Table % does not exist', parent_qual;
  END IF;

  EXECUTE format(
    'SELECT EXISTS (SELECT 1 FROM %I.part_config WHERE parent_table = $1)',
    partman_schema
  ) INTO already_registered USING parent_qual;

  IF already_registered THEN
    IF force_overwrite THEN
      EXECUTE format(
        'UPDATE %I.part_config SET partition_interval = $1, premake = $2 WHERE parent_table = $3',
        partman_schema
      ) USING interval_txt, premake_val, parent_qual;
      EXECUTE format(
        'SELECT %I.run_maintenance($1)',
        partman_schema
      ) USING parent_qual;
    END IF;
    RETURN;
  END IF;

  EXECUTE format(
    'SELECT %I.create_parent(p_parent_table := $1, p_control := $2, p_interval := $3, p_premake := $4, p_jobmon := $5)',
    partman_schema
  ) USING parent_qual, 'bucket_start', interval_txt, premake_val, false;

  EXECUTE format(
    'SELECT %I.run_maintenance($1)',
    partman_schema
  ) USING parent_qual;
END
$partmanreg$;

-- Register distinct_set with pg_partman. Control column: bucket_start.
DO $partmanreg$
DECLARE
  partman_schema text;
  parent_qual text := '__PARTMAN_PARENT_DISTINCT__';
  interval_txt text := '__PARTMAN_INTERVAL__';
  premake_val int := __PARTMAN_PREMAKE__;
  force_overwrite boolean := __PARTMAN_FORCE_OVERWRITE__;
  already_registered boolean;
BEGIN
  SELECT n.nspname INTO partman_schema
  FROM pg_extension e
  JOIN pg_namespace n ON n.oid = e.extnamespace
  WHERE e.extname = 'pg_partman';

  IF partman_schema IS NULL THEN
    RAISE NOTICE 'pg_partman is not installed; skipping partition registration for distinct_set';
    RETURN;
  END IF;

  IF to_regclass(parent_qual) IS NULL THEN
    RAISE EXCEPTION 'Table % does not exist', parent_qual;
  END IF;

  EXECUTE format(
    'SELECT EXISTS (SELECT 1 FROM %I.part_config WHERE parent_table = $1)',
    partman_schema
  ) INTO already_registered USING parent_qual;

  IF already_registered THEN
    IF force_overwrite THEN
      EXECUTE format(
        'UPDATE %I.part_config SET partition_interval = $1, premake = $2 WHERE parent_table = $3',
        partman_schema
      ) USING interval_txt, premake_val, parent_qual;
      EXECUTE format(
        'SELECT %I.run_maintenance($1)',
        partman_schema
      ) USING parent_qual;
    END IF;
    RETURN;
  END IF;

  EXECUTE format(
    'SELECT %I.create_parent(p_parent_table := $1, p_control := $2, p_interval := $3, p_premake := $4, p_jobmon := $5)',
    partman_schema
  ) USING parent_qual, 'bucket_start', interval_txt, premake_val, false;

  EXECUTE format(
    'SELECT %I.run_maintenance($1)',
    partman_schema
  ) USING parent_qual;
END
$partmanreg$;
