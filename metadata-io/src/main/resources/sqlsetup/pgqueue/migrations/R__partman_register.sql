-- Register pg_partman parent for the queue message table.
-- Tokens: __PARTMAN_PARENT_QUALIFIED__, __PARTMAN_INTERVAL__, __PARTMAN_PREMAKE__

DO $partmanreg$
DECLARE
  partman_schema text;
  parent_qual text := '__PARTMAN_PARENT_QUALIFIED__';
  interval_txt text := '__PARTMAN_INTERVAL__';
  premake_val int := __PARTMAN_PREMAKE__;
  already_registered boolean;
BEGIN
  SELECT n.nspname INTO partman_schema
  FROM pg_extension e
  JOIN pg_namespace n ON n.oid = e.extnamespace
  WHERE e.extname = 'pg_partman';

  IF partman_schema IS NULL THEN
    RAISE NOTICE 'pg_partman is not installed; skipping partition registration for pgQueue message table';
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
    RETURN;
  END IF;

  EXECUTE format(
    'SELECT %I.create_parent(p_parent_table := $1, p_control := $2, p_interval := $3, p_premake := $4, p_jobmon := $5)',
    partman_schema
  ) USING parent_qual, 'enqueued_at', interval_txt, premake_val, false;

  EXECUTE format(
    'SELECT %I.run_maintenance($1)',
    partman_schema
  ) USING parent_qual;
END
$partmanreg$;
