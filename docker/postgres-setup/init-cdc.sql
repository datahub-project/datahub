-- Create CDC user with necessary privileges for Debezium
-- Note: In PostgreSQL, we need to create the user first, then grant privileges

-- Create CDC user if it doesn't exist
DO
$$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'CDC_USER') THEN
        CREATE USER "CDC_USER" WITH PASSWORD 'CDC_PASSWORD';
    END IF;
END
$$;

-- Grant necessary privileges for CDC operations
-- REPLICATION privilege allows logical replication which Debezium uses
ALTER USER "CDC_USER" WITH REPLICATION;

-- Grant CONNECT privilege on the database
GRANT CONNECT ON DATABASE "DATAHUB_DB_NAME" TO "CDC_USER";

-- Connect to the specific database to grant table-level privileges
\c DATAHUB_DB_NAME

-- Grant USAGE on schema (assuming public schema)
GRANT USAGE ON SCHEMA public TO "CDC_USER";

-- Grant CREATE privilege on the database (needed for creating publications)
GRANT CREATE ON DATABASE "DATAHUB_DB_NAME" TO "CDC_USER";

-- Grant SELECT on all current tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO "CDC_USER";

-- Grant SELECT on all future tables (for tables created after this script runs)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "CDC_USER";

-- Grant permission to create publications (required for Debezium)
-- In PostgreSQL, creating publications requires either superuser or specific privileges
-- For Debezium CDC, we need to be able to create and drop publications

-- Method 1: Grant superuser temporarily (less secure but works)
ALTER USER "CDC_USER" WITH SUPERUSER;


-- Alternative Method 2: Grant owner privileges on specific tables (more secure)
-- This would require knowing the exact tables beforehand
ALTER TABLE public.metadata_aspect_v2 OWNER TO "CDC_USER";
ALTER TABLE public.metadata_aspect_v2 REPLICA IDENTITY FULL;

CREATE PUBLICATION dbz_publication FOR TABLE public.metadata_aspect_v2;