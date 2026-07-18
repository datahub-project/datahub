-- Install postgis and pgrouting extensions for DataHub
-- This file is executed conditionally based on DATAHUB_PGGRAPH_ENABLED environment variable

DO $$
DECLARE
    postgis_installed BOOLEAN := FALSE;
    pgrouting_installed BOOLEAN := FALSE;
BEGIN
    -- First, install postgis extension if not already present
    IF NOT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'postgis'
    ) THEN
        BEGIN
            CREATE EXTENSION IF NOT EXISTS postgis;
            RAISE NOTICE 'postgis extension installed successfully';
            postgis_installed := TRUE;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'postgis extension installation failed: %', SQLERRM;
            postgis_installed := FALSE;
        END;
    ELSE
        RAISE NOTICE 'postgis extension already exists';
        postgis_installed := TRUE;
    END IF;

    -- Then, install pgrouting extension if not already present
    IF NOT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pgrouting'
    ) THEN
        BEGIN
            CREATE EXTENSION IF NOT EXISTS pgrouting;
            RAISE NOTICE 'pgrouting extension installed successfully';
            pgrouting_installed := TRUE;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'pgrouting extension installation failed: %', SQLERRM;
            pgrouting_installed := FALSE;
        END;
    ELSE
        RAISE NOTICE 'pgrouting extension already exists';
        pgrouting_installed := TRUE;
    END IF;

    -- Log the result
    IF postgis_installed AND pgrouting_installed THEN
        RAISE NOTICE 'Both postgis and pgrouting extensions are available';
    ELSE
        RAISE NOTICE 'Some extensions are not available - postgis: %, pgrouting: %', 
                    postgis_installed, pgrouting_installed;
    END IF;
END $$;
