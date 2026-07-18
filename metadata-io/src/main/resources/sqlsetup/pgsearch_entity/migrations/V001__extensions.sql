-- pgvector extension for DataHub entity search (semantic channel).
-- Executed only when postgres.pgSearch.entity.vector.enabled is true.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
        BEGIN
            CREATE EXTENSION IF NOT EXISTS vector;
            RAISE NOTICE 'vector extension installed successfully';
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'vector extension installation failed: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'vector extension already exists';
    END IF;
END $$;
