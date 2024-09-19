BEGIN;
IF OBJECT_ID('tempdb.dbo.#ProceduresDependencies', 'U') IS NOT NULL
    DROP TABLE #ProceduresDependencies;

CREATE TABLE #ProceduresDependencies(
    id                       INT NOT NULL IDENTITY(1,1) PRIMARY KEY
  , procedure_id             INT
  , current_db               NVARCHAR(128)
  , procedure_schema         NVARCHAR(128)
  , procedure_name           NVARCHAR(128)
  , type                     VARCHAR(2)
  , referenced_server_name   NVARCHAR(128)
  , referenced_database_name NVARCHAR(128)
  , referenced_schema_name   NVARCHAR(128)
  , referenced_entity_name   NVARCHAR(128)
  , referenced_id            INT
  , referenced_object_type   VARCHAR(2)
  , is_selected              INT
  , is_updated               INT
  , is_select_all            INT
  , is_all_columns_found     INT
    );

BEGIN TRY
        WITH dependencies
             AS (
                 SELECT
                        pro.object_id              AS procedure_id
                      , db_name()                  AS current_db
                      , schema_name(pro.schema_id) AS procedure_schema
                      , pro.NAME                   AS procedure_name
                      , pro.type                   AS type
                      , p.referenced_id            AS referenced_id
                      , CASE
                              WHEN p.referenced_server_name IS NULL AND p.referenced_id IS NOT NULL
                              THEN @@SERVERNAME
                              ELSE p.referenced_server_name
                        END                        AS referenced_server_name
                      , CASE
                              WHEN p.referenced_database_name IS NULL AND p.referenced_id IS NOT NULL
                              THEN db_name()
                              ELSE p.referenced_database_name
                        END                        AS referenced_database_name
                      , CASE
                              WHEN p.referenced_schema_name IS NULL AND p.referenced_id IS NOT NULL
                              THEN schema_name(ref_obj.schema_id)
                              ELSE p.referenced_schema_name
                        END                        AS referenced_schema_name
                      , p.referenced_entity_name   AS referenced_entity_name
                      , ref_obj.type               AS referenced_object_type
                      , p.is_selected              AS is_selected
                      , p.is_updated               AS is_updated
                      , p.is_select_all            AS is_select_all
                      , p.is_all_columns_found     AS is_all_columns_found
                 FROM   sys.procedures AS pro
                        CROSS apply sys.dm_sql_referenced_entities(
                                    Concat(schema_name(pro.schema_id), '.', pro.NAME),
                                    'OBJECT') AS p
                        LEFT JOIN sys.objects AS ref_obj
                            ON p.referenced_id = ref_obj.object_id
        )
        INSERT INTO #ProceduresDependencies (
                                     procedure_id
                                   , current_db
                                   , procedure_schema
                                   , procedure_name
                                   , type
                                   , referenced_server_name
                                   , referenced_database_name
                                   , referenced_schema_name
                                   , referenced_entity_name
                                   , referenced_id
                                   , referenced_object_type
                                   , is_selected
                                   , is_updated
                                   , is_select_all
                                   , is_all_columns_found)
        SELECT DISTINCT
            d.procedure_id
          , d.current_db
          , d.procedure_schema
          , d.procedure_name
          , d.type
          , d.referenced_server_name
          , d.referenced_database_name
          , d.referenced_schema_name
          , d.referenced_entity_name
          , d.referenced_id
          , d.referenced_object_type
          , d.is_selected
          , d.is_updated
          , d.is_select_all
          , d.is_all_columns_found
        FROM dependencies AS d;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() = 2020 or ERROR_NUMBER() = 942
        PRINT ERROR_MESSAGE()
    ELSE
        THROW;
END CATCH;

DECLARE @id INT;
DECLARE @referenced_server_name NVARCHAR(128);
DECLARE @referenced_database_name NVARCHAR(128);
DECLARE @referenced_id INT;
DECLARE @referenced_object_type VARCHAR(2);
DECLARE @synonym_base_object_db NVARCHAR(128);
DECLARE @referenced_info TABLE (
    id                INT
  , referenced_type   VARCHAR(2)
  , referenced_schema NVARCHAR(128)
  , referenced_name   NVARCHAR(128)
  );
DECLARE @synonym_info TABLE (
    id              INT
  , referenced_db_name NVARCHAR(128)
  , referenced_schema_name NVARCHAR(128)
  , referenced_name NVARCHAR(1035)
  , referenced_type VARCHAR(2)
  , referenced_object_id INT
  );
DECLARE @SQL NVARCHAR(MAX);


DECLARE proc_depend_cursor CURSOR FOR
SELECT
    id
  , referenced_database_name
  , referenced_id
  , referenced_object_type
FROM #ProceduresDependencies;

OPEN proc_depend_cursor
FETCH NEXT FROM proc_depend_cursor INTO
    @id
  , @referenced_database_name
  , @referenced_id
  , @referenced_object_type;
WHILE @@FETCH_STATUS = 0
BEGIN;

    IF @referenced_id IS NOT NULL
        BEGIN
            BEGIN TRY
                SET @SQL = 'SELECT
                            ' + CAST(@id AS NVARCHAR(10)) + '
                              , o.type
                              , s.name
                              , o.name
                            FROM ' + COALESCE(QUOTENAME(@referenced_database_name), '') + '.sys.objects AS o
                            INNER JOIN ' + COALESCE(QUOTENAME(@referenced_database_name), '') + '.sys.schemas AS s
                                ON o.schema_id = s.schema_id
                            WHERE o.object_id = ' + CAST(@referenced_id AS NVARCHAR(10)) + ';';

                INSERT INTO  @referenced_info(
                    id
                  , referenced_type
                  , referenced_schema
                  , referenced_name
                    )
                EXEC sp_executesql @SQL;

                UPDATE #ProceduresDependencies
                SET
                    referenced_object_type = ri.referenced_type
                  , referenced_schema_name = ri.referenced_schema
                  , referenced_entity_name = ri.referenced_name
                FROM #ProceduresDependencies pd
                INNER JOIN @referenced_info ri
                    ON ri.id = pd.id
                WHERE CURRENT OF proc_depend_cursor;

                SET @referenced_object_type = (
                    SELECT
                        referenced_type
                    FROM @referenced_info
                    WHERE id = @id
                    );
            END TRY
            BEGIN CATCH
                PRINT ERROR_MESSAGE()
            END CATCH;
    END;
				
    IF @referenced_object_type = 'SN'
        BEGIN
            BEGIN TRY
          	    SET @SQL  = 'SELECT
                                 @synonym_base_object_db = CASE 
	                                                           WHEN LEN(synon.base_object_name) - LEN(REPLACE(synon.base_object_name, ''.'', '''')) = 2
	                                                           THEN REPLACE(REPLACE(SUBSTRING(synon.base_object_name, 0, CHARINDEX(''.'', synon.base_object_name)), ''['', ''''), '']'', '''')
	                                                       END
                            FROM ' + COALESCE(QUOTENAME(@referenced_database_name), '') + '.sys.synonyms AS synon
                            WHERE synon.object_id = ' + CAST(@referenced_id AS NVARCHAR(10)) + ';';
          	  
          	    EXEC sp_executesql @SQL,
          	         N'@synonym_base_object_db NVARCHAR(128) OUT', 
          	         @synonym_base_object_db = @synonym_base_object_db out;
          	  
          	    SET @SQL  = 'SELECT
                            ' + CAST(@id as NVARCHAR(10)) + '
                              , ''' + COALESCE(@synonym_base_object_db, @referenced_database_name) + '''
                              , schem.name
                              , syn_obj.name
                              , syn_obj.type
                              , syn_obj.object_id
                            FROM ' + COALESCE(QUOTENAME(@referenced_database_name), '') + '.sys.synonyms AS synon
                            INNER JOIN ' + COALESCE(QUOTENAME(@synonym_base_object_db), QUOTENAME(@referenced_database_name)) + '.sys.objects  AS syn_obj
                                  ON OBJECT_ID(synon.base_object_name) = syn_obj.object_id
                            INNER JOIN ' + COALESCE(QUOTENAME(@synonym_base_object_db), QUOTENAME(@referenced_database_name)) + '.sys.schemas AS schem
                                  ON syn_obj.schema_id = schem.schema_id
                            WHERE synon.object_id = ' + CAST(@referenced_id AS NVARCHAR(10)) + ';';
              
                INSERT INTO @synonym_info(
                    id
                  , referenced_db_name
                  , referenced_schema_name
                  , referenced_name
                  , referenced_type
                  , referenced_object_id
                    )
                EXEC sp_executesql @SQL;
			          
                UPDATE #ProceduresDependencies
                SET
                    referenced_database_name = si.referenced_db_name
	              , referenced_schema_name   = si.referenced_schema_name
                  , referenced_object_type   = si.referenced_type
                  , referenced_entity_name   = si.referenced_name
                  , referenced_id            = si.referenced_object_id
                FROM #ProceduresDependencies pd
                INNER JOIN @synonym_info si
                    ON si.id = pd.id
                WHERE CURRENT OF proc_depend_cursor;
             
            END TRY
            BEGIN CATCH
                PRINT ERROR_MESSAGE()
            END CATCH;
    END;

    FETCH NEXT FROM proc_depend_cursor INTO
        @id
      , @referenced_database_name
      , @referenced_id
      , @referenced_object_type;
            
END;

CLOSE proc_depend_cursor;
DEALLOCATE proc_depend_cursor;
END;