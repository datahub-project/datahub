from datetime import datetime
from typing import List

redshift_datetime_format = "%Y-%m-%d %H:%M:%S"


# See https://stackoverflow.com/questions/72770890/redshift-result-size-exceeds-listagg-limit-on-svl-statementtext
# for why we need to limit the size of the query text.
# We use 290 instead instead of the standard 320, because escape characters can add to the length.
_QUERY_SEQUENCE_LIMIT = 290

_MAX_COPY_ENTRIES_PER_TABLE = 20


class RedshiftCommonQuery:
    CREATE_TEMP_TABLE_CLAUSE = "create temp table"
    CREATE_TEMPORARY_TABLE_CLAUSE = "create temporary table"
    CREATE_TABLE_CLAUSE = "create table"

    @staticmethod
    def get_temp_table_clause(table_name: str) -> List[str]:
        return [
            f"{RedshiftCommonQuery.CREATE_TABLE_CLAUSE} {table_name}",
            f"{RedshiftCommonQuery.CREATE_TEMP_TABLE_CLAUSE} {table_name}",
            f"{RedshiftCommonQuery.CREATE_TEMPORARY_TABLE_CLAUSE} {table_name}",
        ]

    list_databases: str = """SELECT datname FROM pg_database
        WHERE (datname <> ('padb_harvest')::name)
        AND (datname <> ('template0')::name)
        AND (datname <> ('template1')::name)
        """

    # NOTE: although schema owner id is available in tables, we do not use it
    # as getting username from id requires access to pg_catalog.pg_user_info
    # which is available only to superusers.
    # NOTE: Need union here instead of using svv_all_schemas, in order to get
    # external platform related lineage
    # NOTE: Using database_name filter for svv_redshift_schemas, as  otherwise
    # schemas from other shared databases also show up.
    @staticmethod
    def list_schemas(database: str) -> str:
        return f"""
        SELECT 
            schema_name,
            schema_type,
            cast(null as varchar(1024)) as schema_option,
            cast(null as varchar(256)) as external_platform,
            cast(null as varchar(256)) as external_database
        FROM svv_redshift_schemas
        WHERE database_name = '{database}'
          AND schema_name != 'pg_catalog' and schema_name != 'information_schema'
    UNION ALL
        SELECT 
            schemaname as schema_name,
            'external' as schema_type,
            esoptions as schema_option,
            CASE s.eskind
                WHEN '1' THEN 'GLUE'
                WHEN '2' THEN 'HIVE'
                WHEN '3' THEN 'POSTGRES'
                WHEN '4' THEN 'REDSHIFT'
                ELSE 'OTHER'
            END as external_platform,
            databasename as external_database
        FROM SVV_EXTERNAL_SCHEMAS as s
        ORDER BY SCHEMA_NAME;
        """

    @staticmethod
    def get_database_details(database):
        return f"""\
            select 
                database_name,
                database_type, 
                database_options 
            from svv_redshift_databases 
            where database_name='{database}';"""

    # NOTE: although table owner id is available in tables, we do not use it
    # as getting username from id requires access to pg_catalog.pg_user_info
    # which is available only to superusers.
    # NOTE: Tables from shared database are not available in pg_catalog.pg_class
    @staticmethod
    def list_tables(
        database: str,
        skip_external_tables: bool = False,
        is_shared_database: bool = False,
    ) -> str:
        # NOTE: it looks like description is available only in pg_description
        # So this remains preferrred way
        tables_query = """
 SELECT  CASE c.relkind
                WHEN 'r' THEN 'TABLE'
                WHEN 'v' THEN 'VIEW'
                WHEN 'm' THEN 'MATERIALIZED VIEW'
                WHEN 'f' THEN 'FOREIGN TABLE'
            END AS tabletype,
            n.oid AS "schema_oid",
            n.nspname AS "schema",
            c.oid AS "rel_oid",
            c.relname,
            ci.relcreationtime as creation_time,
            CASE c.reldiststyle
                WHEN 0 THEN 'EVEN'
                WHEN 1 THEN 'KEY'
                WHEN 8 THEN 'ALL'
            END AS "diststyle",
            c.relowner AS "owner_id",
            null as "owner_name",
            TRIM(TRAILING ';' FROM pg_catalog.pg_get_viewdef (c.oid,TRUE)) AS "view_definition",
            pg_catalog.array_to_string(c.relacl,'\n') AS "privileges",
            NULL as "location",
            NULL as parameters,
            NULL as input_format,
            NULL As output_format,
            NULL as serde_parameters,
            pgd.description as table_description
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_class_info as ci on c.oid = ci.reloid
        LEFT JOIN pg_catalog.pg_description pgd ON pgd.objsubid = 0 AND pgd.objoid = c.oid
        WHERE c.relkind IN ('r','v','m','S','f')
        AND   n.nspname !~ '^pg_'
        AND   n.nspname != 'information_schema'
"""

        external_tables_query = f"""
        SELECT 'EXTERNAL_TABLE' as tabletype,
            NULL AS "schema_oid",
            schemaname AS "schema",
            NULL AS "rel_oid",
            tablename AS "relname",
            NULL as "creation_time",
            NULL AS "diststyle",
            NULL AS "owner_id",
            NULL AS "owner_name",
            NULL AS "view_definition",
            NULL AS "privileges",
            "location",
            parameters,
            input_format,
            output_format,
            serde_parameters,
            NULL as table_description
        FROM pg_catalog.svv_external_tables
        WHERE redshift_database_name='{database}'
        ORDER BY "schema",
                "relname"
"""
        shared_database_tables_query = f"""
        SELECT table_type as tabletype,
            NULL AS "schema_oid",
            schema_name AS "schema",
            NULL AS "rel_oid",
            table_name AS "relname",
            NULL as "creation_time",
            NULL AS "diststyle",
            table_owner AS "owner_id",
            NULL AS "owner_name",
            NULL AS "view_definition",
            table_acl AS "privileges",
            NULL as "location",
            NULL as parameters,
            NULL as input_format,
            NULL As output_format,
            NULL as serde_parameters,
            NULL as table_description
        FROM svv_redshift_tables
        WHERE database_name='{database}'
        ORDER BY "schema",
                "relname"
"""
        if is_shared_database:
            return shared_database_tables_query
        elif skip_external_tables:
            return tables_query
        else:
            return f"{tables_query} UNION {external_tables_query}"

    @staticmethod
    def list_columns(
        database_name: str, schema_name: str, is_shared_database: bool = False
    ) -> str:
        if is_shared_database:
            return f"""
            SELECT
              schema_name as "schema",
              table_name as "table_name",
              column_name as "name",
              encoding as "encode",
              -- Spectrum represents data types differently.
              -- Standardize, so we can infer types.
              data_type AS "type",
              distkey as "distkey",
              sortkey as "sortkey",
              (case when is_nullable = 'no' then TRUE else FALSE end)  as "notnull",
              null as "comment",
              null as "adsrc",
              ordinal_position as "attnum",
              data_type AS "format_type",
              column_default as "default",
              null as "schema_oid",
              null as "table_oid"
            FROM SVV_REDSHIFT_COLUMNS
            WHERE 1 and schema = '{schema_name}'
            AND database_name = '{database_name}'
            ORDER BY "schema", "table_name", "attnum"
"""
        return f"""
            SELECT
              n.nspname as "schema",
              c.relname as "table_name",
              att.attname as "name",
              format_encoding(att.attencodingtype::integer) as "encode",
              format_type(att.atttypid, att.atttypmod) as "type",
              att.attisdistkey as "distkey",
              att.attsortkeyord as "sortkey",
              att.attnotnull as "notnull",
              pg_catalog.col_description(att.attrelid, att.attnum)
                as "comment",
              adsrc,
              attnum,
              pg_catalog.format_type(att.atttypid, att.atttypmod),
              pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) as default,
              n.oid as "schema_oid",
              c.oid as "table_oid"
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n
              ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_attribute att
              ON att.attrelid = c.oid
            LEFT JOIN pg_catalog.pg_attrdef ad
              ON (att.attrelid, att.attnum) = (ad.adrelid, ad.adnum)
            WHERE n.nspname !~ '^pg_'
              AND   n.nspname != 'information_schema'
              AND att.attnum > 0
              AND NOT att.attisdropped
              and schema = '{schema_name}'
            UNION
            SELECT
              view_schema as "schema",
              view_name as "table_name",
              col_name as "name",
              null as "encode",
              col_type as "type",
              null as "distkey",
              0 as "sortkey",
              null as "notnull",
              null as "comment",
              null as "adsrc",
              null as "attnum",
              col_type as "format_type",
              null as "default",
              null as "schema_oid",
              null as "table_oid"
            FROM pg_get_late_binding_view_cols() cols(
              view_schema name,
              view_name name,
              col_name name,
              col_type varchar,
              col_num int)
            WHERE 1 and schema = '{schema_name}'
            UNION
            SELECT
              schemaname as "schema",
              tablename as "table_name",
              columnname as "name",
              null as "encode",
              -- Spectrum represents data types differently.
              -- Standardize, so we can infer types.
              external_type AS "type",
              null as "distkey",
              0 as "sortkey",
              null as "notnull",
              null as "comment",
              null as "adsrc",
              null as "attnum",
              external_type AS "format_type",
              null as "default",
              null as "schema_oid",
              null as "table_oid"
            FROM SVV_EXTERNAL_COLUMNS
            WHERE 1 and schema = '{schema_name}'
            AND redshift_database_name = '{database_name}'
            ORDER BY "schema", "table_name", "attnum"
"""

    @staticmethod
    def view_lineage_query() -> str:
        return """
                        select
                            distinct
                            srcnsp.nspname as source_schema
                            ,
                            srcobj.relname as source_table
                            ,
                            tgtnsp.nspname as target_schema
                            ,
                            tgtobj.relname as target_table
                        from
                            pg_catalog.pg_class as srcobj
                        inner join
                            pg_catalog.pg_depend as srcdep
                                on
                            srcobj.oid = srcdep.refobjid
                        inner join
                            pg_catalog.pg_depend as tgtdep
                                on
                            srcdep.objid = tgtdep.objid
                        join
                            pg_catalog.pg_class as tgtobj
                                on
                            tgtdep.refobjid = tgtobj.oid
                            and srcobj.oid <> tgtobj.oid
                        left outer join
                            pg_catalog.pg_namespace as srcnsp
                                on
                            srcobj.relnamespace = srcnsp.oid
                        left outer join
                            pg_catalog.pg_namespace tgtnsp
                                on
                            tgtobj.relnamespace = tgtnsp.oid
                        where
                            tgtdep.deptype = 'i'
                            --dependency_internal
                            and tgtobj.relkind = 'v'
                            --i=index, v=view, s=sequence
                            and tgtnsp.nspname not in ('pg_catalog', 'information_schema')
                            order by target_schema, target_table asc
                    """

    @staticmethod
    def list_late_view_ddls_query() -> str:
        return """
                SELECT
                    n.nspname AS target_schema
                    ,c.relname AS target_table
                    , COALESCE(pg_get_viewdef(c.oid, TRUE), '') AS ddl
                FROM
                    pg_catalog.pg_class AS c
                INNER JOIN
                    pg_catalog.pg_namespace AS n
                    ON c.relnamespace = n.oid
                WHERE relkind = 'v'
                and
                n.nspname not in ('pg_catalog', 'information_schema')
                """

    @staticmethod
    def alter_table_rename_query(
        db_name: str, start_time: datetime, end_time: datetime
    ) -> str:
        start_time_str: str = start_time.strftime(redshift_datetime_format)
        end_time_str: str = end_time.strftime(redshift_datetime_format)

        return f"""
            SELECT  transaction_id,
                    session_id,
                    start_time,
                    query_text
            FROM       sys_query_history SYS
            WHERE      SYS.status = 'success'
            AND        SYS.query_type = 'DDL'
            AND        SYS.database_name = '{db_name}'
            AND        SYS.start_time >= '{start_time_str}'
            AND        SYS.end_time < '{end_time_str}'
            AND        SYS.query_text ILIKE '%alter table % rename to %'
        """

    @staticmethod
    def list_copy_commands_sql(
        db_name: str, start_time: datetime, end_time: datetime
    ) -> str:
        return """\
SELECT DISTINCT
    target_schema,
    target_table,
    filename
FROM (
    SELECT
        sti."schema" AS target_schema,
        sti."table" AS target_table,
        c.file_name AS filename,
        ROW_NUMBER() OVER (
            PARTITION BY sti."schema", sti."table"
            ORDER BY si.start_time DESC
        ) AS rn
    FROM
        SYS_QUERY_DETAIL AS si
    JOIN SYS_LOAD_DETAIL AS c ON si.query_id = c.query_id
    JOIN SVV_TABLE_INFO sti ON sti.table_id = si.table_id
    WHERE
        sti.database = '{db_name}'
        AND si.start_time >= '{start_time}'
        AND si.start_time < '{end_time}'
) subquery
WHERE rn <= {_MAX_COPY_ENTRIES_PER_TABLE}
ORDER BY target_schema, target_table, filename
""".format(
            # We need the original database name for filtering
            db_name=db_name,
            start_time=start_time.strftime(redshift_datetime_format),
            end_time=end_time.strftime(redshift_datetime_format),
            _MAX_COPY_ENTRIES_PER_TABLE=_MAX_COPY_ENTRIES_PER_TABLE,
        )

    @staticmethod
    def additional_table_metadata_query() -> str:
        raise NotImplementedError

    @staticmethod
    def usage_query(start_time: str, end_time: str, database: str) -> str:
        raise NotImplementedError

    @staticmethod
    def operation_aspect_query(start_time: str, end_time: str) -> str:
        raise NotImplementedError

    @staticmethod
    def stl_scan_based_lineage_query(
        db_name: str, start_time: datetime, end_time: datetime
    ) -> str:
        raise NotImplementedError

    @staticmethod
    def list_unload_commands_sql(
        db_name: str, start_time: datetime, end_time: datetime
    ) -> str:
        raise NotImplementedError

    @staticmethod
    def temp_table_ddl_query(start_time: datetime, end_time: datetime) -> str:
        raise NotImplementedError

    @staticmethod
    def list_insert_create_queries_sql(
        db_name: str, start_time: datetime, end_time: datetime
    ) -> str:
        raise NotImplementedError

    @staticmethod
    def list_outbound_datashares() -> str:
        return """SELECT \
            share_type, \
            share_name, \
            trim(producer_namespace) as producer_namespace, \
            source_database \
        FROM svv_datashares
        WHERE share_type='OUTBOUND'\
        """

    @staticmethod
    def get_inbound_datashare(database: str) -> str:
        return f"""SELECT \
            share_type, \
            share_name, \
            trim(producer_namespace) as producer_namespace, \
            consumer_database \
        FROM svv_datashares
        WHERE share_type='INBOUND'
        AND consumer_database= '{database}'\
        """


class RedshiftProvisionedQuery(RedshiftCommonQuery):
    @staticmethod
    def additional_table_metadata_query() -> str:
        return """
            select
                ti.database,
                ti.schema,
                "table",
                size,
                tbl_rows,
                estimated_visible_rows,
                skew_rows,
                last_accessed,
                case
                    when smi.name is not null then 1
                    else 0
                end as is_materialized
            from
                pg_catalog.svv_table_info as ti
            left join (
                select
                    tbl,
                    max(endtime) as last_accessed
                from
                    pg_catalog.stl_insert
                group by
                    tbl) as la on
                (la.tbl = ti.table_id)
            left join stv_mv_info smi on
                smi.db_name = ti.database
                and smi.schema = ti.schema
                and smi.name = ti.table
                ;
        """

    @staticmethod
    def stl_scan_based_lineage_query(
        db_name: str, start_time: datetime, end_time: datetime
    ) -> str:
        return """
                        select
                            distinct cluster,
                            target_schema,
                            target_table,
                            username as username,
                            source_schema,
                            source_table,
                            querytxt as ddl,  -- TODO: this querytxt is truncated to 4000 characters
                            starttime as timestamp
                        from
                                (
                            select
                                distinct tbl as target_table_id,
                                sti.schema as target_schema,
                                sti.table as target_table,
                                sti.database as cluster,
                                query,
                                starttime
                            from
                                stl_insert
                            join SVV_TABLE_INFO sti on
                                sti.table_id = tbl
                            where starttime >= '{start_time}'
                            and starttime < '{end_time}'
                            and cluster = '{db_name}'
                                ) as target_tables
                        join ( (
                            select
                                sui.usename as username,
                                ss.tbl as source_table_id,
                                sti.schema as source_schema,
                                sti.table as source_table,
                                scan_type,
                                sq.query as query,
                                sq.querytxt as querytxt
                            from
                                (
                                select
                                    distinct userid,
                                    query,
                                    tbl,
                                    type as scan_type
                                from
                                    stl_scan
                            ) ss
                            join SVV_TABLE_INFO sti on
                                sti.table_id = ss.tbl
                            left join stl_query sq on
                                ss.query = sq.query
                            left join svl_user_info sui on
                                sq.userid = sui.usesysid
                            where
                                sui.usename <> 'rdsdb')
                        ) as source_tables
                                using (query)
                        where
                            scan_type in (1, 2, 3)
                        order by cluster, target_schema, target_table, starttime asc
                    """.format(
            # We need the original database name for filtering
            db_name=db_name,
            start_time=start_time.strftime(redshift_datetime_format),
            end_time=end_time.strftime(redshift_datetime_format),
        )

    @staticmethod
    def list_unload_commands_sql(
        db_name: str, start_time: datetime, end_time: datetime
    ) -> str:
        return """
            select
                distinct
                    sti.database as cluster,
                    sti.schema as source_schema,
                    sti."table" as source_table,
                    unl.path as filename
            from
                stl_unload_log unl
            join stl_scan sc on
                sc.query = unl.query and
                sc.starttime >= '{start_time}' and
                sc.starttime < '{end_time}'
            join SVV_TABLE_INFO sti on
                sti.table_id = sc.tbl
            where
                unl.start_time >= '{start_time}' and
                unl.start_time < '{end_time}' and
                sti.database = '{db_name}'
              and sc.type in (1, 2, 3)
            order by cluster, source_schema, source_table, filename, unl.start_time asc
            """.format(
            # We need the original database name for filtering
            db_name=db_name,
            start_time=start_time.strftime(redshift_datetime_format),
            end_time=end_time.strftime(redshift_datetime_format),
        )

    @staticmethod
    def list_insert_create_queries_sql(
        db_name: str, start_time: datetime, end_time: datetime
    ) -> str:
        return """\
with query_txt as (
    select
        query,
        pid,
        LISTAGG(case
            when LEN(RTRIM(text)) = 0 then text
            else RTRIM(text)
        end) within group (
            order by sequence
        ) as ddl
    from (
        select
            query,
            pid,
            text,
            sequence
        from
            STL_QUERYTEXT
        where
            sequence < {_QUERY_SEQUENCE_LIMIT}
        order by
            sequence
    )
    group by
        query,
        pid
)
select
    distinct tbl as target_table_id,
    sti.schema as target_schema,
    sti.table as target_table,
    sti.database as cluster,
    usename as username,
    ddl,
    sq.query as query_id,
    min(si.starttime) as timestamp,
    ANY_VALUE(pid) as session_id
from
    stl_insert as si
left join SVV_TABLE_INFO sti on
    sti.table_id = tbl
left join svl_user_info sui on
    si.userid = sui.usesysid
left join query_txt sq on
    si.query = sq.query
left join stl_load_commits slc on
    slc.query = si.query
where
        sui.usename <> 'rdsdb'
        and cluster = '{db_name}'
        and slc.query IS NULL
        and si.starttime >= '{start_time}'
        and si.starttime < '{end_time}'
group by
    target_table_id,
    target_schema,
    target_table,
    cluster,
    username,
    ddl,
    sq.query
        """.format(
            _QUERY_SEQUENCE_LIMIT=_QUERY_SEQUENCE_LIMIT,
            # We need the original database name for filtering
            db_name=db_name,
            start_time=start_time.strftime(redshift_datetime_format),
            end_time=end_time.strftime(redshift_datetime_format),
        )

    @staticmethod
    def temp_table_ddl_query(start_time: datetime, end_time: datetime) -> str:
        start_time_str: str = start_time.strftime(redshift_datetime_format)

        end_time_str: str = end_time.strftime(redshift_datetime_format)

        return rf"""-- DataHub Redshift Source temp table DDL query
select
    *
from (
    select
        session_id,
        transaction_id,
        start_time,
        userid,
        REGEXP_REPLACE(REGEXP_SUBSTR(REGEXP_REPLACE(query_text,'\\\\n','\\n'), '(CREATE(?:[\\n\\s\\t]+(?:temp|temporary))?(?:[\\n\\s\\t]+)table(?:[\\n\\s\\t]+)[^\\n\\s\\t()-]+)', 0, 1, 'ipe'),'[\\n\\s\\t]+',' ',1,'p') as create_command,
        query_text,
        row_number() over (
            partition by session_id, TRIM(query_text)
            order by start_time desc
        ) rn
    from (
        select
            pid as session_id,
            xid as transaction_id,
            starttime as start_time,
            type,
            query_text,
            userid
        from (
            select
                starttime,
                pid,
                xid,
                type,
                userid,
                LISTAGG(case
                    when LEN(RTRIM(text)) = 0 then text
                    else RTRIM(text)
                end,
                '') within group (
                    order by sequence
                ) as query_text
            from
                SVL_STATEMENTTEXT
            where
                type in ('DDL', 'QUERY')
                AND        starttime >= '{start_time_str}'
                AND        starttime < '{end_time_str}'
                AND sequence < {_QUERY_SEQUENCE_LIMIT}
            group by
                starttime,
                pid,
                xid,
                type,
                userid
            order by
                starttime,
                pid,
                xid,
                type,
                userid
                asc
        )
        where
            type in ('DDL', 'QUERY')
    )
    where
        (create_command ilike 'create temp table %'
            or create_command ilike 'create temporary table %'
            -- we want to get all the create table statements and not just temp tables if non temp table is created and dropped in the same transaction
            or create_command ilike 'create table %')
        -- Redshift creates temp tables with the following names: volt_tt_%. We need to filter them out.
        and query_text not ilike 'CREATE TEMP TABLE volt_tt_%'
        and create_command not like 'CREATE TEMP TABLE volt_tt_'
        -- We need to filter out our query and it was not possible earlier when we did not have any comment in the query
        and query_text not ilike '%https://stackoverflow.com/questions/72770890/redshift-result-size-exceeds-listagg-limit-on-svl-statementtext%'

)
where
    rn = 1
"""

    # Add this join to the sql query for more metrics on completed queries
    # LEFT JOIN svl_query_metrics_summary sqms ON ss.query = sqms.query
    # Reference: https://docs.aws.amazon.com/redshift/latest/dg/r_SVL_QUERY_METRICS_SUMMARY.html

    # this sql query joins stl_scan over table info,
    # querytext, and user info to get usage stats
    # using non-LEFT joins here to limit the results to
    # queries run by the user on user-defined tables.
    @staticmethod
    def usage_query(start_time: str, end_time: str, database: str) -> str:
        return f"""
            SELECT DISTINCT ss.userid as userid,
                   ss.query as query,
                   sui.usename as username,
                   ss.tbl as tbl,
                   sq.querytxt as querytxt,
                   sti.database as database,
                   sti.schema as schema,
                   sti.table as table,
                   sq.starttime as starttime,
                   sq.endtime as endtime
            FROM stl_scan ss
              JOIN svv_table_info sti ON ss.tbl = sti.table_id
              JOIN stl_query sq ON ss.query = sq.query
              JOIN svl_user_info sui ON sq.userid = sui.usesysid
            WHERE ss.starttime >= '{start_time}'
            AND ss.starttime < '{end_time}'
            AND sti.database = '{database}'
            AND sq.aborted = 0
            AND NOT (
                sq.querytxt LIKE 'small table validation: %'
                OR sq.querytxt LIKE 'Small table conversion: %'
                OR sq.querytxt LIKE 'padb_fetch_sample: %'
            )
            ORDER BY ss.endtime DESC;
        """.strip()

    @staticmethod
    def operation_aspect_query(start_time: str, end_time: str) -> str:
        return f"""
          (SELECT
              DISTINCT si.userid AS userid,
              si.query AS query,
              si.rows AS rows,
              sui.usename AS username,
              si.tbl AS tbl,
              sq.querytxt AS querytxt,
              sti.database AS database,
              sti.schema AS schema,
              sti.table AS table,
              sq.starttime AS starttime,
              sq.endtime AS endtime,
              'insert' AS operation_type
            FROM
              (select userid, query, sum(rows) as rows, tbl
                from stl_insert si
                where si.rows > 0
                AND si.starttime >= '{start_time}'
                AND si.starttime < '{end_time}'
                group by userid, query, tbl
              ) as si
              JOIN svv_table_info sti ON si.tbl = sti.table_id
              JOIN stl_query sq ON si.query = sq.query
              JOIN svl_user_info sui ON sq.userid = sui.usesysid
            WHERE
              sq.aborted = 0)
        UNION
          (SELECT
              DISTINCT sd.userid AS userid,
              sd.query AS query,
              sd.rows AS ROWS,
              sui.usename AS username,
              sd.tbl AS tbl,
              sq.querytxt AS querytxt,
              sti.database AS database,
              sti.schema AS schema,
              sti.table AS table,
              sq.starttime AS starttime,
              sq.endtime AS endtime,
              'delete' AS operation_type
            FROM
              (select userid, query, sum(rows) as rows, tbl
                from stl_delete sd
                where sd.rows > 0
                AND sd.starttime >= '{start_time}'
                AND sd.starttime < '{end_time}'
                group by userid, query, tbl
              ) as sd
              JOIN svv_table_info sti ON sd.tbl = sti.table_id
              JOIN stl_query sq ON sd.query = sq.query
              JOIN svl_user_info sui ON sq.userid = sui.usesysid
            WHERE
              sq.aborted = 0)
        ORDER BY
          endtime DESC
        """.strip()


class RedshiftServerlessQuery(RedshiftCommonQuery):
    # stl_insert -> SYS_QUERY_DETAIL - showing less accesses
    # stv_mv_info -> SVV_MV_INFO - not tested, seems to be fine
    @staticmethod
    def additional_table_metadata_query() -> str:
        return """
            select
                ti.database,
                ti.schema,
                "table",
                size,
                tbl_rows,
                estimated_visible_rows,
                skew_rows,
                last_accessed,
                case
                    when smi.name is not null then 1
                    else 0
                end as is_materialized
            from
                pg_catalog.svv_table_info as ti
            left join (
                SELECT
                    table_id as tbl,
                    max(end_time) as last_accessed
                FROM
                    SYS_QUERY_DETAIL
                GROUP BY
                    table_id) as la on
                (la.tbl = ti.table_id)
            left join SVV_MV_INFO smi on
                smi.database_name = ti.database
                and smi.schema_name = ti.schema
                and smi.name = ti.table
                ;
        """

    @staticmethod
    def stl_scan_based_lineage_query(
        db_name: str, start_time: datetime, end_time: datetime
    ) -> str:
        return """
            WITH queries AS (
                SELECT
                    sti.database as cluster,
                    sti.schema AS "schema",
                    sti.table AS "table",
                    qs.table_id AS table_id,
                    qs.query_id as query_id,
                    qs.step_name as step_name,
                    sui.user_name as username,
                    source,
                    MIN(qs.start_time) as "timestamp" -- multiple duplicate records with start_time increasing slightly by miliseconds
                FROM
                    SYS_QUERY_DETAIL qs
                    JOIN
                    SVV_TABLE_INFO sti ON sti.table_id = qs.table_id
                    LEFT JOIN
                    SVV_USER_INFO sui ON qs.user_id = sui.user_id
                WHERE
                    cluster = '{db_name}' AND
                    qs.user_id <> 1 AND -- this is user 'rdsdb'
                    qs.start_time >= '{start_time}' AND
                    qs.start_time < '{end_time}'
                GROUP BY cluster, "schema", "table", qs.table_id, query_id, step_name, username, source -- to be sure we are not making duplicates ourselves the list of group by must match whatever we use in "group by" and "where" of subsequent queries ("cluster" is already set to single value in this query)
            ),
            unique_query_text AS (
                SELECT
                    query_id,
                    sequence,
                    text
                FROM (
                    SELECT
                        query_id,
                        "sequence",
                        text,
                        ROW_NUMBER() OVER (
                        PARTITION BY query_id, sequence
                        ) as rn
                    FROM SYS_QUERY_TEXT
                    )
                WHERE rn = 1
            ),
            scan_queries AS (
                SELECT
                    "schema" as source_schema,
                    "table" as source_table,
                    table_id as source_table_id,
                    queries.query_id as query_id,
                    username,
                    LISTAGG(qt."text") WITHIN GROUP (ORDER BY sequence) AS query_text
                FROM
                    "queries" LEFT JOIN
                    unique_query_text qt ON qt.query_id = queries.query_id
                WHERE
                    source = 'Redshift(local)' AND
                    step_name = 'scan' AND
                    qt.sequence < 16 -- truncating query to not exceed Redshift limit on LISTAGG function (each sequence has at most 4k characters, limit is 64k, divided by 4k gives 16, starts count from 0)
                GROUP BY source_schema, source_table, source_table_id, queries.query_id, username
            ),
            insert_queries AS (
                SELECT
                    "schema" as target_schema,
                    "table" as target_table,
                    table_id as target_table_id,
                    query_id,
                    cluster,
                    min("timestamp") as "timestamp"
                FROM
                    queries
                WHERE
                    step_name = 'insert'
                GROUP BY cluster, target_schema, target_table, target_table_id, query_id
            )
            SELECT
                cluster,
                target_schema,
                target_table,
                username,
                source_schema,
                source_table,
                query_text AS ddl,
                "timestamp"
            FROM scan_queries
                JOIN insert_queries on insert_queries.query_id = scan_queries.query_id
            WHERE source_table_id <> target_table_id
            ORDER BY cluster, target_schema, target_table, "timestamp" ASC;
                    """.format(
            # We need the original database name for filtering
            db_name=db_name,
            start_time=start_time.strftime(redshift_datetime_format),
            end_time=end_time.strftime(redshift_datetime_format),
        )

    @staticmethod
    def list_unload_commands_sql(
        db_name: str, start_time: datetime, end_time: datetime
    ) -> str:
        return """
            SELECT
                DISTINCT
                sti.database as cluster,
                sti.schema as source_schema,
                sti."table" as source_table,
                unl.file_name as filename
            FROM SYS_UNLOAD_DETAIL unl
            JOIN SYS_QUERY_DETAIL qd ON
                unl.query_id = qd.query_id
            JOIN SVV_TABLE_INFO sti ON
                sti.table_id = qd.table_id
            WHERE
                qd.step_name = 'scan' AND
                unl.start_time >= '{start_time}' AND
                unl.start_time < '{end_time}' AND
                sti.database = '{db_name}'
            ORDER BY cluster, source_schema, source_table, filename, unl.start_time ASC
            """.format(
            # We need the original database name for filtering
            db_name=db_name,
            start_time=start_time.strftime(redshift_datetime_format),
            end_time=end_time.strftime(redshift_datetime_format),
        )

    # the differences vs old query are:
    # * we additionally get queries like "insert into <table> values (...)" (to be confirmed it is not a problem)
    # * querytxt do not contain newlines (to be confirmed it is not a problem)
    @staticmethod
    def list_insert_create_queries_sql(
        db_name: str, start_time: datetime, end_time: datetime
    ) -> str:
        return """
            SELECT
                DISTINCT
                cluster,
                target_schema,
                target_table,
                username,
                query_id,
                LISTAGG(CASE WHEN LEN(RTRIM(querytxt)) = 0 THEN querytxt ELSE RTRIM(querytxt) END) WITHIN GROUP (ORDER BY sequence) AS ddl,
                ANY_VALUE(session_id) AS session_id,
                starttime AS timestamp
            FROM
            (
                SELECT
                    DISTINCT
                    qd.table_id AS target_table_id,
                    sti.schema AS target_schema,
                    sti.table AS target_table,
                    sti.database AS cluster,
                    sui.user_name AS username,
                    qt."text" AS querytxt,
                    qd.query_id AS query_id,
                    qd.start_time AS starttime,
                    qt.sequence AS sequence,
                    qt.session_id AS session_id
                FROM
                    SYS_QUERY_DETAIL qd
                    JOIN SVV_TABLE_INFO sti ON sti.table_id = qd.table_id
                    LEFT JOIN SVV_USER_INFO sui ON sui.user_id = qd.user_id
                    LEFT JOIN SYS_QUERY_TEXT qt ON qt.query_id = qd.query_id
                    LEFT JOIN SYS_LOAD_DETAIL ld ON ld.query_id = qd.query_id
                WHERE
                    qd.step_name = 'insert' AND
                    sui.user_name <> 'rdsdb' AND
                    cluster = '{db_name}' AND
                    qd.start_time >= '{start_time}' AND
                    qd.start_time < '{end_time}' AND
                    qt.sequence < 16 AND -- See https://stackoverflow.com/questions/72770890/redshift-result-size-exceeds-listagg-limit-on-svl-statementtext
                    ld.query_id IS NULL -- filter out queries which are also stored in SYS_LOAD_DETAIL
                ORDER BY target_table ASC
            )
            GROUP BY cluster, query_id, target_schema, target_table, username, starttime
            ORDER BY cluster, query_id, target_schema, target_table, starttime ASC
            ;
                """.format(
            # We need the original database name for filtering
            db_name=db_name,
            start_time=start_time.strftime(redshift_datetime_format),
            end_time=end_time.strftime(redshift_datetime_format),
        )

    # when loading from s3 using prefix with a single file it produces 2 lines (for file and just directory) - also
    # behaves like this when run in the old way

    # handles "create table IF ..." statements wrong probably - "create command" field contains only "create table if" in such cases
    # also similar happens if for example table name contains special characters quoted with " i.e. "test-table1"
    # it is also worth noting that "query_type" field from SYS_QUERY_HISTORY could be probably used to improve many
    # of complicated queries in this file
    # However, note that we can't really use this query fully everywhere, despite it being simpler, because
    # the SYS_QUERY_TEXT.text field is truncated to 4000 characters and strips out linebreaks.
    @staticmethod
    def temp_table_ddl_query(start_time: datetime, end_time: datetime) -> str:
        start_time_str: str = start_time.strftime(redshift_datetime_format)

        end_time_str: str = end_time.strftime(redshift_datetime_format)

        return rf"""-- DataHub Redshift Source temp table DDL query
                    SELECT
                            *
                    FROM
                    (
                            SELECT
                                    session_id,
                                    transaction_id,
                                    start_time,
                                    userid,
                                    query_text,
                                    REGEXP_REPLACE(REGEXP_SUBSTR(REGEXP_REPLACE(query_text,'\\\\n','\\n'), '(CREATE(?:[\\n\\s\\t]+(?:temp|temporary))?(?:[\\n\\s\\t]+)table(?:[\\n\\s\\t]+)[^\\n\\s\\t()-]+)', 0, 1, 'ipe'),'[\\n\\s\\t]+',' ',1,'p') AS create_command,
                                    ROW_NUMBER() OVER (
                                    PARTITION BY session_id, query_text
                                    ORDER BY start_time DESC
                                    ) rn
                            FROM
                            (
                                    SELECT
                                            qh.session_id AS session_id,
                                            qh.transaction_id AS transaction_id,
                                            qh.start_time AS start_time,
                                            qh.user_id AS userid,
                                            LISTAGG(qt."text") WITHIN GROUP (ORDER BY sequence) AS query_text
                                    FROM
                                            SYS_QUERY_HISTORY qh
                                            LEFT JOIN SYS_QUERY_TEXT qt on qt.query_id = qh.query_id
                                    WHERE
                                            query_type IN ('DDL', 'CTAS', 'OTHER', 'COMMAND')
                                            AND qh.start_time >= '{start_time_str}'
                                            AND qh.start_time < '{end_time_str}'
                                            AND qt.sequence < 16
                                    GROUP BY qh.start_time, qh.session_id, qh.transaction_id, qh.user_id
                                    ORDER BY qh.start_time, qh.session_id, qh.transaction_id, qh.user_id ASC
                            )
                            WHERE
                                    (
                                            create_command ILIKE '%create temp table %'
                                            OR create_command ILIKE '%create temporary table %'
                                            -- we want to get all the create table statements and not just temp tables if non temp table is created and dropped in the same transaction
                                            OR create_command ILIKE '%create table %'
                                    )
                                    -- Redshift creates temp tables with the following names: volt_tt_%. We need to filter them out.
                                    AND query_text NOT ILIKE '%CREATE TEMP TABLE volt_tt_%'
                                    AND create_command NOT ILIKE '%CREATE TEMP TABLE volt_tt_%'
                    )
                    WHERE
                            rn = 1
                    ORDER BY start_time ASC
                    ;
            """

    # new approach does not include "COPY" commands
    @staticmethod
    def usage_query(start_time: str, end_time: str, database: str) -> str:
        return f"""
            SELECT
                DISTINCT
                qh.user_id as userid,
                qh.query_id as query,
                sui.user_name as username,
                qd.table_id as tbl,
                qh.query_text as querytxt, -- truncated to 4k characters, join with SYS_QUERY_TEXT to build full query using "sequence" number field
                sti.database as database,
                sti.schema as schema,
                sti.table as table,
                qh.start_time as starttime,
                qh.end_time as endtime
            FROM
                SYS_QUERY_DETAIL qd
                JOIN SVV_TABLE_INFO sti ON qd.table_id = sti.table_id
                JOIN SVV_USER_INFO sui ON sui.user_id = qd.user_id
                JOIN SYS_QUERY_HISTORY qh ON qh.query_id = qd.query_id
            WHERE
                qd.step_name = 'scan'
                AND qh.start_time >= '{start_time}'
                AND qh.start_time < '{end_time}'
                AND sti.database = '{database}'
                AND qh.status = 'success'
            ORDER BY qh.end_time DESC
            ;
        """.strip()

    @staticmethod
    def operation_aspect_query(start_time: str, end_time: str) -> str:
        return f"""
            SELECT
                DISTINCT
                qd.user_id AS userid,
                qd.query_id AS query,
                qd.rows AS rows,
                sui.user_name AS username,
                qd.table_id AS tbl,
                qh.query_text AS querytxt, -- truncated to 4k characters, join with SYS_QUERY_TEXT to build full query using "sequence" number field
                sti.database AS database,
                sti.schema AS schema,
                sti.table AS table,
                qh.start_time AS starttime,
                qh.end_time AS endtime,
                qd.step_name as operation_type
            FROM
                (
                    SELECT
                        qd.user_id,
                        qd.query_id,
                        sum(qd.output_rows) as rows,
                        qd.table_id,
                        qd.step_name
                    FROM
                        SYS_QUERY_DETAIL qd
                    WHERE
                        qd.step_name in ('insert', 'delete')
                        AND qd.start_time >= '{start_time}'
                        AND qd.start_time < '{end_time}'
                    GROUP BY qd.user_id, qd.query_id, qd.table_id, qd.step_name
                ) qd
                JOIN SVV_TABLE_INFO sti ON qd.table_id = sti.table_id
                JOIN SVV_USER_INFO sui ON sui.user_id = qd.user_id
                JOIN SYS_QUERY_HISTORY qh ON qh.query_id = qd.query_id
            WHERE
                qh.status = 'success'
            ORDER BY
              endtime DESC
        """.strip()
