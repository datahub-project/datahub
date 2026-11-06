"""ODBC hierarchical navigation: per-platform upstream URN shapes, built end to end
through the M-Query parser. Pure parsing, so these run as unit tests."""

from typing import List, Optional, Tuple

import pytest

import datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes as powerbi_data_classes
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.m_query import parser
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    DataPlatformTable,
    Lineage,
)


def get_default_instances(
    override_config: Optional[dict] = None,
) -> Tuple[
    PipelineContext, PowerBiDashboardSourceConfig, AbstractDataPlatformInstanceResolver
]:
    config = PowerBiDashboardSourceConfig.model_validate(
        {
            "tenant_id": "fake",
            "client_id": "foo",
            "client_secret": "bar",
            "enable_advance_lineage_sql_construct": False,
            "extract_column_level_lineage": False,
            **(override_config or {}),
        }
    )
    return (
        PipelineContext(run_id="fake"),
        config,
        create_dataplatform_instance_resolver(config),
    )


def combine_upstreams_from_lineage(lineage: List[Lineage]) -> List[DataPlatformTable]:
    return [upstream for item in lineage for upstream in item.upstreams]


# MySQL ODBC navigation exposing Database + Table.
_ODBC_MYSQL_NAV_DATABASE_TABLE = (
    'let\n    Source = Odbc.DataSource("driver={MySQL ODBC 9.2 Unicode Driver};'
    'server=10.1.10.1;database=employees;dsn=testdb01", [HierarchicalNavigation=true]),\n'
    '    employees_Database = Source{[Name="employees",Kind="Database"]}[Data],\n'
    '    employees_Table = employees_Database{[Name="employees",Kind="Table"]}[Data]\n'
    "in\n    employees_Table"
)


# BigQuery ODBC hierarchical navigation that surfaces only dataset.table. The
# Simba driver maps projects to catalogs, so its navigation normally starts at
# Kind="Database"; this covers a driver that reports no catalog. A 3-tier
# project.dataset.table URN cannot be built from navigation alone.
_ODBC_BIGQUERY_NAV_MISSING_PROJECT = (
    'let\n    Source = Odbc.DataSource("driver={Simba ODBC Driver};dsn=bq_analytics", '
    "[HierarchicalNavigation=true]),\n"
    '    analytics_ds_Schema = Source{[Name="analytics_ds",Kind="Schema"]}[Data],\n'
    '    events_Table = analytics_ds_Schema{[Name="events",Kind="Table"]}[Data]\n'
    "in\n    events_Table"
)

# BigQuery ODBC navigation that surfaces only the leaf table (no dataset, no
# project). Both higher levels must come from dsn_to_database_schema.
_ODBC_BIGQUERY_NAV_TABLE_ONLY = (
    'let\n    Source = Odbc.DataSource("driver={Simba ODBC Driver};dsn=bq_events", '
    "[HierarchicalNavigation=true]),\n"
    '    events_Table = Source{[Name="events",Kind="Table"]}[Data]\n'
    "in\n    events_Table"
)

# MySQL (database.table fallback) ODBC navigation that surfaces only the leaf
# table. A two-segment dsn_to_database_schema value must not splice a spurious
# schema tier: only the database level is backfilled, keeping the URN two-part.
_ODBC_MYSQL_NAV_TABLE_ONLY = (
    'let\n    Source = Odbc.DataSource("driver={MySQL ODBC 9.2 Unicode Driver};'
    'server=10.1.10.1;dsn=testdb01", [HierarchicalNavigation=true]),\n'
    '    employees_Table = Source{[Name="employees",Kind="Table"]}[Data]\n'
    "in\n    employees_Table"
)

# BigQuery (three-tier) ODBC navigation that surfaces the Kind=Database node
# (project) and the leaf table but omits the Schema (dataset) level. The dataset
# must be backfilled from dsn_to_database_schema even though the database tier is
# already populated, otherwise no project.dataset.table URN can be built.
_ODBC_BIGQUERY_NAV_DATABASE_NO_SCHEMA = (
    'let\n    Source = Odbc.DataSource("driver={Simba ODBC Driver};dsn=bq_analytics", '
    "[HierarchicalNavigation=true]),\n"
    '    my_project_Database = Source{[Name="my_project",Kind="Database"]}[Data],\n'
    '    events_Table = my_project_Database{[Name="events",Kind="Table"]}[Data]\n'
    "in\n    events_Table"
)

# Hive (two-tier) ODBC navigation that surfaces only the leaf table; the schema
# level must be backfilled from dsn_to_database_schema.
_ODBC_HIVE_NAV_MISSING_SCHEMA = (
    'let\n    Source = Odbc.DataSource("driver={Cloudera ODBC Driver for Apache Hive};'
    'server=hive.example.com;dsn=hive_prod", [HierarchicalNavigation=true]),\n'
    '    user_profile_Table = Source{[Name="user_profile",Kind="Table"]}[Data]\n'
    "in\n    user_profile_Table"
)

# Real-world Hive (two-tier) ODBC navigation: it surfaces the Kind=Database
# pseudo-catalog ("HIVE") but no Schema level. The schema must still be backfilled
# from dsn_to_database_schema even though the database tier is already populated.
_ODBC_HIVE_NAV_DATABASE_NO_SCHEMA = (
    'let\n    Source = Odbc.DataSource("driver={Cloudera ODBC Driver for Apache Hive};'
    'server=hive.example.com;dsn=hive_prod", [HierarchicalNavigation=true]),\n'
    '    HIVE_Database = Source{[Name="HIVE",Kind="Database"]}[Data],\n'
    '    user_profile_Table = HIVE_Database{[Name="user_profile",Kind="Table"]}[Data]\n'
    "in\n    user_profile_Table"
)

# Oracle (two-tier) ODBC navigation that surfaces Schema + Table. Oracle URNs are
# schema.table (OracleSource defaults add_database_name_to_urn=False), so no
# database tier is prepended.
_ODBC_ORACLE_NAV_SCHEMA_TABLE = (
    'let\n    Source = Odbc.DataSource("driver={Oracle in OraClient19Home1};'
    'dsn=oracle_prod", [HierarchicalNavigation=true]),\n'
    '    SALES_Schema = Source{[Name="SALES",Kind="Schema"]}[Data],\n'
    '    ORDERS_Table = SALES_Schema{[Name="ORDERS",Kind="Table"]}[Data]\n'
    "in\n    ORDERS_Table"
)

# Oracle (two-tier) ODBC navigation that surfaces a Kind=Database pseudo-catalog
# and the leaf table but omits Schema. With no dsn_to_database_schema mapping the
# schema cannot be resolved, so lineage must warn and skip rather than emit a
# truncated database.table URN.
_ODBC_ORACLE_NAV_DATABASE_NO_SCHEMA = (
    'let\n    Source = Odbc.DataSource("driver={Oracle in OraClient19Home1};'
    'dsn=oracle_prod", [HierarchicalNavigation=true]),\n'
    '    ORCLPDB_Database = Source{[Name="ORCLPDB",Kind="Database"]}[Data],\n'
    '    ORDERS_Table = ORCLPDB_Database{[Name="ORDERS",Kind="Table"]}[Data]\n'
    "in\n    ORDERS_Table"
)

# Oracle ODBC navigation that surfaces Database + Schema + Table. When the user
# opts into 3-part URNs via server_to_platform_instance default_database, the
# navigation-supplied database (ORCLPDB) must win over the configured
# default_database (MYDB) — matching OracleLineage's effective_db precedence and
# the "navigation always wins" contract.
_ODBC_ORACLE_NAV_DATABASE_SCHEMA_TABLE = (
    'let\n    Source = Odbc.DataSource("driver={Oracle in OraClient19Home1};'
    'dsn=oracle_prod", [HierarchicalNavigation=true]),\n'
    '    ORCLPDB_Database = Source{[Name="ORCLPDB",Kind="Database"]}[Data],\n'
    '    SALES_Schema = ORCLPDB_Database{[Name="SALES",Kind="Schema"]}[Data],\n'
    '    ORDERS_Table = SALES_Schema{[Name="ORDERS",Kind="Table"]}[Data]\n'
    "in\n    ORDERS_Table"
)

# Athena ODBC navigation that surfaces only the leaf table; the database
# (which occupies Athena's schema slot) is backfilled from a one-part
# dsn_to_database_schema value. extract_server resolves the DSN as the server
# name, so a server_to_platform_instance mapping keyed on the DSN applies — the
# resulting two-part database.table URN must keep its platform_instance prefix
# (the pre-fix bug stripped the instance because make_urn embeds it as the
# leading name segment).
_ODBC_ATHENA_NAV_TABLE_ONLY = (
    'let\n    Source = Odbc.DataSource("driver={Simba Amazon Athena ODBC Driver};'
    'dsn=athena_prod", [HierarchicalNavigation=true]),\n'
    '    accounts_Table = Source{[Name="accounts",Kind="Table"]}[Data]\n'
    "in\n    accounts_Table"
)

# Athena ODBC navigation that surfaces the Kind=Database catalog, the Schema
# (Athena's real database) and the leaf table. The catalog is stripped to match
# the standalone Athena connector; with a server_to_platform_instance mapping the
# strip must drop the catalog, not the embedded platform_instance prefix.
_ODBC_ATHENA_NAV_CATALOG_SCHEMA_TABLE = (
    'let\n    Source = Odbc.DataSource("driver={Simba Amazon Athena ODBC Driver};'
    'dsn=athena_prod", [HierarchicalNavigation=true]),\n'
    '    awsdatacatalog_Database = Source{[Name="awsdatacatalog",Kind="Database"]}[Data],\n'
    '    mydb_Schema = awsdatacatalog_Database{[Name="mydb",Kind="Schema"]}[Data],\n'
    '    accounts_Table = mydb_Schema{[Name="accounts",Kind="Table"]}[Data]\n'
    "in\n    accounts_Table"
)

# Athena reached through Odbc.Query rather than navigation. sqlglot lowercases the
# whole URN, platform_instance included, so a mixed-case configured instance only
# survives the catalog strip because the prefix match ignores case.
_ODBC_ATHENA_QUERY_CATALOG_TABLE = (
    'let\n    Source = Odbc.Query("driver={Simba Amazon Athena ODBC Driver};'
    'dsn=athena_prod", "SELECT * FROM awsdatacatalog.mydb.orders")\n'
    "in\n    Source"
)


# Db2 (three-tier: the standalone Db2Source emits database.schema.object) ODBC
# navigation that surfaces a Database node and the leaf table but omits Schema.
# Before Db2 was classified three-tier this emitted a truncated
# database.table URN; it must now backfill the schema from dsn_to_database_schema,
# or warn and skip when no mapping supplies it.
_ODBC_DB2_NAV_DATABASE_NO_SCHEMA = (
    'let\n    Source = Odbc.DataSource("driver={IBM DB2 ODBC DRIVER};'
    'dsn=db2_prod", [HierarchicalNavigation=true]),\n'
    '    SAMPLE_Database = Source{[Name="SAMPLE",Kind="Database"]}[Data],\n'
    '    ORDERS_Table = SAMPLE_Database{[Name="ORDERS",Kind="Table"]}[Data]\n'
    "in\n    ORDERS_Table"
)


# Each case exercises a distinct ODBC navigation / dsn_to_database_schema backfill
# branch: (expression, table name, full_name, override_config, expected_urn).
_ODBC_NAV_SUCCESS_CASES = [
    pytest.param(
        _ODBC_BIGQUERY_NAV_MISSING_PROJECT,
        "events",
        "analytics.events",
        {
            "dsn_to_platform_name": {"bq_analytics": "bigquery"},
            "dsn_to_database_schema": {"bq_analytics": "my_project"},
        },
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.analytics_ds.events,PROD)",
        id="bigquery-project-from-dsn-mapping",
    ),
    pytest.param(
        _ODBC_BIGQUERY_NAV_MISSING_PROJECT,
        "events",
        "analytics.events",
        {
            # Windows ODBC DSNs are case-insensitive: a key that differs from the
            # connection string's dsn=bq_analytics only in case still applies.
            "dsn_to_platform_name": {"BQ_Analytics": "bigquery"},
            "dsn_to_database_schema": {"bq_analytics": "my_project"},
        },
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.analytics_ds.events,PROD)",
        id="bigquery-dsn-to-platform-name-matches-case-insensitively",
    ),
    pytest.param(
        _ODBC_BIGQUERY_NAV_TABLE_ONLY,
        "events",
        "analytics.events",
        {
            "dsn_to_platform_name": {"bq_events": "bigquery"},
            "dsn_to_database_schema": {"bq_events": "my_project.analytics_ds"},
        },
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.analytics_ds.events,PROD)",
        id="bigquery-project-and-dataset-from-dsn-mapping",
    ),
    pytest.param(
        _ODBC_BIGQUERY_NAV_MISSING_PROJECT,
        "events",
        "analytics.events",
        {
            "dsn_to_platform_name": {"bq_analytics": "bigquery"},
            "dsn_to_database_schema": {"bq_analytics": "my_project.wrong_dataset"},
        },
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.analytics_ds.events,PROD)",
        id="bigquery-navigation-wins-over-dsn-mapping",
    ),
    pytest.param(
        _ODBC_BIGQUERY_NAV_DATABASE_NO_SCHEMA,
        "events",
        "analytics.events",
        {
            "dsn_to_platform_name": {"bq_analytics": "bigquery"},
            "dsn_to_database_schema": {"bq_analytics": "wrong_project.analytics_ds"},
        },
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.analytics_ds.events,PROD)",
        id="bigquery-database-node-backfills-schema",
    ),
    pytest.param(
        _ODBC_HIVE_NAV_MISSING_SCHEMA,
        "user_profile",
        "product_analytics.user_profile",
        {"dsn_to_database_schema": {"hive_prod": "product_analytics"}},
        "urn:li:dataset:(urn:li:dataPlatform:hive,product_analytics.user_profile,PROD)",
        id="hive-single-segment-dsn-backfills-schema",
    ),
    pytest.param(
        _ODBC_HIVE_NAV_MISSING_SCHEMA,
        "user_profile",
        "product_analytics.user_profile",
        {"dsn_to_database_schema": {"hive_prod": "hive_catalog.product_analytics"}},
        "urn:li:dataset:(urn:li:dataPlatform:hive,product_analytics.user_profile,PROD)",
        id="hive-two-part-dsn-backfills-schema",
    ),
    pytest.param(
        _ODBC_HIVE_NAV_DATABASE_NO_SCHEMA,
        "user_profile",
        "product_analytics.user_profile",
        {"dsn_to_database_schema": {"hive_prod": "hive_catalog.product_analytics"}},
        "urn:li:dataset:(urn:li:dataPlatform:hive,product_analytics.user_profile,PROD)",
        id="hive-database-node-still-backfills-schema",
    ),
    pytest.param(
        _ODBC_ORACLE_NAV_SCHEMA_TABLE,
        "ORDERS",
        "SALES.ORDERS",
        {},
        "urn:li:dataset:(urn:li:dataPlatform:oracle,sales.orders,PROD)",
        id="oracle-schema-table-two-tier",
    ),
    # End-to-end opt-in: driven from a real server_to_platform_instance entry (not
    # an injected resolver), keyed on the server that extract_server derives from
    # the ODBC connect string — the DSN "oracle_prod" for this DSN-only string, not
    # a TNS alias. Pins that the key resolves on this path so default_database
    # actually prepends the database tier (a wrong key would silently stay 2-part).
    pytest.param(
        _ODBC_ORACLE_NAV_SCHEMA_TABLE,
        "ORDERS",
        "SALES.ORDERS",
        {"server_to_platform_instance": {"oracle_prod": {"default_database": "MYDB"}}},
        "urn:li:dataset:(urn:li:dataPlatform:oracle,mydb.sales.orders,PROD)",
        id="oracle-default-database-opt-in-resolves-from-config",
    ),
    # End-to-end precedence: with a nav Database node present, the opt-in
    # default_database is only a fallback — navigation (ORCLPDB) wins over the
    # configured MYDB, mirroring OracleLineage's effective_db. Pins that the
    # documented "navigation always wins" contract holds on the real-config path,
    # not just the injected-resolver unit test.
    pytest.param(
        _ODBC_ORACLE_NAV_DATABASE_SCHEMA_TABLE,
        "ORDERS",
        "SALES.ORDERS",
        {"server_to_platform_instance": {"oracle_prod": {"default_database": "MYDB"}}},
        "urn:li:dataset:(urn:li:dataPlatform:oracle,orclpdb.sales.orders,PROD)",
        id="oracle-nav-database-wins-over-default-database",
    ),
    pytest.param(
        _ODBC_MYSQL_NAV_DATABASE_TABLE,
        "employees",
        "employees.employees",
        {"dsn_to_database_schema": {"testdb01": "warehouse.sales"}},
        "urn:li:dataset:(urn:li:dataPlatform:mysql,employees.employees,PROD)",
        id="mysql-database-table-two-part-dsn-no-schema-tier",
    ),
    pytest.param(
        _ODBC_MYSQL_NAV_TABLE_ONLY,
        "employees",
        "employees.employees",
        {"dsn_to_database_schema": {"testdb01": "warehouse.sales"}},
        "urn:li:dataset:(urn:li:dataPlatform:mysql,warehouse.employees,PROD)",
        id="mysql-table-only-two-part-stays-two-part",
    ),
    pytest.param(
        _ODBC_ATHENA_NAV_TABLE_ONLY,
        "accounts",
        "mydb.accounts",
        {
            "dsn_to_database_schema": {"athena_prod": "mydb"},
            "server_to_platform_instance": {
                "athena_prod": {
                    "platform_instance": "my_instance",
                    "env": "PROD",
                }
            },
        },
        "urn:li:dataset:(urn:li:dataPlatform:athena,my_instance.mydb.accounts,PROD)",
        id="athena-table-only-preserves-platform-instance",
    ),
    pytest.param(
        _ODBC_ATHENA_NAV_CATALOG_SCHEMA_TABLE,
        "accounts",
        "mydb.accounts",
        {
            "server_to_platform_instance": {
                "athena_prod": {
                    "platform_instance": "my_instance",
                    "env": "PROD",
                }
            },
        },
        "urn:li:dataset:(urn:li:dataPlatform:athena,my_instance.mydb.accounts,PROD)",
        id="athena-catalog-strip-preserves-platform-instance",
    ),
    # dataset_type_mapping resolves on the PowerBI platform name, so the instance
    # only applies if ODBC resolves with the runtime pair (Athena), not ODBC's own.
    pytest.param(
        _ODBC_ATHENA_NAV_CATALOG_SCHEMA_TABLE,
        "accounts",
        "mydb.accounts",
        {
            "dataset_type_mapping": {
                "Amazon Athena": {"platform_instance": "my_instance"}
            }
        },
        "urn:li:dataset:(urn:li:dataPlatform:athena,my_instance.mydb.accounts,PROD)",
        id="athena-platform-instance-from-dataset-type-mapping",
    ),
    pytest.param(
        _ODBC_ATHENA_QUERY_CATALOG_TABLE,
        "orders",
        "mydb.orders",
        {
            "server_to_platform_instance": {
                "athena_prod": {"platform_instance": "My_Inst", "env": "PROD"}
            }
        },
        "urn:li:dataset:(urn:li:dataPlatform:athena,my_inst.mydb.orders,PROD)",
        id="athena-odbc-query-mixed-case-instance-strips-catalog",
    ),
    pytest.param(
        _ODBC_DB2_NAV_DATABASE_NO_SCHEMA,
        "ORDERS",
        "SAMPLE.ORDERS",
        {"dsn_to_database_schema": {"db2_prod": "ignored_db.myschema"}},
        "urn:li:dataset:(urn:li:dataPlatform:db2,sample.myschema.orders,PROD)",
        id="db2-database-node-backfills-schema",
    ),
]

# (expression, table name, full_name, override_config, expected_warning_title) for
# navigation that cannot build a fully-qualified name and must warn and skip.
_ODBC_NAV_WARN_CASES = [
    pytest.param(
        _ODBC_BIGQUERY_NAV_MISSING_PROJECT,
        "events",
        "analytics.events",
        {"dsn_to_platform_name": {"bq_analytics": "bigquery"}},
        "Can not determine qualified table name",
        id="bigquery-missing-project",
    ),
    pytest.param(
        _ODBC_BIGQUERY_NAV_TABLE_ONLY,
        "events",
        "analytics.events",
        {
            "dsn_to_platform_name": {"bq_events": "bigquery"},
            "dsn_to_database_schema": {"bq_events": "my_project"},
        },
        "Can not determine qualified table name",
        id="bigquery-table-only-single-segment-mapping",
    ),
    pytest.param(
        _ODBC_ORACLE_NAV_DATABASE_NO_SCHEMA,
        "ORDERS",
        "SALES.ORDERS",
        {},
        "Cannot build two-tier ODBC table name",
        id="oracle-missing-schema",
    ),
    pytest.param(
        _ODBC_DB2_NAV_DATABASE_NO_SCHEMA,
        "ORDERS",
        "SAMPLE.ORDERS",
        {},
        "Can not determine qualified table name",
        id="db2-missing-schema-warns-instead-of-truncated-urn",
    ),
    pytest.param(
        _ODBC_MYSQL_NAV_TABLE_ONLY,
        "employees",
        "employees.employees",
        {},
        "Can not determine qualified table name",
        id="mysql-table-only-without-mapping-warns-instead-of-none-database",
    ),
]


@pytest.mark.parametrize(
    "expression, name, full_name, override_config, expected_urn",
    _ODBC_NAV_SUCCESS_CASES,
)
def test_odbc_navigation_builds_expected_urn(
    expression, name, full_name, override_config, expected_urn
):
    """ODBC hierarchical navigation (plus optional dsn_to_database_schema backfill)
    builds the expected upstream URN. Each param id names the platform-shape /
    backfill branch being exercised."""
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=expression,
        name=name,
        full_name=full_name,
    )

    reporter = PowerBiDashboardSourceReport()
    ctx, config, platform_instance_resolver = get_default_instances(override_config)

    data_platform_tables: List[DataPlatformTable] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )[0].upstreams

    assert len(data_platform_tables) == 1
    assert data_platform_tables[0].urn == expected_urn


@pytest.mark.parametrize(
    "expression, name, full_name, override_config, expected_warning_title",
    _ODBC_NAV_WARN_CASES,
)
def test_odbc_navigation_warns_and_skips(
    expression, name, full_name, override_config, expected_warning_title
):
    """ODBC hierarchical navigation that cannot build a fully-qualified name for a
    platform whose standalone connector requires it must warn and skip rather than
    emit a truncated URN. Each param id names the scenario."""
    table: powerbi_data_classes.Table = powerbi_data_classes.Table(
        columns=[],
        measures=[],
        expression=expression,
        name=name,
        full_name=full_name,
    )

    reporter = PowerBiDashboardSourceReport()
    ctx, config, platform_instance_resolver = get_default_instances(override_config)

    lineages: List[Lineage] = parser.get_upstream_tables(
        table,
        reporter,
        ctx=ctx,
        config=config,
        platform_instance_resolver=platform_instance_resolver,
    )

    assert combine_upstreams_from_lineage(lineages) == []
    warning_titles = [entry.title for entry in reporter.warnings]
    assert expected_warning_title in warning_titles, (
        f"Expected warning '{expected_warning_title}'; got: {warning_titles}"
    )
