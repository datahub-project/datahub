import datetime
import logging
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    cast,
)
from unittest import mock

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import NoSuchTableError, ProgrammingError
from sqlalchemy.pool import NullPool, QueuePool, StaticPool
from sqlalchemy.sql import sqltypes as types
from sqlalchemy.types import TypeEngine

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source_protocols import MetadataWorkUnitIterable
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetSubTypes,
    FlowContainerSubTypes,
)
from datahub.ingestion.source.sql.sql_common import (
    PipelineContext,
    SQLAlchemySource,
    SqlWorkUnit,
    _pool_accepts_sizing_options,
    get_column_type,
    get_schema_metadata,
)
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)
from datahub.ingestion.source.sql.stored_procedures.models import (
    BaseProcedure,
    get_procedure_flow_name,
)
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DomainsClass,
    ForeignKeyConstraintClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from tests.unit.conftest import (
    aspects_by_urn,
    aspects_of_type,
    log_contexts,
    log_messages,
    log_titles,
    single_aspect,
)
from tests.unit.sql_test_helpers import make_procedure


class _TestSQLAlchemyConfig(SQLCommonConfig):
    def get_sql_alchemy_url(self):
        return "mysql+pymysql://user:pass@localhost:5330"


class _TestSQLAlchemySource(SQLAlchemySource):
    @classmethod
    def create(cls, config_dict, ctx):
        config = _TestSQLAlchemyConfig.model_validate(config_dict)
        return cls(config, ctx, "TEST")


def get_test_sql_alchemy_source():
    return _TestSQLAlchemySource.create(
        config_dict={}, ctx=PipelineContext(run_id="test_ctx")
    )


class _TestTwoTierSQLAlchemyConfig(TwoTierSQLAlchemyConfig):
    # Test stub returning a fixed URL. The MRO has two different signatures
    # for `get_sql_alchemy_url` (TwoTierSQLAlchemyConfig uses `current_db`,
    # SQLAlchemyConnectionConfig uses `database`), so we silence the override
    # check rather than pick one and break the other.
    def get_sql_alchemy_url(self, *args: object, **kwargs: object) -> str:  # type: ignore[override]
        return "mysql+pymysql://user:pass@localhost:5330"


class _TestTwoTierSQLAlchemySource(TwoTierSQLAlchemySource):
    @classmethod
    def create(cls, config_dict, ctx):
        config = _TestTwoTierSQLAlchemyConfig.model_validate(config_dict)
        return cls(config, ctx, "TEST")


def test_three_tier_source_emits_schema_key_for_procedures():
    source = get_test_sql_alchemy_source()
    schema_key = source._get_procedure_schema_key("test_db", "test_schema")
    assert schema_key is not None
    assert schema_key.database == "test_db"
    assert schema_key.db_schema == "test_schema"


_TWO_TIER_CONFIG_DICT = {
    "host_port": "localhost:5330",
    "scheme": "mysql+pymysql",
}


def test_two_tier_source_omits_schema_key_for_procedures():
    """Two-tier sources (MySQL, MariaDB, Hive, …) pass `db_name == schema` to
    `_process_procedures`. Building a SchemaKey from that produces flow names
    like ``test_db.test_db.stored_procedures``; the override must instead
    yield None so the flow falls back to ``test_db.stored_procedures``."""
    source = _TestTwoTierSQLAlchemySource.create(
        config_dict=_TWO_TIER_CONFIG_DICT, ctx=PipelineContext(run_id="test_ctx")
    )
    assert source._get_procedure_schema_key("test_db", "test_db") is None


def test_procedure_flow_name_two_tier_does_not_duplicate_database():
    """End-to-end protection for the two-tier flow-name bug: when the source
    omits the SchemaKey, `get_procedure_flow_name` must produce
    ``{database}.stored_procedures`` (single ``test_db``), not
    ``{database}.{database}.stored_procedures``."""
    source = _TestTwoTierSQLAlchemySource.create(
        config_dict=_TWO_TIER_CONFIG_DICT, ctx=PipelineContext(run_id="test_ctx")
    )
    database_key = source._get_procedure_database_key("test_db")
    schema_key = source._get_procedure_schema_key("test_db", "test_db")

    flow_name = get_procedure_flow_name(database_key, schema_key)

    assert flow_name == "test_db.stored_procedures"


def test_generate_foreign_key():
    source = get_test_sql_alchemy_source()
    fk_dict: Dict[str, str] = {
        "name": "test_constraint",
        "referred_table": "test_table",
        "referred_schema": "test_referred_schema",
        "constrained_columns": ["test_column"],  # type: ignore
        "referred_columns": ["test_referred_column"],  # type: ignore
    }
    foreign_key = source.get_foreign_key_metadata(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:TEST,test_schema.base_urn,PROD)",
        schema="test_schema",
        fk_dict=fk_dict,
        inspector=mock.Mock(),
    )

    assert fk_dict.get("name") == foreign_key.name
    assert foreign_key.foreignFields == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:TEST,test_referred_schema.test_table,PROD),test_referred_column)"
    ]
    assert foreign_key.sourceFields == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:TEST,test_schema.base_urn,PROD),test_column)"
    ]


def test_use_source_schema_for_foreign_key_if_not_specified():
    source = get_test_sql_alchemy_source()
    fk_dict: Dict[str, str] = {
        "name": "test_constraint",
        "referred_table": "test_table",
        "constrained_columns": ["test_column"],  # type: ignore
        "referred_columns": ["test_referred_column"],  # type: ignore
    }
    foreign_key = source.get_foreign_key_metadata(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:TEST,test_schema.base_urn,PROD)",
        schema="test_schema",
        fk_dict=fk_dict,
        inspector=mock.Mock(),
    )

    assert fk_dict.get("name") == foreign_key.name
    assert foreign_key.foreignFields == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:TEST,test_schema.test_table,PROD),test_referred_column)"
    ]
    assert foreign_key.sourceFields == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:TEST,test_schema.base_urn,PROD),test_column)"
    ]


PLATFORM_FROM_SQLALCHEMY_URI_TEST_CASES: Dict[str, str] = {
    "awsathena://test_athena:3316/athenadb": "athena",
    "bigquery://test_bq:3316/bigquery": "bigquery",
    "clickhouse://test_clickhouse:3316/clickhousedb": "clickhouse",
    "druid://test_druid:1101/druiddb": "druid",
    "hive://test_hive:1201/hive": "hive",
    "mongodb://test_mongodb:1201/mongo": "mongodb",
    "mssql://test_mssql:1201/mssqldb": "mssql",
    "mysql://test_mysql:1201/mysql": "mysql",
    "oracle://test_oracle:3306/oracledb": "oracle",
    "pinot://test_pinot:3306/pinot": "pinot",
    "postgresql://test_postgres:5432/postgres": "postgres",
    "presto://test_presto:5432/prestodb": "presto",
    "redshift://test_redshift:5432/redshift": "redshift",
    "jdbc:postgres://test_redshift:5432/redshift.amazonaws": "redshift",
    "postgresql://test_redshift:5432/redshift.amazonaws": "redshift",
    "snowflake://test_snowflake:5432/snowflakedb": "snowflake",
    "trino://test_trino:5432/trino": "trino",
    "unknowndb://host:1/db": "external",
}


@pytest.mark.parametrize(
    "uri, expected_platform",
    PLATFORM_FROM_SQLALCHEMY_URI_TEST_CASES.items(),
    ids=PLATFORM_FROM_SQLALCHEMY_URI_TEST_CASES.keys(),
)
def test_get_platform_from_sqlalchemy_uri(uri: str, expected_platform: str) -> None:
    platform: str = get_platform_from_sqlalchemy_uri(uri)
    assert platform == expected_platform


def test_get_db_schema_with_dots_in_view_name():
    source = get_test_sql_alchemy_source()
    database, schema = source.get_db_schema(
        dataset_identifier="database.schema.long.view.name1"
    )
    assert database == "database"
    assert schema == "schema"


def test_test_connection_success():
    source = get_test_sql_alchemy_source()
    with mock.patch(
        "datahub.ingestion.source.sql.sql_common.SQLAlchemySource.get_inspectors",
        side_effect=lambda: [],
    ):
        report = source.test_connection({})
        assert report is not None
        assert report.basic_connectivity
        assert report.basic_connectivity.capable
        assert report.basic_connectivity.failure_reason is None


def test_test_connection_failure():
    source = get_test_sql_alchemy_source()
    report = source.test_connection({})
    assert report is not None
    assert report.basic_connectivity
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason
    assert "Connection refused" in report.basic_connectivity.failure_reason


@pytest.mark.parametrize(
    "upstream_field_path,downstream_field_path,expected_simplified_upstream,expected_simplified_downstream",
    [
        (
            "[version=2.0].[type=string].employee_id",
            "[version=2.0].[type=string].employee_id",
            "employee_id",
            "employee_id",
        ),
        (
            "[version=2.0].[type=struct].[type=map].[type=struct].job_history",
            "[version=2.0].[type=struct].[type=map].[type=struct].job_history",
            "job_history",
            "job_history",
        ),
        (
            "[version=2.0].[type=struct].[type=array].[type=string].skills",
            "[version=2.0].[type=struct].[type=array].[type=string].skills",
            "skills",
            "skills",
        ),
        (" spaced . field ", "spaced.field", "spaced.field", "spaced.field"),
    ],
)
def test_fine_grained_lineages(
    upstream_field_path,
    downstream_field_path,
    expected_simplified_upstream,
    expected_simplified_downstream,
):
    source = get_test_sql_alchemy_source()

    downstream_field = SchemaFieldClass(
        fieldPath=downstream_field_path,
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="string",
    )

    lineages = source.get_fine_grained_lineages(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:TEST,test_db.test_schema.downstream_table,PROD)",
        upstream_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:TEST,test_db.test_schema.upstream_table,PROD)",
        schema_fields=[downstream_field],
    )

    assert lineages is not None
    assert len(lineages) == 1

    def get_field_from_urn(urn):
        return urn.split(",")[-1].rstrip(")")

    actual_downstream = get_field_from_urn(lineages[0].downstreams[0])
    actual_upstream = get_field_from_urn(lineages[0].upstreams[0])

    assert actual_downstream == expected_simplified_downstream
    assert actual_upstream == expected_simplified_upstream


def test_loop_profiler_requests_propagates_candidate_generation_errors():
    # Regression guard: a non-NotImplementedError raised by generate_profile_candidates
    # must propagate out of loop_profiler_requests, not be caught. A blanket
    # except Exception around that call once silently made Oracle's guardrail fail
    # open (ORA-00942 on DBA_TABLES became "profile every table"); this test pins
    # that only NotImplementedError is swallowed.
    source = _TestSQLAlchemySource.create(
        config_dict={"profiling": {"profile_table_row_limit": 1_000_000}},
        ctx=PipelineContext(run_id="test_ctx"),
    )

    def boom(*args, **kwargs):
        raise RuntimeError("candidate query failed")

    source.generate_profile_candidates = boom  # type: ignore[method-assign]
    inspector = mock.MagicMock()

    with pytest.raises(RuntimeError, match="candidate query failed"):
        list(source.loop_profiler_requests(inspector, "my_schema", source.config))


def test_loop_views_keeps_view_when_definition_is_unavailable():
    # SA 2.0 dialects (Oracle, Postgres) raise NoSuchTableError when the catalog
    # has no view text; 1.4 returned None. The view must still be emitted, with
    # an empty definition, instead of being dropped as "Error processing view".
    engine = create_engine("sqlite://")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (a INTEGER)"))
        conn.execute(text("CREATE VIEW v AS SELECT a FROM t"))
        inspector = inspect(conn)
        source = get_test_sql_alchemy_source()
        with mock.patch.object(
            inspector, "get_view_definition", side_effect=NoSuchTableError("v")
        ):
            workunits = list(source.loop_views(inspector, "main", source.config))

    view_properties = [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, ViewPropertiesClass)
    ]
    assert len(view_properties) == 1
    assert view_properties[0].viewLogic == ""
    assert not source.report.warnings
    assert not source.report.failures


class _TestSQLiteConfig(SQLCommonConfig):
    # In-memory SQLite uses SingletonThreadPool, so it exercises the
    # non-pooling path. (File-backed SQLite uses QueuePool on SQLAlchemy 2.0.)
    def get_sql_alchemy_url(self):
        return "sqlite://"


@pytest.mark.parametrize(
    "url, options, expected",
    [
        ("sqlite:///test.db", {}, True),
        ("sqlite://", {}, False),
        ("sqlite:///:memory:", {}, False),
        # An explicit poolclass wins over the dialect default, both ways.
        ("sqlite://", {"poolclass": QueuePool}, True),
        ("sqlite:///test.db", {"poolclass": NullPool}, False),
        ("sqlite:///test.db", {"poolclass": StaticPool}, False),
        # Driver not installed -> keep the previous behaviour.
        ("not-a-real-dialect://host/db", {}, True),
    ],
)
def test_pool_accepts_sizing_options(url: str, options: dict, expected: bool) -> None:
    assert _pool_accepts_sizing_options(url, options) is expected


def test_add_default_options_skips_max_overflow_for_non_pooling_dialect() -> None:
    source = get_test_sql_alchemy_source()
    config = _TestSQLiteConfig.model_validate({"profiling": {"enabled": True}})

    source._add_default_options(config)

    # Passing max_overflow to a non-QueuePool engine makes create_engine() raise
    # TypeError, which previously aborted the run before profiling anything.
    assert "max_overflow" not in config.options
    create_engine(config.get_sql_alchemy_url(), **config.options).dispose()


def test_add_default_options_skips_max_overflow_for_explicit_non_queue_pool() -> None:
    source = get_test_sql_alchemy_source()
    config = _TestSQLAlchemyConfig.model_validate(
        {"profiling": {"enabled": True}, "options": {"poolclass": NullPool}}
    )

    source._add_default_options(config)

    assert "max_overflow" not in config.options


def test_add_default_options_sets_max_overflow_when_pooled() -> None:
    source = get_test_sql_alchemy_source()
    config = _TestSQLAlchemyConfig.model_validate({"profiling": {"enabled": True}})

    source._add_default_options(config)

    assert config.options["max_overflow"] == config.profiling.max_workers


def test_add_default_options_noop_without_profiling() -> None:
    source = get_test_sql_alchemy_source()
    config = _TestSQLAlchemyConfig.model_validate({})

    source._add_default_options(config)

    assert "max_overflow" not in config.options


# ---------------------------------------------------------------------------
# End-to-end over a real (file-backed) SQLite database.
#
# A three-tier style source, like Postgres, whose identifiers are
# ``db.schema.table``: the relative ``sqlite:///testdb`` URL makes the engine's
# ``url.database`` a plain name, so get_db_name needs no override.
# ---------------------------------------------------------------------------

_SQLITE_SCHEMA_SQL = """
CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER REFERENCES parent(id),
    note TEXT
);
INSERT INTO parent VALUES (1, 'a'), (2, 'b');
INSERT INTO child VALUES (1, 1, 'x'), (2, 2, 'y'), (3, 1, NULL);
CREATE VIEW child_view AS SELECT id, note FROM child;
"""


class _SqliteConfig(SQLCommonConfig):
    connect_uri: str
    include_stored_procedures: bool = False
    procedure_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    def get_sql_alchemy_url(self) -> str:
        return self.connect_uri


class _SqliteSource(SQLAlchemySource):
    def __init__(self, config: _SqliteConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx, "sqlite")

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "_SqliteSource":
        return cls(_SqliteConfig.model_validate(config_dict), ctx)

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        return f"{self.get_db_name(inspector)}.{schema}.{entity}"


@pytest.fixture
def sqlite_db(sqlite_file_db: Callable[..., str]) -> str:
    return sqlite_file_db("testdb", _SQLITE_SCHEMA_SQL)


def _sqlite_source(connect_uri: str, **overrides: Any) -> _SqliteSource:
    return _SqliteSource.create(
        {"connect_uri": connect_uri, **overrides}, PipelineContext(run_id="test")
    )


CHILD_URN = "urn:li:dataset:(urn:li:dataPlatform:sqlite,testdb.main.child,PROD)"
PARENT_URN = "urn:li:dataset:(urn:li:dataPlatform:sqlite,testdb.main.parent,PROD)"
VIEW_URN = "urn:li:dataset:(urn:li:dataPlatform:sqlite,testdb.main.child_view,PROD)"


def test_workunits_describe_tables_views_containers_and_view_lineage(
    sqlite_db: str,
) -> None:
    """The whole SQLAlchemySource walk over a real database: containers,
    schemas (keys, nullability, types, FKs), subtypes, view logic and the view
    lineage the single aggregator emits at the end."""
    source = _sqlite_source(sqlite_db)

    aspects = aspects_by_urn(source.get_workunits_internal())

    containers = {
        single_aspect(a, ContainerPropertiesClass).name: urn
        for urn, a in aspects.items()
        if urn.startswith("urn:li:container:")
    }
    assert set(containers) == {"testdb", "main"}
    # The schema container hangs off the database container, and every
    # dataset off the schema container.
    assert (
        single_aspect(aspects[containers["main"]], ContainerClass).container
        == containers["testdb"]
    )
    for urn in (CHILD_URN, PARENT_URN, VIEW_URN):
        assert (
            single_aspect(aspects[urn], ContainerClass).container
            == (containers["main"])
        )

    child_schema = single_aspect(aspects[CHILD_URN], SchemaMetadataClass)
    assert [f.fieldPath for f in child_schema.fields] == ["id", "parent_id", "note"]
    assert child_schema.fields[0].isPartOfKey
    assert not child_schema.fields[1].isPartOfKey
    assert child_schema.fields[2].nullable
    assert isinstance(child_schema.fields[0].type.type, NumberTypeClass)
    assert isinstance(child_schema.fields[2].type.type, StringTypeClass)
    assert child_schema.foreignKeys is not None
    assert child_schema.foreignKeys[0].foreignDataset == PARENT_URN
    assert child_schema.foreignKeys[0].sourceFields == [
        f"urn:li:schemaField:({CHILD_URN},parent_id)"
    ]
    assert single_aspect(aspects[CHILD_URN], SubTypesClass).typeNames == [
        DatasetSubTypes.TABLE
    ]
    assert single_aspect(aspects[CHILD_URN], DatasetPropertiesClass).name == "child"

    assert single_aspect(aspects[VIEW_URN], SubTypesClass).typeNames == [
        DatasetSubTypes.VIEW
    ]
    view_properties = single_aspect(aspects[VIEW_URN], ViewPropertiesClass)
    assert "SELECT id, note FROM child" in view_properties.viewLogic
    dataset_properties = single_aspect(aspects[VIEW_URN], DatasetPropertiesClass)
    assert dataset_properties.customProperties["is_view"] == "True"
    lineage = single_aspect(aspects[VIEW_URN], UpstreamLineageClass)
    assert [u.dataset for u in lineage.upstreams] == [CHILD_URN]

    assert source.report.tables_scanned == 2
    assert source.report.views_scanned == 1
    assert not source.report.warnings
    assert not source.report.failures


def test_patterns_and_include_flags_decide_what_is_emitted(sqlite_db: str) -> None:
    source = _sqlite_source(
        sqlite_db,
        table_pattern={"deny": [".*parent"]},
        view_pattern={"allow": [".*view"]},
    )
    aspects = aspects_by_urn(source.get_workunits_internal())
    assert CHILD_URN in aspects
    assert VIEW_URN in aspects
    assert PARENT_URN not in aspects
    assert "testdb.main.parent" in source.report.filtered

    source = _sqlite_source(sqlite_db, include_tables=False, include_views=False)
    aspects = aspects_by_urn(source.get_workunits_internal())
    assert not [urn for urn in aspects if urn.startswith("urn:li:dataset:")]

    source = _sqlite_source(sqlite_db, schema_pattern={"deny": ["main"]})
    aspects = aspects_by_urn(source.get_workunits_internal())
    assert not [urn for urn in aspects if urn.startswith("urn:li:dataset:")]
    assert "main.*" in source.report.filtered


def test_platform_instance_is_stamped_on_datasets_and_urns(sqlite_db: str) -> None:
    source = _sqlite_source(sqlite_db, platform_instance="inst")

    aspects = aspects_by_urn(source.get_workunits_internal())

    for name in ("child", "child_view"):
        urn = (
            f"urn:li:dataset:(urn:li:dataPlatform:sqlite,inst.testdb.main.{name},PROD)"
        )
        dpi = single_aspect(aspects[urn], DataPlatformInstanceClass)
        assert (
            dpi.instance
            == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:sqlite,inst)"
        )


def test_domain_patterns_attach_domains_to_datasets_and_containers(
    sqlite_db: str,
) -> None:
    source = _sqlite_source(
        sqlite_db,
        domain={
            "urn:li:domain:children": {"allow": [".*child.*"]},
            "urn:li:domain:everything": {"allow": ["testdb$", "testdb\\.main$"]},
        },
    )

    aspects = aspects_by_urn(source.get_workunits_internal())

    assert single_aspect(aspects[CHILD_URN], DomainsClass).domains == [
        "urn:li:domain:children"
    ]
    assert single_aspect(aspects[VIEW_URN], DomainsClass).domains == [
        "urn:li:domain:children"
    ]
    assert not aspects_of_type(aspects[PARENT_URN], DomainsClass)
    container_domains = [
        single_aspect(a, DomainsClass).domains
        for urn, a in aspects.items()
        if urn.startswith("urn:li:container:")
    ]
    assert container_domains == [
        ["urn:li:domain:everything"],
        ["urn:li:domain:everything"],
    ]


class _LocationSource(_SqliteSource):
    """A source whose tables are backed by storage, as Athena's and Hive's are."""

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        return (
            f"about {table}",
            {"location": f"s3://bucket/{table}"},
            f"urn:li:dataset:(urn:li:dataPlatform:s3,bucket/{table},PROD)",
        )


def _location_source(connect_uri: str, **overrides: Any) -> _LocationSource:
    return _LocationSource(
        _SqliteConfig.model_validate({"connect_uri": connect_uri, **overrides}),
        PipelineContext(run_id="test"),
    )


LOCATION_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/child,PROD)"


def test_table_location_lineage_links_to_storage_with_column_lineage(
    sqlite_db: str,
) -> None:
    """A table backed by a storage location gets COPY lineage from it, column by
    column. Asserted on the last upstreamLineage emitted, which is the one the
    server keeps (see the xfail below for why there is more than one)."""
    aspects = aspects_by_urn(_location_source(sqlite_db).get_workunits_internal())

    lineage = aspects_of_type(aspects[CHILD_URN], UpstreamLineageClass)[-1]
    assert [(u.dataset, u.type) for u in lineage.upstreams] == [
        (LOCATION_URN, DatasetLineageTypeClass.COPY)
    ]
    assert lineage.fineGrainedLineages is not None
    assert [fg.downstreams for fg in lineage.fineGrainedLineages] == [
        [f"urn:li:schemaField:({CHILD_URN},id)"],
        [f"urn:li:schemaField:({CHILD_URN},parent_id)"],
        [f"urn:li:schemaField:({CHILD_URN},note)"],
    ]
    assert (
        single_aspect(aspects[CHILD_URN], DatasetPropertiesClass).description
        == "about child"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "_process_table emits the storage-location UpstreamLineage directly and "
        "also registers the same edge with the aggregator twice, which emits it "
        "again at the end of the run: two upstreamLineage aspects per table"
    ),
)
def test_table_location_lineage_is_emitted_once(sqlite_db: str) -> None:
    aspects = aspects_by_urn(_location_source(sqlite_db).get_workunits_internal())

    single_aspect(aspects[CHILD_URN], UpstreamLineageClass)


def test_table_location_lineage_can_be_switched_off(sqlite_db: str) -> None:
    source = _location_source(sqlite_db, include_table_location_lineage=False)

    aspects = aspects_by_urn(source.get_workunits_internal())

    assert not aspects_of_type(aspects[CHILD_URN], UpstreamLineageClass)


def test_view_without_readable_columns_is_still_emitted(sqlite_db: str) -> None:
    """A view whose columns cannot be reflected keeps its view logic and is
    still recorded as discovered, so query-history consumers do not mistake it
    for a temp table."""
    source = _sqlite_source(sqlite_db)
    engine = create_engine(sqlite_db)
    with engine.connect() as conn:
        inspector = inspect(conn)
        with mock.patch.object(inspector, "get_columns", side_effect=KeyError("v")):
            aspects = aspects_by_urn(
                source.loop_views(inspector, "main", source.config)
            )
    engine.dispose()

    assert VIEW_URN in aspects
    assert not aspects_of_type(aspects[VIEW_URN], SchemaMetadataClass)
    assert single_aspect(aspects[VIEW_URN], ViewPropertiesClass)
    assert "testdb.main.child_view" in log_contexts(source.report.warnings)
    assert "testdb.main.child_view" in source.discovered_datasets


def test_failing_table_or_view_is_warned_about_not_fatal(sqlite_db: str) -> None:
    source = _sqlite_source(sqlite_db)
    engine = create_engine(sqlite_db)
    with engine.connect() as conn:
        inspector = inspect(conn)
        with mock.patch.object(
            inspector, "get_pk_constraint", side_effect=RuntimeError("boom")
        ):
            table_aspects = aspects_by_urn(
                source.loop_tables(inspector, "main", source.config)
            )
        with mock.patch.object(
            inspector, "get_view_definition", side_effect=RuntimeError("boom")
        ):
            view_aspects = aspects_by_urn(
                source.loop_views(inspector, "main", source.config)
            )
    engine.dispose()

    assert CHILD_URN not in table_aspects and VIEW_URN not in view_aspects
    assert {"Error processing table", "Error processing view"} <= log_messages(
        source.report.warnings
    )
    assert {"main.child", "main.child_view"} <= log_contexts(source.report.warnings)
    assert not source.report.failures


def test_listing_failure_is_reported_as_failure(sqlite_db: str) -> None:
    source = _sqlite_source(sqlite_db)
    engine = create_engine(sqlite_db)
    with engine.connect() as conn:
        inspector = inspect(conn)
        with mock.patch.object(
            inspector, "get_table_names", side_effect=RuntimeError("no catalog")
        ):
            assert not list(source.loop_tables(inspector, "main", source.config))
        with mock.patch.object(
            inspector, "get_view_names", side_effect=RuntimeError("no catalog")
        ):
            assert not list(source.loop_views(inspector, "main", source.config))
    engine.dispose()

    assert {"Error processing tables", "Error processing views"} <= log_messages(
        source.report.failures
    )


def test_duplicate_table_names_are_emitted_once(sqlite_db: str) -> None:
    """Some dialects list a table twice (case variants, synonyms); the second
    listing must not produce a second dataset."""
    source = _sqlite_source(sqlite_db)
    engine = create_engine(sqlite_db)
    with engine.connect() as conn:
        inspector = inspect(conn)
        with mock.patch.object(
            inspector, "get_table_names", return_value=["child", "child"]
        ):
            workunits = list(source.loop_tables(inspector, "main", source.config))
    engine.dispose()

    assert [wu.id for wu in workunits if isinstance(wu, SqlWorkUnit)] == [
        "testdb.main.child"
    ]
    assert source.report.tables_scanned == 1


def test_get_table_properties_retries_with_quoted_schema_on_programming_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Snowflake needs the schema quoted when fetching table comments. Run at
    DEBUG so the retry's log call is formatted too: it used to pass the
    exception as an extra argument to an f-string message, which made logging
    raise inside the handler."""
    caplog.set_level(logging.DEBUG, logger="datahub.ingestion.source.sql.sql_common")
    source = get_test_sql_alchemy_source()
    inspector = mock.MagicMock()
    inspector.get_table_comment.side_effect = [
        ProgrammingError("stmt", {}, Exception("needs quoting")),
        {"text": "a comment", "properties": {"owner": "me"}},
    ]

    description, properties, location = source.get_table_properties(
        inspector, "sch", "tbl"
    )

    assert (description, properties, location) == ("a comment", {"owner": "me"}, None)
    assert inspector.get_table_comment.call_args_list[1] == mock.call("tbl", '"sch"')
    assert any("Retrying with quoted schema" in m for m in caplog.messages)


def test_get_table_properties_handles_dialects_without_comments() -> None:
    source = get_test_sql_alchemy_source()
    inspector = mock.MagicMock()
    inspector.get_table_comment.side_effect = NotImplementedError()

    assert source.get_table_properties(inspector, "sch", "tbl") == (None, {}, None)


def test_column_and_foreign_key_reflection_failures_degrade_gracefully() -> None:
    """Unreadable or empty column metadata and MySQL's KeyError-from-FK-reflection
    leave the table with no columns / FKs and a warning keyed by the dataset."""
    source = get_test_sql_alchemy_source()
    inspector = mock.MagicMock()
    inspector.get_columns.side_effect = RuntimeError("reflection failed")
    inspector.get_foreign_keys.side_effect = KeyError("CONSTRAINT_NAME")

    assert source._get_columns("db.sch.tbl", inspector, "sch", "tbl") == []
    assert source._get_foreign_keys("urn:li:dataset:x", inspector, "sch", "tbl") == []

    inspector.get_columns.side_effect = None
    inspector.get_columns.return_value = []
    assert source._get_columns("db.sch.tbl", inspector, "sch", "tbl") == []

    assert log_contexts(source.report.warnings) == {"db.sch.tbl"}
    messages = log_messages(source.report.warnings)
    assert "missing column information" in messages
    assert any("unable to get column information" in m for m in messages)


def test_schema_fields_carry_keys_partitions_and_tags() -> None:
    source = get_test_sql_alchemy_source()
    columns: List[Dict[str, Any]] = [
        {"name": "id", "type": types.Integer(), "nullable": False, "comment": "pk"},
        {"name": "dt", "type": types.Date(), "nullable": True, "full_type": "date"},
        {"name": "payload", "type": types.JSON(), "nullable": True},
    ]

    fields = source.get_schema_fields(
        "db.sch.tbl",
        columns,
        mock.MagicMock(),
        pk_constraints={"constrained_columns": ["id"]},
        partition_keys=["dt"],
        tags={"id": ["pii"], "dt": []},
    )

    by_name = {f.fieldPath: f for f in fields}
    assert by_name["id"].isPartOfKey and not by_name["id"].isPartitioningKey
    assert by_name["id"].description == "pk"
    assert by_name["id"].globalTags is not None
    assert [t.tag for t in by_name["id"].globalTags.tags] == ["urn:li:tag:pii"]
    assert by_name["dt"].isPartitioningKey and not by_name["dt"].isPartOfKey
    assert by_name["dt"].nativeDataType == "date"
    assert by_name["dt"].globalTags is None
    assert isinstance(by_name["payload"].type.type, RecordTypeClass)


def test_schema_fields_tolerate_list_shaped_pk_constraints() -> None:
    # Hive returns a list rather than a dict for primary keys.
    source = get_test_sql_alchemy_source()
    fields = source.get_schema_fields(
        "db.sch.tbl",
        [{"name": "id", "type": types.Integer(), "nullable": False}],
        mock.MagicMock(),
        pk_constraints=cast(Any, []),
    )
    assert not fields[0].isPartOfKey


def test_get_column_type_maps_known_unknown_and_unmapped_types() -> None:
    """Types DataHub knows to have no equivalent map to NullType silently; a
    type it has never seen also maps to NullType but is reported as an info."""
    report = SQLSourceReport()

    assert isinstance(
        get_column_type(report, "t", types.Interval()).type, NullTypeClass
    )
    assert not report.infos

    class _VendorType(TypeEngine):
        pass

    assert isinstance(get_column_type(report, "t", _VendorType()).type, NullTypeClass)
    assert "Unable to map column types to DataHub types" in log_titles(report.infos)


def test_get_schema_metadata_simplifies_field_paths_and_keeps_foreign_keys() -> None:
    report = SQLSourceReport()
    fields = [
        SchemaFieldClass(
            fieldPath="[version=2.0].[type=struct].[type=string].a",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="string",
        )
    ]
    fk = ForeignKeyConstraintClass("fk", [], [], "urn:li:dataset:other")

    schema = get_schema_metadata(
        report,
        "t",
        "TEST",
        [],
        foreign_keys=[fk],
        canonical_schema=fields,
        simplify_nested_field_paths=True,
    )

    assert [f.fieldPath for f in schema.fields] == ["a"]
    assert schema.foreignKeys == [fk]
    assert (
        get_schema_metadata(report, "t", "TEST", [], foreign_keys=[]).foreignKeys
        is None
    )


def test_get_db_name_reads_the_engine_url(sqlite_engine: Engine) -> None:
    source = get_test_sql_alchemy_source()
    with sqlite_engine.connect() as conn:
        assert source.get_db_name(inspect(conn)) == ""

    inspector = mock.MagicMock()
    inspector.engine = object()
    with pytest.raises(Exception, match="Unable to get database name"):
        source.get_db_name(inspector)


def test_test_connection_ignores_stateful_ingestion(sqlite_db: str) -> None:
    # Stateful ingestion needs a DataHub server, which a connection test must not.
    report = _SqliteSource.test_connection(
        {"connect_uri": sqlite_db, "stateful_ingestion": {"enabled": True}}
    )
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable


def test_error_records_a_failure_with_a_truncated_reason() -> None:
    source = get_test_sql_alchemy_source()
    source.error(logging.getLogger("test"), "key", "x" * 200)
    assert "key" in log_contexts(source.report.failures)
    assert "x" * 100 in log_messages(source.report.failures)


def test_aggregator_failure_is_reported_not_raised(sqlite_db: str) -> None:
    source = _sqlite_source(sqlite_db)
    with mock.patch.object(
        source.aggregator, "gen_metadata", side_effect=RuntimeError("graph down")
    ):
        assert not list(source._generate_aggregator_workunits())
    assert "Failed to generate lineage/usage from SQL parsing" in log_titles(
        source.report.failures
    )


# --- profiling -------------------------------------------------------------


class _ProfilingSource(_SqliteSource):
    """Exercises the hooks a profiling-capable source overrides; each test sets
    the instance attributes it needs."""

    def __init__(self, config: _SqliteConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.candidates: Optional[List[str]] = None
        self.partitioned: Set[str] = set()
        self.partition_of: Dict[str, str] = {}
        self.storage_bytes: Dict[str, int] = {}
        self.fail_metadata = False

    def generate_profile_candidates(
        self,
        inspector: Inspector,
        threshold_time: Optional[datetime.datetime],
        schema: str,
    ) -> Optional[List[str]]:
        if self.candidates is None:
            raise NotImplementedError()
        return self.candidates

    def is_table_partitioned(
        self, database: Optional[str], schema: str, table: str
    ) -> Optional[bool]:
        return table in self.partitioned

    def generate_partition_profiler_query(
        self, schema: str, table: str, partition_datetime: Optional[datetime.datetime]
    ) -> Tuple[Optional[str], Optional[str]]:
        return self.partition_of.get(table), None

    def add_profile_metadata(self, inspector: Inspector) -> None:
        if self.fail_metadata:
            raise RuntimeError("no size catalog")
        self.profile_metadata_info.dataset_name_to_storage_bytes = dict(
            self.storage_bytes
        )


_PROFILING = {
    "enabled": True,
    "max_workers": 1,
    "include_field_stddev_value": False,
    "report_dropped_profiles": True,
}


def _profiling_source(connect_uri: str, **overrides: Any) -> _ProfilingSource:
    config = _SqliteConfig.model_validate(
        {"connect_uri": connect_uri, "profiling": _PROFILING, **overrides}
    )
    return _ProfilingSource(config, PipelineContext(run_id="test"))


def _profiles(workunits: MetadataWorkUnitIterable) -> Dict[str, DatasetProfileClass]:
    profiles: Dict[str, DatasetProfileClass] = {}
    for urn, aspects in aspects_by_urn(
        wu for wu in workunits if isinstance(wu, MetadataWorkUnit)
    ).items():
        profiles[urn] = single_aspect(aspects, DatasetProfileClass)
    return profiles


def test_profiling_emits_a_profile_per_eligible_table(sqlite_db: str) -> None:
    """Storage size comes from the source's own catalog (add_profile_metadata)
    when the profiler has none; profile_pattern-denied tables are counted."""
    source = _profiling_source(sqlite_db, profile_pattern={"deny": [".*parent"]})
    source.storage_bytes = {"testdb.main.child": 4096}

    profiles = _profiles(source.get_profiling_internal())

    assert set(profiles) == {CHILD_URN}
    assert profiles[CHILD_URN].rowCount == 3
    assert profiles[CHILD_URN].columnCount == 3
    assert profiles[CHILD_URN].sizeInBytes == 4096
    assert source.report.entities_profiled == 1
    assert source.report.profiling_skipped_table_profile_pattern["main"] == 1
    assert "profile of testdb.main.parent" in source.report.filtered
    assert not source.report.warnings


def test_profile_candidates_restrict_what_is_profiled(sqlite_db: str) -> None:
    """A size/row/freshness limit asks the source for candidates; a table the
    source does not list is skipped as "other"."""
    source = _profiling_source(
        sqlite_db, profiling={**_PROFILING, "profile_if_updated_since_days": 1}
    )
    source.candidates = ["testdb.main.parent"]

    assert set(_profiles(source.get_profiling_internal())) == {PARENT_URN}
    assert source.report.profiling_skipped_other["main"] == 1
    assert source.report.num_tables_not_eligible_profiling["main"] == 1


def test_partitioned_tables_follow_the_partition_settings(sqlite_db: str) -> None:
    """child has a partition but partition profiling is off, so it is skipped
    quietly; parent is partitioned yet no partition could be picked, which is
    warned about under its dataset name."""
    source = _profiling_source(
        sqlite_db, profiling={**_PROFILING, "partition_profiling_enabled": False}
    )
    source.partition_of = {"child": "dt=2024-01-01"}
    source.partitioned = {"parent"}

    assert not _profiles(source.get_profiling_internal())
    assert log_contexts(source.report.warnings) == {"testdb.main.parent"}


def test_profile_metadata_failure_is_a_warning_not_a_stop(sqlite_db: str) -> None:
    source = _profiling_source(sqlite_db)
    source.fail_metadata = True

    profiles = _profiles(source.get_profiling_internal())

    assert set(profiles) == {CHILD_URN, PARENT_URN}
    assert profiles[CHILD_URN].sizeInBytes is None
    assert "profile_metadata" in log_contexts(source.report.warnings)


def test_profiling_is_off_unless_enabled(sqlite_db: str) -> None:
    source = _sqlite_source(sqlite_db)
    assert not source.is_profiling_enabled_internal()
    assert _profiling_source(sqlite_db).is_profiling_enabled_internal()


# --- stored procedures ------------------------------------------------------


class _ProcedureSource(_SqliteSource):
    def __init__(self, config: _SqliteConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.procedures: List[BaseProcedure] = []
        self.listing_error: Optional[Exception] = None

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> Iterable[BaseProcedure]:
        if self.listing_error is not None:
            raise self.listing_error
        return self.procedures


def _procedure_source(connect_uri: str, **overrides: Any) -> _ProcedureSource:
    config = _SqliteConfig.model_validate(
        {
            "connect_uri": connect_uri,
            "include_stored_procedures": True,
            "include_tables": False,
            "include_views": False,
            **overrides,
        }
    )
    return _ProcedureSource(config, PipelineContext(run_id="test"))


FLOW_URN = "urn:li:dataFlow:(sqlite,testdb.main.stored_procedures,PROD)"


def test_stored_procedures_become_jobs_under_one_flow(sqlite_db: str) -> None:
    source = _procedure_source(sqlite_db, procedure_pattern={"deny": [".*skipped"]})
    source.procedures = [
        make_procedure("load_child", "INSERT INTO child SELECT * FROM parent"),
        make_procedure("skipped", "SELECT 1"),
    ]

    aspects = aspects_by_urn(source.get_workunits_internal())

    assert single_aspect(aspects[FLOW_URN], SubTypesClass).typeNames == [
        FlowContainerSubTypes.PROCEDURE_CONTAINER
    ]
    job_urn = f"urn:li:dataJob:({FLOW_URN},load_child)"
    assert single_aspect(aspects[job_urn], DataJobInfoClass).name == "load_child"
    io = single_aspect(aspects[job_urn], DataJobInputOutputClass)
    assert io.inputDatasets == [PARENT_URN]
    assert io.outputDatasets == [CHILD_URN]
    assert f"urn:li:dataJob:({FLOW_URN},skipped)" not in aspects
    assert "testdb.main.skipped" in source.report.filtered


def test_procedure_listing_errors_are_warnings(sqlite_db: str) -> None:
    source = _procedure_source(sqlite_db)
    source.listing_error = RuntimeError("no catalog access")

    aspects = aspects_by_urn(source.get_workunits_internal())

    assert FLOW_URN not in aspects
    assert "Failed to get procedures for schema" in log_titles(source.report.warnings)


def test_dialect_without_procedure_support_is_a_warning(sqlite_db: str) -> None:
    source = _sqlite_source(
        sqlite_db,
        include_stored_procedures=True,
        include_tables=False,
        include_views=False,
    )

    aspects = aspects_by_urn(source.get_workunits_internal())

    assert not [urn for urn in aspects if urn.startswith("urn:li:dataFlow:")]
    assert "Stored procedures not supported" in log_titles(source.report.warnings)
    assert not source.report.failures


def test_unexpected_procedure_failure_is_a_failure(sqlite_db: str) -> None:
    source = _procedure_source(sqlite_db)
    inspector = mock.MagicMock()
    inspector.engine = object()  # get_db_name cannot read a url off this

    list(source.get_schema_level_workunits(inspector, "main", "testdb"))

    assert "Failed to list stored procedures for schema" in log_titles(
        source.report.failures
    )
