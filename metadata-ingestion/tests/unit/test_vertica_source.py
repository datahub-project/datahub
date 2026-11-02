from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pytest
from sqlalchemy import create_engine, exc as sa_exc, inspect, types as sa_types
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine import make_url
from sqlalchemy.engine.default import DefaultDialect

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.vertica import VerticaConfig, VerticaSource
from datahub.ingestion.source.sql.vertica_inspector import (
    DataHubVerticaDialect,
    VerticaInspector,
)
from datahub.metadata.schema_classes import UpstreamLineageClass


class _FakeResult:
    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> List[Dict[str, Any]]:
        return self._rows

    def scalar(self) -> Optional[Any]:
        return next(iter(self._rows[0].values())) if self._rows else None

    def __iter__(self) -> Iterator[Tuple[Any, ...]]:
        for row in self._rows:
            yield tuple(v for k, v in row.items() if k != "_schema")


class _FakeConnection:
    """Answers catalog queries from canned rows keyed on a SQL substring.

    Substrings are tried in insertion order, so list the more specific ones first
    when several queries hit the same catalog table.
    """

    def __init__(self, rows_by_table: Dict[str, List[Dict[str, Any]]]) -> None:
        self._rows_by_table = rows_by_table
        self.executed: List[Tuple[str, Dict[str, Any]]] = []

    def execute(
        self, statement: Any, params: Optional[Dict[str, Any]] = None
    ) -> _FakeResult:
        sql = str(statement)
        self.executed.append((sql, params or {}))
        for table, rows in self._rows_by_table.items():
            if table in sql:
                schema = (params or {}).get("schema")
                return _FakeResult(
                    [row for row in rows if row.get("_schema", schema) == schema]
                )
        return _FakeResult([])


def _inspector_with(fake: _FakeConnection) -> VerticaInspector:
    inspector = VerticaInspector(inspect(create_engine("sqlite://")))

    @contextmanager
    def _fake_connection() -> Iterator[_FakeConnection]:
        yield fake

    inspector._reflection_connection = _fake_connection  # type: ignore[method-assign,assignment]
    return inspector


def test_reflection_connection_reuses_bound_connection():
    """Vertica reflection reuses the inspector's bound connection (no per-query checkout).

    get_inspectors() binds the inspector to a live Connection, so the Vertica-specific
    reflection methods must reuse it rather than opening a new connection each call —
    and must not close the borrowed connection.
    """
    engine = create_engine("sqlite://")
    conn = engine.connect()
    inspector = VerticaInspector(inspect(conn))

    with inspector._reflection_connection() as reused:
        assert reused is conn
    assert not conn.closed  # borrowed connection must stay open
    conn.close()


def test_reflection_connection_falls_back_to_short_lived_connection():
    """If bound to an Engine, open and close a short-lived connection instead."""
    engine = create_engine("sqlite://")
    inspector = VerticaInspector(inspect(engine))

    with inspector._reflection_connection() as opened:
        assert not opened.closed
    assert opened.closed  # short-lived connection is closed on exit


def test_vertica_uri_https():
    config = VerticaConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:5433",
            "database": "db",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "vertica+vertica_python://user:password@host:5433/db"
    )


def test_view_lineage_is_database_qualified():
    fake = _FakeConnection(
        {
            "v_catalog.view_tables": [
                {
                    "database_name": "vmart",
                    "table_name": "sales_view",
                    "table_schema": "public",
                    "reference_table_name": "customer_dimension",
                    "reference_table_schema": "public",
                },
                {
                    "database_name": "vmart",
                    "table_name": "sales_view",
                    "table_schema": "public",
                    "reference_table_name": "store_sales_fact",
                    "reference_table_schema": "store",
                },
            ]
        }
    )
    inspector = _inspector_with(fake)

    assert inspector._populate_view_lineage("sales_view", "public") == {
        "vmart.public.sales_view": [
            ("vmart.public.customer_dimension", "[]", "[]"),
            ("vmart.store.store_sales_fact", "[]", "[]"),
        ]
    }

    source = VerticaSource(
        VerticaConfig.model_validate({"host_port": "host:5433", "database": "vmart"}),
        PipelineContext(run_id="vertica-test"),
    )
    dataset_name = source.get_identifier(
        schema="public", entity="sales_view", inspector=inspector
    )
    lineage = source._get_upstream_lineage_info(
        f"urn:li:dataset:(urn:li:dataPlatform:vertica,{dataset_name},PROD)",
        inspector,
        "sales_view",
        "public",
    )
    assert isinstance(lineage, UpstreamLineageClass)
    assert [upstream.dataset for upstream in lineage.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:vertica,vmart.public.customer_dimension,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:vertica,vmart.store.store_sales_fact,PROD)",
    ]


def test_schema_wide_queries_run_once_per_schema():
    fake = _FakeConnection(
        {
            "v_catalog.tables": [
                {"_schema": "public", "table_name": "a", "owner_name": "alice"},
                {"_schema": "public", "table_name": "b", "owner_name": "bob"},
                {"_schema": "store", "table_name": "c", "owner_name": "carol"},
            ]
        }
    )
    inspector = _inspector_with(fake)

    assert inspector.get_table_owner("a", "public") == "alice"
    assert inspector.get_table_owner("b", "public") == "bob"
    assert len(fake.executed) == 1

    assert inspector.get_table_owner("c", "store") == "carol"
    assert len(fake.executed) == 2
    assert [params["schema"] for _, params in fake.executed] == ["public", "store"]


def test_projection_properties_query_once_per_schema():
    fake = _FakeConnection({})
    inspector = _inspector_with(fake)

    inspector.get_projection_comment("p1", "public")
    queries_for_first_projection = len(fake.executed)
    inspector.get_projection_comment("p2", "public")

    assert queries_for_first_projection > 0
    assert len(fake.executed) == queries_for_first_projection


def test_vertica_scheme_resolves_to_datahub_dialect():
    assert (
        make_url("vertica+vertica_python://user@host:5433/db").get_dialect()
        is DataHubVerticaDialect
    )


@pytest.mark.parametrize(
    "method_name",
    sorted(name for name in dir(PGDialect) if name.startswith("get_multi_")),
)
def test_multi_reflection_avoids_pg_catalog(method_name: str) -> None:
    # PGDialect's get_multi_* query pg_catalog, which Vertica lacks; Table
    # autoload must go through DefaultDialect's per-table fallbacks instead.
    assert getattr(DataHubVerticaDialect, method_name) is getattr(
        DefaultDialect, method_name
    )


def test_view_definition_reads_vertica_catalog():
    fake = _FakeConnection({"v_catalog.views": [{"view_definition": "SELECT 1"}]})

    definition = DataHubVerticaDialect().get_view_definition(
        fake,  # type: ignore[arg-type]
        "Sales_View",
        schema="Public",
    )

    assert definition == "SELECT 1"
    ((sql, params),) = fake.executed
    assert "pg_catalog" not in sql
    assert params == {"schema": "public", "view": "sales_view"}


def test_projection_columns_map_vertica_types():
    fake = _FakeConnection(
        {
            "PROJECTION_COLUMNS": [
                {
                    "projection_column_name": name,
                    "data_type": data_type,
                    "column_default": "",
                    "is_nullable": True,
                    "projection_name": projection,
                }
                for projection, name, data_type in [
                    ("orders_super", "id", "Integer"),
                    ("orders_super", "code", "Varchar(80)"),
                    ("orders_super", "amount", "Numeric(10,2)"),
                    ("orders_super", "created_at", "TimestampTz"),
                    ("orders_super", "day", "Date"),
                    ("orders_super", "note", "Long Varchar(1000)"),
                    ("orders_super", "token", "Uuid"),
                    ("orders_super", "age", "Interval Day to Second"),
                    ("other_super", "ignored", "Integer"),
                ]
            ]
        }
    )
    inspector = _inspector_with(fake)

    columns = {
        column["name"]: column
        for column in inspector.get_projection_columns("ORDERS_SUPER", "Public")
    }

    assert list(columns) == [
        "id",
        "code",
        "amount",
        "created_at",
        "day",
        "note",
        "token",
        "age",
    ]
    assert fake.executed[0][1] == {"schema": "public"}
    assert isinstance(columns["id"]["type"], sa_types.INTEGER)
    assert isinstance(columns["code"]["type"], sa_types.VARCHAR)
    assert columns["code"]["type"].length == 80
    assert isinstance(columns["amount"]["type"], sa_types.NUMERIC)
    assert (columns["amount"]["type"].precision, columns["amount"]["type"].scale) == (
        10,
        2,
    )
    assert isinstance(columns["created_at"]["type"], sa_types.TIMESTAMP)
    assert columns["created_at"]["type"].timezone is True
    assert isinstance(columns["day"]["type"], sa_types.DATE)
    assert columns["note"]["type"].length == 1000
    assert columns["token"]["type"].__visit_name__ == "UUID"
    assert isinstance(columns["age"]["type"], INTERVAL)
    assert all(column["nullable"] for column in columns.values())
    assert {column["table_name"] for column in columns.values()} == {"orders_super"}


def test_unrecognized_column_type_falls_back_to_null_type():
    inspector = _inspector_with(_FakeConnection({}))

    with pytest.warns(sa_exc.SAWarning, match="geo_blob"):
        column = inspector._get_column_info(
            "shape", "geo_blob", None, True, "t", "public"
        )

    assert column["type"] is sa_types.NULLTYPE


def test_sequence_default_marks_integer_column_autoincrement():
    inspector = _inspector_with(_FakeConnection({}))

    column = inspector._get_column_info(
        "id", "integer", "nextval('orders_seq')", False, "orders", "public"
    )

    assert column["autoincrement"] is True
    assert column["default"] == "nextval('\"public\".orders_seq')"
    assert column["nullable"] is False


def test_projection_names_filter_by_lowercased_schema():
    fake = _FakeConnection(
        {
            "v_catalog.projections": [
                {"_schema": "public", "projection_name": "orders_super"},
                {"_schema": "public", "projection_name": "orders_b0"},
            ]
        }
    )

    assert _inspector_with(fake).get_projection_names("PUBLIC") == [
        "orders_super",
        "orders_b0",
    ]


def test_view_and_projection_owners_match_case_insensitively():
    fake = _FakeConnection(
        {
            "v_catalog.views": [
                {"_schema": "public", "table_name": "Sales_View", "owner_name": "dan"}
            ],
            "v_catalog.projections": [
                {
                    "_schema": "public",
                    "table_name": "Orders_Super",
                    "owner_name": "erin",
                }
            ],
        }
    )
    inspector = _inspector_with(fake)

    assert inspector.get_view_owner("sales_view", "Public") == "dan"
    assert inspector.get_view_owner("missing", "public") is None
    assert inspector.get_projection_owner("ORDERS_SUPER", "public") == "erin"
    assert inspector.get_projection_owner("missing", "public") is None


def test_projection_comment_properties():
    # Real Vertica returns identifiers with their original case, while the
    # queries lower-case projection_name for matching.
    fake = _FakeConnection(
        {
            "ros_count": [
                {"ros_count": 3, "projection_name": "orders_super"},
                {"ros_count": 9, "projection_name": "other_super"},
            ],
            "is_super_projection": [
                {
                    "is_super_projection": True,
                    "is_key_constraint_projection": False,
                    "is_aggregate_projection": False,
                    "has_expressions": True,
                    "projection_name": "orders_super",
                }
            ],
            "is_segmented": [
                {
                    "is_segmented": True,
                    "segment_expression": "hash(orders.id)",
                    "projection_name": "orders_super",
                }
            ],
            "count(partition_key)": [
                {"projection_name": "orders_super", "Partition_Size": 4}
            ],
            "partition_key FROM": [
                {"projection_name": "orders_super", "partition_key": "2020"}
            ],
            "used_bytes": [
                {"used_bytes": 2048, "projection_name": "orders_super"},
                {"used_bytes": 1024, "projection_name": "orders_super"},
            ],
            "DEPOT_PIN_POLICIES": [{"cnt": 1, "object_name": "orders"}],
        }
    )

    comment = _inspector_with(fake).get_projection_comment("Orders_Super", "public")

    assert comment["text"].startswith("Vertica physically stores table data")
    assert comment["properties"] == {
        "ROS_Count": "3",
        "Projection_Type": "is_super_projection, has_expressions",
        "Is_Segmented": "True",
        "Segmentation_key": "hash(orders.id)",
        "Projection_size": "3 KB",
        "Partition_Key": "2020",
        "Number_Of_Partitions": "4",
        "Projection_Cached": "True",
    }


def test_projection_comment_defaults_when_catalog_has_no_rows():
    comment = _inspector_with(_FakeConnection({})).get_projection_comment(
        "orders_super", "public"
    )

    assert comment["properties"] == {
        "ROS_Count": "Not Available",
        "Projection_Type": "Not Available",
        "Is_Segmented": "Not Available",
        "Segmentation_key": "Not Available",
        "Projection_size": "0 KB",
        "Partition_Key": "Not Available",
        "Number_Of_Partitions": "0",
        "Projection_Cached": "False",
    }


def test_projection_lineage_maps_projection_to_anchor_table():
    fake = _FakeConnection(
        {
            "vs_projections": [
                {"basename": "orders", "schemaname": "public", "name": "orders_super"},
                {"basename": "orders", "schemaname": "public", "name": "orders_b0"},
            ]
        }
    )

    assert _inspector_with(fake)._populate_projection_lineage(
        "orders_super", "public"
    ) == {
        "public.orders_super": [("public.orders", "[]", "[]")],
        "public.orders_b0": [("public.orders", "[]", "[]")],
    }


def test_database_properties_for_eon_cluster():
    fake = _FakeConnection(
        {
            "v_catalog.shards": [{"database_mode": "Eon"}],
            "storage_locations": [
                {"location_path": "s3://bucket/a"},
                {"location_path": "s3://bucket/b"},
            ],
            "subclusters.subcluster_name": [
                {"subcluster_name": "primary", "subclustersize": "12"}
            ],
            "disk_storage": [{"cluster_size": 12}],
        }
    )

    assert _inspector_with(fake)._get_database_properties("vmart") == {
        "cluster_type": "Eon",
        "cluster_size": "12 GB",
        "subcluster": " primary -- 12 GB |  ",
        "communal_storage_path": "s3://bucket/a | s3://bucket/b | ",
    }


def test_schema_properties():
    fake = _FakeConnection(
        {
            "v_catalog.projections": [{"_schema": "public", "pc": 5}],
            "USER_FUNCTIONS": [
                {"_schema": "public", "function_name": "f1"},
                {"_schema": "public", "function_name": "f2"},
            ],
            "USER_LIBRARIES": [
                {"_schema": "public", "lib_name": "lib", "description": "udx lib"}
            ],
        }
    )

    assert _inspector_with(fake)._get_schema_properties("Public") == {
        "projection_count": "5",
        "udx_list": "f1, f2, ",
        "udx_language": "lib -- udx lib |  ",
    }


def test_models_names_and_comment():
    fake = _FakeConnection(
        {
            "SELECT model_name FROM models": [
                {"_schema": "public", "model_name": "churn"}
            ],
            "owner_name from models": [{"owner_name": "frank"}],
            "attr_name= :attr_name": [
                {"predictor": "age", "coefficient": 0.5},
                {"predictor": "tenure", "coefficient": 1.5},
            ],
            "GET_MODEL_ATTRIBUTE": [
                {
                    "attr_name": "details",
                    "attr_fields": "predictor,coefficient",
                    "#_of_rows": 2,
                }
            ],
        }
    )
    inspector = _inspector_with(fake)

    assert inspector.get_models_names("PUBLIC") == ["churn"]

    comment = inspector.get_model_comment("churn", "Public")

    assert comment["properties"]["used_by"] == "frank"
    assert comment["properties"]["Model Attributes"] == str(
        [
            {
                "attr_name": "details",
                "attr_fields": "predictor,coefficient",
                "#_of_rows": 2,
            }
        ]
    )
    assert comment["properties"]["Model Specifications"] == str(
        [
            {
                "attr_name": "details",
                "predictor": ["age", "tenure"],
                "coefficient": [0.5, 1.5],
            }
        ]
    )
    assert "public.churn" in {params.get("model_full") for _, params in fake.executed}


def test_temp_table_names_read_vertica_catalog():
    fake = _FakeConnection({"is_temp_table": [{"table_name": "scratch"}]})

    assert DataHubVerticaDialect().get_temp_table_names(fake) == ["scratch"]  # type: ignore[arg-type]
    assert "pg_catalog" not in fake.executed[0][0]


def test_view_definition_uses_default_schema_when_unspecified():
    fake = _FakeConnection({"v_catalog.views": [{"view_definition": "SELECT 2"}]})
    dialect = DataHubVerticaDialect()
    dialect.default_schema_name = "Public"

    assert dialect.get_view_definition(fake, "v") == "SELECT 2"  # type: ignore[arg-type]
    assert fake.executed[0][1] == {"schema": "public", "view": "v"}
