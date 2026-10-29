from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Tuple
from unittest import mock

from sqlalchemy import create_engine, inspect

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.vertica import VerticaConfig, VerticaSource
from datahub.ingestion.source.sql.vertica_inspector import VerticaInspector
from datahub.metadata.schema_classes import MetadataChangeEventClass
from tests.unit.test_vertica_source import _FakeConnection

# Row = (entity urn, aspect name, aspect JSON). MCEs are expanded to one row per
# snapshot aspect, in snapshot order, so the rows read the same for MCE and MCP output.
Row = Tuple[str, str, Dict[str, Any]]

_CATALOG: Dict[str, List[Dict[str, Any]]] = {
    "v_catalog.tables": [{"table_name": "orders", "owner_name": "alice"}],
    "v_catalog.views": [{"table_name": "sales_view", "owner_name": "dan"}],
    "v_catalog.view_tables": [
        {
            "database_name": "vmart",
            "table_name": "sales_view",
            "table_schema": "main",
            "reference_table_name": "orders",
            "reference_table_schema": "main",
        }
    ],
    "projection_name as table_name, owner_name": [
        {"table_name": "orders_super", "owner_name": "erin"},
        {"table_name": "orders_b0", "owner_name": "erin"},
    ],
    "SELECT projection_name from v_catalog.projections": [
        {"projection_name": "orders_super"},
        {"projection_name": "orders_b0"},
    ],
    "PROJECTION_COLUMNS": [
        {
            "projection_column_name": column,
            "data_type": data_type,
            "column_default": "",
            "is_nullable": True,
            "projection_name": projection,
        }
        for projection in ("orders_super", "orders_b0")
        for column, data_type in (("id", "Integer"), ("amount", "Numeric(10,2)"))
    ],
    # Rows exist so a projection lineage wu could be built; none is, because the
    # inspector keys them schema.name while the source looks up db.schema.name.
    "vs_projections": [
        {"basename": "orders", "schemaname": "main", "name": "orders_super"},
        {"basename": "orders", "schemaname": "main", "name": "orders_b0"},
    ],
    "SELECT model_name FROM models": [{"model_name": "churn"}],
    "owner_name from models": [{"owner_name": "frank"}],
    "attr_name= :attr_name": [{"predictor": "age", "coefficient": 0.5}],
    "GET_MODEL_ATTRIBUTE": [
        {
            "attr_name": "details",
            "attr_fields": "predictor,coefficient",
            "#_of_rows": 1,
        }
    ],
}


def _inspector() -> VerticaInspector:
    # Tables and views are reflected from sqlite (schema "main"); Vertica's own
    # catalog queries are answered by the fake connection.
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount NUMERIC)"
        )
        conn.exec_driver_sql("CREATE VIEW sales_view AS SELECT id FROM orders")
    inspector = VerticaInspector(inspect(engine))
    fake = _FakeConnection(_CATALOG)

    @contextmanager
    def _fake_connection() -> Iterator[_FakeConnection]:
        yield fake

    inspector._reflection_connection = _fake_connection  # type: ignore[method-assign,assignment]
    # sqlite has no table per projection, so its key reflection would raise.
    inspector.get_pk_constraint = lambda *args, **kwargs: {  # type: ignore[method-assign,assignment]
        "constrained_columns": [],
        "name": None,
    }
    inspector.get_foreign_keys = lambda *args, **kwargs: []  # type: ignore[method-assign]
    return inspector


def _run() -> List[Row]:
    inspector = _inspector()
    source = VerticaSource(
        VerticaConfig.model_validate({"host_port": "host:5433", "database": "vmart"}),
        PipelineContext(run_id="vertica-workunits-test"),
    )
    with mock.patch.object(
        VerticaSource, "get_inspectors", lambda self: iter([inspector])
    ):
        workunits = list(source.get_workunits())

    # Per-entity exceptions are swallowed into the report; an entity missing for
    # that reason must not pass as "not emitted".
    assert not source.report.failures
    assert not source.report.warnings

    rows: List[Row] = []
    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeEventClass):
            # Tables and views come from SQLAlchemySource as MCEs; projections
            # and models must not.
            assert wu.get_urn() not in _ASPECT_ORDER
            mcps = list(mcps_from_mce(wu.metadata))
        else:
            mcps = [wu.metadata]  # type: ignore[list-item]
        for mcp in mcps:
            assert mcp.entityUrn and mcp.aspectName and mcp.aspect
            rows.append((mcp.entityUrn, mcp.aspectName, mcp.aspect.to_obj()))
    return rows


def _ds(name: str) -> str:
    return make_dataset_urn("vertica", name, "PROD")


_PROJECTION_ORDER = [
    "ownership",
    "container",
    "status",
    "datasetProperties",
    "schemaMetadata",
    "subTypes",
    "browsePathsV2",
]
_ASPECT_ORDER: Dict[str, List[str]] = {
    _ds("vmart.main.orders_super"): _PROJECTION_ORDER,
    _ds("vmart.main.orders_b0"): _PROJECTION_ORDER,
    _ds("vmart.Entities.churn"): _PROJECTION_ORDER[1:],
}

_AUDIT_ZERO = {"time": 0, "actor": "urn:li:corpuser:unknown"}


def _schema_metadata(name: str, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "schemaName": name,
        "platform": "urn:li:dataPlatform:vertica",
        "version": 0,
        "created": _AUDIT_ZERO,
        "lastModified": _AUDIT_ZERO,
        "hash": "",
        "platformSchema": {
            "com.linkedin.pegasus2avro.schema.MySqlDDL": {"tableSchema": ""}
        },
        "fields": fields,
    }


def _number_field(path: str, native_type: str) -> Dict[str, Any]:
    return {
        "fieldPath": path,
        "nullable": True,
        "description": "",
        "type": {"type": {"com.linkedin.pegasus2avro.schema.NumberType": {}}},
        "nativeDataType": native_type,
        "recursive": False,
        "isPartOfKey": False,
    }


def _projection_aspects(projection: str) -> Dict[str, Dict[str, Any]]:
    return {
        "status": {"removed": False},
        "datasetProperties": {
            "customProperties": {
                "ROS_Count": "Not Available",
                "Projection_Type": "Not Available",
                "Is_Segmented": "Not Available",
                "Segmentation_key": "Not Available",
                "Projection_size": "0 KB",
                "Partition_Key": "Not Available",
                "Number_Of_Partitions": "0",
                "Projection_Cached": "False",
            },
            "name": projection,
            "description": (
                "Vertica physically stores table data in projections, which are "
                "collections of table columns. Projections store data in a format "
                "that optimizes query execution. For more info on projections and "
                "corresponding properties check out the Vertica Docs: "
                "https://www.vertica.com/docs"
            ),
            "tags": [],
        },
        "schemaMetadata": _schema_metadata(
            f"vmart.main.{projection}",
            [
                _number_field("id", "INTEGER"),
                _number_field("amount", "NUMERIC(10, 2)"),
            ],
        ),
    }


_MODEL_ASPECTS: Dict[str, Dict[str, Any]] = {
    "status": {"removed": False},
    "datasetProperties": {
        "customProperties": {
            "used_by": "frank",
            "Model Attributes": str(
                [
                    {
                        "attr_name": "details",
                        "attr_fields": "predictor,coefficient",
                        "#_of_rows": 1,
                    }
                ]
            ),
            "Model Specifications": str(
                [{"attr_name": "details", "predictor": ["age"], "coefficient": [0.5]}]
            ),
        },
        "name": "churn",
        "description": (
            "Vertica provides a number of machine learning functions for performing "
            "in-database analysis. These functions perform data preparation, model "
            "training, and predictive tasks. These properties shows the Model "
            "attributes and Specifications in the current schema."
        ),
        "tags": [],
    },
    # _process_models never fills its column list, so a model's schema is always empty.
    "schemaMetadata": _schema_metadata("vmart.Entities.churn", []),
}


def test_projection_and_model_aspect_order() -> None:
    rows = _run()

    for urn, expected in _ASPECT_ORDER.items():
        assert [aspect for row_urn, aspect, _ in rows if row_urn == urn] == expected


def test_projection_and_model_aspects() -> None:
    rows = _run()
    expected = {
        _ds("vmart.main.orders_super"): _projection_aspects("orders_super"),
        _ds("vmart.main.orders_b0"): _projection_aspects("orders_b0"),
        _ds("vmart.Entities.churn"): _MODEL_ASPECTS,
    }

    actual = {
        urn: {
            aspect: value
            for row_urn, aspect, value in rows
            if row_urn == urn and aspect in expected[urn]
        }
        for urn in expected
    }
    assert actual == expected


def test_lineage_rows() -> None:
    rows = _run()

    lineage = [
        (urn, value) for urn, aspect, value in rows if aspect == "upstreamLineage"
    ]
    assert lineage == [
        (
            _ds("vmart.main.sales_view"),
            {
                "upstreams": [
                    {
                        "auditStamp": _AUDIT_ZERO,
                        "dataset": _ds("vmart.main.orders"),
                        "type": "TRANSFORMED",
                    }
                ]
            },
        )
    ]
    # The view has exactly one status; the lineage wu adds none.
    assert [
        aspect
        for urn, aspect, _ in rows
        if urn == _ds("vmart.main.sales_view") and aspect == "status"
    ] == ["status"]
