import json
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    DataModelTable,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.file_lineage import qri_part_id
from datahub.ingestion.source.qlik_sense.qlik_base_api import UNREAD_DATA_CONNECTIONS
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.qvd_lineage import (
    QvdPathResolver,
    app_qvd_lineage,
    canonical_path,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    DataConnectionInfo,
    QvdReference,
    parse_script,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    MetadataChangeProposalClass,
    SchemaMetadataClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from tests.unit.qlik_sense.qlik_fakes import ON_PREM, ONPREM_CONFIG


def _urn(name: str) -> str:
    return make_dataset_urn("qlik-sense", name)


def test_qvd_loads_and_stores_are_parsed() -> None:
    parsed = parse_script(
        """
        Orders: LOAD id, amount AS total FROM [lib://QVD Data/raw/orders.qvd] (qvd);
        Concatenate (Orders) LOAD id, amount AS total FROM 'lib://QVD Data/raw/old.qvd';
        Hist: LOAD * FROM \\\\fileserver\\share\\hist.qvd (qvd);
        STORE Orders INTO [lib://QVD Data/clean/orders.qvd];
        STORE id, total AS amount FROM Orders INTO [lib://QVD Data/clean/ids.qvd] (qvd);
        """
    )
    assert [
        (load.label, load.file.connection, load.file.path) for load in parsed.qvd_loads
    ] == [
        ("Orders", "QVD Data", "raw/orders.qvd"),
        ("Orders", "QVD Data", "raw/old.qvd"),
        ("Hist", None, "\\\\fileserver\\share\\hist.qvd"),
    ]
    assert [(f.name, f.sources) for f in parsed.qvd_loads[0].fields.fields] == [
        ("id", ["id"]),
        ("total", ["amount"]),
    ]
    assert [(store.label, store.file.path) for store in parsed.qvd_stores] == [
        ("Orders", "clean/orders.qvd"),
        ("Orders", "clean/ids.qvd"),
    ]
    assert parsed.qvd_stores[0].fields is None
    stored_fields = parsed.qvd_stores[1].fields
    assert stored_fields is not None
    assert [f.name for f in stored_fields.fields] == ["id", "amount"]


def test_concatenate_after_a_preceding_load_adds_to_its_table() -> None:
    parsed = parse_script(
        """
        Orders:
        LOAD id, amount AS total;
        LOAD * FROM [lib://C/a.qvd] (qvd);
        Concatenate LOAD * FROM [lib://C/b.qvd] (qvd);
        """
    )
    assert [(load.label, load.file.path) for load in parsed.qvd_loads] == [
        ("Orders", "a.qvd"),
        ("Orders", "b.qvd"),
    ]


@pytest.mark.parametrize(
    "script, expected",
    [
        (
            "T: LOAD x AS final; LOAD a AS x; LOAD * FROM [lib://C/a.qvd] (qvd);",
            [("final", ["a"])],
        ),
        # The statement reading the file renames its columns as well.
        (
            "T: LOAD x AS final; LOAD a AS x FROM [lib://C/a.qvd] (qvd);",
            [("final", ["a"])],
        ),
        (
            "T: LOAD *, x AS y; LOAD a AS x, b FROM [lib://C/a.qvd] (qvd);",
            [("x", ["a"]), ("b", ["b"]), ("y", ["a"])],
        ),
    ],
)
def test_qvd_columns_go_through_every_preceding_load(
    script: str, expected: List[Any]
) -> None:
    [load] = parse_script(script.replace("; ", ";\n")).qvd_loads
    assert [(f.name, f.sources) for f in load.fields.fields] == expected


@pytest.mark.parametrize(
    "statement",
    [
        # LOAD assumes txt when the format is omitted and the file isn't .qvd.
        "Csv: LOAD * FROM [lib://QVD Data/x.csv];",
        "Txt: LOAD * FROM [lib://QVD Data/x.qvd] (txt);",
        "STORE Orders INTO [lib://QVD Data/x.csv] (txt);",
        # Qlik names an unlabelled table, which a STORE can't be matched to.
        "LOAD * FROM [lib://QVD Data/x.qvd] (qvd);",
        # A mapping table is only used by ApplyMap.
        "Map: MAPPING LOAD a, b FROM [lib://QVD Data/map.qvd] (qvd);",
    ],
)
def test_other_formats_and_tables_are_not_qvd_lineage(statement: str) -> None:
    parsed = parse_script(statement)
    assert not parsed.qvd_loads and not parsed.qvd_stores
    assert not parsed.unresolved_qvd_paths


@pytest.mark.parametrize(
    "statement, unresolved",
    [
        ("All: LOAD * FROM [lib://QVD Data/*.qvd] (qvd);", "lib://QVD Data/*.qvd"),
        ("Rel: LOAD * FROM data\\rel.qvd (qvd);", "data\\rel.qvd"),
        ("Var: LOAD * FROM [$(vUnset)x.qvd] (qvd);", "$(vUnset)x.qvd"),
    ],
)
def test_qvd_paths_naming_no_single_file_are_reported(
    statement: str, unresolved: str
) -> None:
    parsed = parse_script(statement)
    assert not parsed.qvd_loads
    assert parsed.unresolved_qvd_paths == [unresolved]


def test_set_variables_in_qvd_paths_are_expanded() -> None:
    parsed = parse_script(
        "SET vQvd = lib://QVD Data/clean/;\n"
        "Orders: LOAD id FROM [$(vQvd)orders.qvd] (qvd);"
    )
    assert parsed.qvd_loads[0].file == QvdReference(
        connection="QVD Data", path="clean/orders.qvd"
    )


def test_qvd_paths_resolve_to_one_canonical_file() -> None:
    resolver = QvdPathResolver(
        folders={"QVD Data": "\\\\qlik-sense\\QVD\\", "Other": "D:\\Data"},
        ignored_connections=["ServerLogFolder"],
        folder_aliases={"//qlik-sense/qvd": "//qlik-sense.corp.local/qvd"},
    )
    assert resolver.resolve(
        QvdReference(connection="QVD Data", path="Clean\\..\\clean/Orders.qvd")
    ) == ("//qlik-sense.corp.local/qvd/clean/orders.qvd")
    assert (
        resolver.resolve(QvdReference(path="\\\\QLIK-SENSE.corp.local\\qvd\\x.qvd"))
        == "//qlik-sense.corp.local/qvd/x.qvd"
    )
    assert resolver.resolve(QvdReference(connection="Other", path="y.qvd")) == (
        "d:/data/y.qvd"
    )
    # The monitoring apps' log QVDs and the Engine's buffers are Qlik's own.
    assert (
        resolver.resolve(QvdReference(connection="ServerLogFolder", path="a.qvd"))
        is None
    )
    assert (
        resolver.resolve(
            QvdReference(path="C:\\ProgramData\\Qlik\\Sense\\Engine\\Buffers\\b.qvd")
        )
        is None
    )
    assert resolver.resolve(QvdReference(connection="Missing", path="z.qvd")) is None
    assert resolver.unresolved == ["lib://Missing/z.qvd"]
    assert canonical_path("C:\\Data\\") == "c:/data"


ETL_SCRIPT = """
Orders: LOAD id, amount FROM [lib://QVD Data/raw/orders.qvd] (qvd);
STORE Orders INTO [lib://QVD Data/clean/orders.qvd] (qvd);
"""
REPORT_SCRIPT = """
Sales: LOAD id, amount AS revenue FROM [lib://Shared QVDs/clean/orders.qvd] (qvd);
"""
CONNECTIONS = {
    "QVD Data": DataConnectionInfo(name="QVD Data", folder_path="\\\\nas\\qvd\\"),
    # The same folder, reached through another connection.
    "Shared QVDs": DataConnectionInfo(name="Shared QVDs", folder_path="//NAS/qvd"),
    "Apps": DataConnectionInfo(name="Apps", folder_path="//nas/apps"),
}
RAW = _urn("//nas/qvd/raw/orders.qvd")
CLEAN = _urn("//nas/qvd/clean/orders.qvd")
# Not a folder connection, so Qlik gives no folder for it.
STORAGE_ETL_SCRIPT = ETL_SCRIPT.replace(
    "lib://QVD Data/clean", "lib://Lake - Azure/Clean"
)
STORAGE_MAPPING: Dict[str, Dict[str, Any]] = {
    "Lake - Azure": {"storage_uri": "abfss://lake@account.dfs.core.windows.net/qlik"}
}
STORAGE_QVD = make_dataset_urn("abs", "lake/qlik/Clean/orders.qvd")


def _app(
    app_id: str,
    script: Optional[str],
    data_model: Optional[Dict[str, List[str]]] = None,
) -> App:
    return App.model_validate(
        {
            "data_model_tables": [
                DataModelTable.from_engine(
                    {"qName": name, "qFields": [{"qName": field} for field in fields]}
                )
                for name, fields in (data_model or {}).items()
            ],
            "id": app_id,
            "qTitle": app_id,
            "qri": f"qri:app:sense://{app_id}",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "stream-1",
            "script": script,
            "script_read_failed": script is None,
        }
    )


def _run(
    apps: List[App],
    current: Optional[UpstreamLineageClass] = None,
    connections_unread: bool = False,
    file_connection_to_storage: Optional[Dict[str, Dict[str, Any]]] = None,
    on_prem: Optional[Dict[str, Any]] = None,
) -> "tuple[QlikSenseSource, Dict[str, Dict[str, Any]]]":
    graph = MagicMock()
    # The QVDs' lineage as DataHub holds it before the run.
    graph.get_aspect.return_value = current
    graph.get_entities.side_effect = lambda entity_name, urns, aspects: (
        {urn: {UpstreamLineageClass.ASPECT_NAME: (current, None)} for urn in urns}
        if current is not None
        else {}
    )
    with patch(
        "datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI"
    ) as mock_api_cls:
        stream = MagicMock(id="stream-1")
        stream.name = "Finance"
        api = mock_api_cls.return_value
        api.get_spaces.return_value = [stream]
        api.spaces = {"stream-1": "Finance"}
        api.unread_items = (
            [UnreadItem(name=UNREAD_DATA_CONNECTIONS, cause="503")]
            if connections_unread
            else []
        )
        api.failed_user_lookups = {}
        api.change_check_failures = []
        api.lineage_read_failures = []
        api.field_lineage_failures = []
        api.opened_without_data = []
        api.include_read_failures = []
        api.reload_statement_read_failures = []
        api.engine_refusals = None
        api.get_user_name.return_value = None
        api.get_data_connections.return_value = (
            {} if connections_unread else CONNECTIONS
        )
        api.get_items.return_value = apps
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(
                {
                    "on_prem": {**ON_PREM, **(on_prem or {})},
                    "file_connection_to_storage": file_connection_to_storage or {},
                }
            ),
            PipelineContext(run_id="test-qvd-lineage", graph=graph),
        )
        aspects: Dict[str, Dict[str, Any]] = {}
        for wu in source.get_workunits_internal():
            if not wu.is_primary_source:
                aspects.setdefault(wu.get_urn(), {})["not_primary_source"] = True
            for aspect_type in (
                UpstreamLineageClass,
                SchemaMetadataClass,
                DatasetPropertiesClass,
                SubTypesClass,
                ContainerClass,
                ContainerPropertiesClass,
            ):
                aspect = wu.get_aspect_of_type(aspect_type)
                if aspect:
                    aspects.setdefault(wu.get_urn(), {})[aspect_type.__name__] = aspect
            if (
                isinstance(wu.metadata, MetadataChangeProposalClass)
                and wu.metadata.changeType == "PATCH"
                and wu.metadata.aspect is not None
            ):
                aspects.setdefault(wu.get_urn(), {}).setdefault("patch", []).extend(
                    op["path"] for op in json.loads(wu.metadata.aspect.value)
                )
                if wu.metadata.aspectName == UpstreamLineageClass.ASPECT_NAME:
                    aspects[wu.get_urn()].setdefault("lineage_ops", []).extend(
                        (op["op"], _pointer_parts(op["path"]))
                        for op in json.loads(wu.metadata.aspect.value)
                    )
    return source, aspects


def _pointer_parts(path: str) -> List[str]:
    return [part.replace("~1", "/").replace("~0", "~") for part in path.split("/")[1:]]


def _upstreams(aspects: Dict[str, Dict[str, Any]], urn: str, op: str) -> List[str]:
    """The upstream datasets a QVD lineage patch adds or removes."""
    return [
        parts[1]
        for kind, parts in aspects[urn].get("lineage_ops", [])
        if kind == op and parts[0] == "upstreams"
    ]


def _added_columns(
    aspects: Dict[str, Dict[str, Any]], urn: str
) -> Dict[str, List[str]]:
    columns: Dict[str, List[str]] = {}
    for kind, parts in aspects[urn].get("lineage_ops", []):
        if kind == "add" and parts[0] == "fineGrainedLineages":
            # fineGrainedLineages / transform / downstream / query / upstream
            columns.setdefault(parts[2], []).append(parts[4])
    return columns


def test_qvd_lineage_runs_from_etl_app_through_qvd_to_reporting_app() -> None:
    source, aspects = _run(
        [_app("etl-app", ETL_SCRIPT), _app("report-app", REPORT_SCRIPT)]
    )
    orders, sales = _urn("etl-app.orders"), _urn("report-app.sales")

    def upstreams(urn: str) -> List[str]:
        return [u.dataset for u in aspects[urn]["UpstreamLineageClass"].upstreams]

    def column_lineage(urn: str) -> Dict[str, List[str]]:
        lineage = aspects[urn]["UpstreamLineageClass"].fineGrainedLineages or []
        return {cll.downstreams[0]: cll.upstreams for cll in lineage}

    assert upstreams(orders) == [RAW]
    # A QVD is not built by one app, so its lineage is patched.
    assert _upstreams(aspects, CLEAN, "add") == [orders]
    assert upstreams(sales) == [CLEAN]
    assert _added_columns(aspects, CLEAN) == {
        make_schema_field_urn(CLEAN, "id"): [make_schema_field_urn(orders, "id")],
        make_schema_field_urn(CLEAN, "amount"): [
            make_schema_field_urn(orders, "amount")
        ],
    }
    assert column_lineage(sales)[make_schema_field_urn(sales, "revenue")] == [
        make_schema_field_urn(CLEAN, "amount")
    ]
    # The stored QVD takes its schema from the table written into it, and the
    # raw QVD, which no app writes, from the fields read out of it.
    for qvd in (CLEAN, RAW):
        schema = aspects[qvd]["SchemaMetadataClass"]
        assert [field.fieldPath for field in schema.fields] == ["id", "amount"]
        assert aspects[qvd]["SubTypesClass"].typeNames == ["QVD"]
    assert aspects[CLEAN]["DatasetPropertiesClass"].name == "orders"
    assert source.reporter.qvds_found == 3
    assert not source.reporter.warnings


def test_qvds_of_a_storage_connection_take_the_file_dataset_qlik_cloud_uses() -> None:
    # Separators doubled where a variable left a path part empty.
    report = REPORT_SCRIPT.replace(
        "lib://Shared QVDs/clean", "lib://$(vPrefix)Lake - Azure//Clean"
    )
    source, aspects = _run(
        [_app("etl-app", STORAGE_ETL_SCRIPT), _app("report-app", report)],
        file_connection_to_storage=STORAGE_MAPPING,
    )

    assert _upstreams(aspects, STORAGE_QVD, "add") == [_urn("etl-app.orders")]
    assert [
        u.dataset
        for u in aspects[_urn("report-app.sales")]["UpstreamLineageClass"].upstreams
    ] == [STORAGE_QVD]
    # Another platform's dataset: only its lineage is written, and never as
    # the source that could remove it when stale.
    assert set(aspects[STORAGE_QVD]) == {"patch", "lineage_ops", "not_primary_source"}
    assert not source.reporter.unresolved_qvd_paths


def test_storage_qvd_gets_table_lineage_only() -> None:
    report = REPORT_SCRIPT.replace(
        "lib://Shared QVDs/clean", "lib://Lake - Azure/Clean"
    )
    source, aspects = _run(
        [_app("etl-app", STORAGE_ETL_SCRIPT), _app("report-app", report)],
        file_connection_to_storage=STORAGE_MAPPING,
    )
    sales = aspects[_urn("report-app.sales")]["UpstreamLineageClass"]

    # The storage sources write no QVD schema, so its fields don't exist.
    assert not _added_columns(aspects, STORAGE_QVD)
    assert not any(
        STORAGE_QVD in upstream
        for cll in sales.fineGrainedLineages or []
        for upstream in cll.upstreams or []
    )
    assert source.reporter.storage_qvds_resolved == 2
    assert source.reporter.qvds_found == 1


def test_folder_connection_mapped_to_storage_takes_the_storage_dataset() -> None:
    clean = make_dataset_urn("abs", "lake/qlik/clean/orders.qvd")
    _, aspects = _run(
        [_app("etl-app", ETL_SCRIPT)],
        file_connection_to_storage={"QVD Data": STORAGE_MAPPING["Lake - Azure"]},
    )

    assert _upstreams(aspects, clean, "add") == [_urn("etl-app.orders")]
    assert CLEAN not in aspects and RAW not in aspects


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_cloud_and_onprem_name_a_storage_qvd_alike(mock_api_cls: MagicMock) -> None:
    script = "Orders: LOAD id FROM [lib://Lake - Azure/Clean//orders.qvd] (qvd);"
    cloud = QlikSenseSource(
        QlikSourceConfig.model_validate(
            {
                "tenant_hostname": "tenant.example.com",
                "api_key": "key",
                "file_connection_to_storage": STORAGE_MAPPING,
            }
        ),
        PipelineContext(run_id="test-qvd-lineage"),
    )
    cloud._data_connections = {}
    cloud._app_space_ids["etl-app"] = "space-1"
    cloud._file_sources_by_app["etl-app"] = cloud._file_source_resolver(
        _app("etl-app", script), parse_script(script)
    )
    # As Qlik Cloud's lineage graph names the file: each part hashed.
    qri = (
        f"qri:file:adls://{qri_part_id('unknown')}"
        f"#{qri_part_id('Clean/orders.qvd')}#{qri_part_id('item')}"
    )
    _, aspects = _run(
        [_app("etl-app", STORAGE_ETL_SCRIPT.replace("/Clean/", "/Clean//"))],
        file_connection_to_storage=STORAGE_MAPPING,
    )

    assert cloud._file_source_resolution("etl-app", qri).urn == STORAGE_QVD
    assert _upstreams(aspects, STORAGE_QVD, "add") == [_urn("etl-app.orders")]


def test_onprem_storage_connections_need_a_storage_uri() -> None:
    with pytest.raises(ValueError, match="storage_uri"):
        QlikSourceConfig.model_validate(
            {**ONPREM_CONFIG, "file_connection_to_storage": {"Lake - Azure": {}}}
        )


def test_qvd_lineage_is_only_added_to_when_an_app_script_could_not_be_read() -> None:
    source, aspects = _run(
        [
            _app("etl-app", ETL_SCRIPT),
            _app("report-app", REPORT_SCRIPT),
            _app("unreadable-app", None),
        ]
    )
    # The unreadable app may also write orders.qvd, so edges are only added,
    # which keeps whatever that app wrote.
    assert "UpstreamLineageClass" not in aspects[CLEAN]
    assert _upstreams(aspects, CLEAN, "add") == [_urn("etl-app.orders")]
    assert not _upstreams(aspects, CLEAN, "remove")
    # The QVD an app read this run writes still gets its schema, but one only
    # read keeps its last one.
    assert "SchemaMetadataClass" in aspects[CLEAN]
    assert "SchemaMetadataClass" not in aspects[RAW]
    assert "Lineage of Qlik QVDs only added to" in [
        w.title for w in source.reporter.warnings
    ]


def test_qvd_no_app_writes_any_more_loses_its_old_writer() -> None:
    # A table of an app read this run that no longer stores into the QVD.
    old_writer = _urn("report-app.orders")
    _, aspects = _run(
        [_app("report-app", REPORT_SCRIPT)],
        current=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=old_writer, type=DatasetLineageTypeClass.COPY)
            ]
        ),
    )
    assert _upstreams(aspects, CLEAN, "remove") == [old_writer]


@pytest.mark.parametrize(
    "store",
    [
        pytest.param(
            "FOR EACH vT IN 'a', 'b'\nSTORE Orders INTO [lib://QVD Data/$(vT).qvd] (qvd);\nNEXT vT;",
            id="loop",
        ),
        pytest.param("STORE Orders INTO [lib://Unknown/x.qvd] (qvd);", id="connection"),
    ],
)
def test_app_with_an_unresolved_store_adds_its_other_writes(store: str) -> None:
    old_writer = _urn("report-app.orders")
    staged = _urn("//nas/qvd/staged/orders.qvd")
    source, aspects = _run(
        [
            _app(
                "report-app",
                REPORT_SCRIPT
                + "Orders: LOAD 1 AS id AUTOGENERATE 1;\n"
                + "STORE Orders INTO [lib://QVD Data/staged/orders.qvd] (qvd);\n"
                + store,
            )
        ],
        current=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=old_writer, type=DatasetLineageTypeClass.COPY)
            ]
        ),
    )
    # The unresolved STORE may still write the QVD the old edge comes from.
    assert source._apps_qvds_unknown == {
        "report-app": "a STORE path could not be resolved"
    }
    assert not _upstreams(aspects, CLEAN, "remove")
    # The STORE that resolved still gets its edge.
    assert _upstreams(aspects, staged, "add") == [old_writer]
    assert source._app_qvds["report-app"].writes_complete is False


def test_qvd_written_by_an_unread_app_keeps_its_schema() -> None:
    # The writer is outside this run, such as in a stream the recipe leaves out.
    _, aspects = _run(
        [_app("report-app", REPORT_SCRIPT)],
        current=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=_urn("etl-app.orders"), type=DatasetLineageTypeClass.COPY
                )
            ]
        ),
    )
    # The fields the reporting app reads are only a subset.
    assert "SchemaMetadataClass" not in aspects[CLEAN]
    assert not _upstreams(aspects, CLEAN, "remove")


def test_qvd_writers_are_kept_when_data_connections_could_not_be_listed() -> None:
    old_writer = _urn("report-app.orders")
    source, aspects = _run(
        [_app("report-app", REPORT_SCRIPT)],
        current=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=old_writer, type=DatasetLineageTypeClass.COPY)
            ]
        ),
        connections_unread=True,
    )
    # Every lib:// path is unresolved, so the app's writes are unknown.
    assert source._apps_qvds_unknown == {
        "report-app": "data connections could not be listed"
    }
    assert not any(_upstreams(aspects, urn, "remove") for urn in aspects)


def test_storage_qvd_writes_are_added_when_data_connections_could_not_be_listed() -> (
    None
):
    old_writer = _urn("etl-app.old_orders")
    _, aspects = _run(
        [_app("etl-app", STORAGE_ETL_SCRIPT)],
        current=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=old_writer, type=DatasetLineageTypeClass.COPY)
            ]
        ),
        connections_unread=True,
        file_connection_to_storage=STORAGE_MAPPING,
    )
    # The storage path needs no folder listing, so its write is added, while
    # the app's other writes are unknown and its old edge is kept.
    assert _upstreams(aspects, STORAGE_QVD, "add") == [_urn("etl-app.orders")]
    assert not any(_upstreams(aspects, urn, "remove") for urn in aspects)


def test_stale_qlik_edge_is_removed_from_a_storage_qvd() -> None:
    manual = make_dataset_urn("snowflake", "db.schema.orders")
    _, aspects = _run(
        [_app("etl-app", STORAGE_ETL_SCRIPT)],
        current=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=manual, type=DatasetLineageTypeClass.COPY),
                UpstreamClass(
                    dataset=_urn("etl-app.old_orders"),
                    type=DatasetLineageTypeClass.COPY,
                ),
            ]
        ),
        file_connection_to_storage=STORAGE_MAPPING,
    )
    assert _upstreams(aspects, STORAGE_QVD, "remove") == [_urn("etl-app.old_orders")]
    assert aspects[STORAGE_QVD]["not_primary_source"]


@pytest.mark.parametrize(
    "mapping, expected",
    [
        pytest.param(
            {"platform_instance": "lake-prod"},
            make_dataset_urn_with_platform_instance(
                "abs", "lake/qlik/Clean/orders.qvd", "lake-prod"
            ),
            id="platform_instance",
        ),
        pytest.param(
            {"env": "DEV"},
            make_dataset_urn("abs", "lake/qlik/Clean/orders.qvd", "DEV"),
            id="env",
        ),
        pytest.param(
            {"convert_urns_to_lowercase": True},
            make_dataset_urn("abs", "lake/qlik/clean/orders.qvd"),
            id="lowercase",
        ),
    ],
)
def test_storage_qvd_urn_follows_the_connection_mapping(
    mapping: Dict[str, Any], expected: str
) -> None:
    _, aspects = _run(
        [_app("etl-app", STORAGE_ETL_SCRIPT)],
        file_connection_to_storage={
            "Lake - Azure": {**STORAGE_MAPPING["Lake - Azure"], **mapping}
        },
    )
    assert _upstreams(aspects, expected, "add") == [_urn("etl-app.orders")]


def test_qvd_stored_without_its_extension_is_not_an_app_table() -> None:
    other_qvd = _urn("//nas/qvd/staging/orders.dat")
    _, aspects = _run(
        [_app("etl-app", ETL_SCRIPT)],
        current=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=other_qvd, type=DatasetLineageTypeClass.COPY)
            ]
        ),
    )
    assert other_qvd not in _upstreams(aspects, CLEAN, "remove")


def test_qvd_lineage_keeps_edges_not_from_app_tables() -> None:
    manual = make_dataset_urn("snowflake", "db.schema.orders")
    _, aspects = _run(
        [_app("etl-app", ETL_SCRIPT)],
        current=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=manual, type=DatasetLineageTypeClass.COPY),
                # A table of an app read this run that no longer stores here.
                UpstreamClass(
                    dataset=_urn("etl-app.old_orders"),
                    type=DatasetLineageTypeClass.COPY,
                ),
                # A table of an app this run did not read, which may still.
                UpstreamClass(
                    dataset=_urn("other-app.orders"), type=DatasetLineageTypeClass.COPY
                ),
            ]
        ),
    )
    assert _upstreams(aspects, CLEAN, "remove") == [_urn("etl-app.old_orders")]
    assert _upstreams(aspects, CLEAN, "add") == [_urn("etl-app.orders")]


def test_connection_names_match_without_case() -> None:
    resolver = QvdPathResolver(
        folders={"QVD Data": "//nas/qvd"},
        ignored_connections=[],
        folder_aliases={},
    )
    assert resolver.resolve(QvdReference(connection="qvd data", path="x.qvd")) == (
        "//nas/qvd/x.qvd"
    )


def test_stored_qvd_takes_the_data_model_fields_a_load_star_hides() -> None:
    parsed = parse_script(
        "Orders: LOAD * FROM [lib://QVD Data/raw.qvd] (qvd);\n"
        "STORE Orders INTO [lib://QVD Data/clean.qvd] (qvd);"
    )
    resolver = QvdPathResolver(
        folders={"QVD Data": "//nas/qvd"}, ignored_connections=[], folder_aliases={}
    )
    lineage = app_qvd_lineage(
        "etl-app",
        parsed,
        resolver,
        _urn,
        table_fields={"etl-app.orders": ["id", "amount"]},
    )
    [write] = lineage.qvds.writes[_urn("//nas/qvd/clean.qvd")]
    assert [(f.name, f.source) for f in write.fields or []] == [
        ("id", "id"),
        ("amount", "amount"),
    ]


def test_qvds_are_in_folder_containers_of_their_share() -> None:
    _, aspects = _run([_app("etl-app", ETL_SCRIPT), _app("report-app", REPORT_SCRIPT)])
    names = {
        urn: props["ContainerPropertiesClass"].name
        for urn, props in aspects.items()
        if "ContainerPropertiesClass" in props
        and props["SubTypesClass"].typeNames == ["Folder"]
    }
    # One container per folder, however many QVDs and apps it holds.
    assert sorted(names.values()) == ["clean", "nas", "qvd", "raw"]
    clean_folder = aspects[CLEAN]["ContainerClass"].container
    raw_folder = aspects[RAW]["ContainerClass"].container
    assert names[clean_folder] == "clean"
    assert names[raw_folder] == "raw"
    share = aspects[clean_folder]["ContainerClass"].container
    assert names[share] == "qvd"
    assert names[aspects[share]["ContainerClass"].container] == "nas"


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_every_app_emits_the_folders_of_its_qvds(mock_api_cls: MagicMock) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(ONPREM_CONFIG),
        PipelineContext(run_id="test-qvd-lineage"),
    )

    def folder_urns(paths: Dict[str, str]) -> List[str]:
        return [
            wu.get_urn()
            for wu in source._gen_file_entities(paths, DatasetSubTypes.QLIK_QVD)
            if wu.get_aspect_of_type(ContainerPropertiesClass)
        ]

    first = folder_urns({CLEAN: "//nas/qvd/clean/orders.qvd"})
    second = folder_urns({RAW: "//nas/qvd/raw/orders.qvd"})

    # Each app's checkpoint keeps only what it emitted, so the server and
    # share it shares with the other app are emitted by both.
    assert len(first) == len(second) == 3
    assert first[:2] == second[:2]


def _qvd_folders(config: Dict[str, Any], path: str) -> List[Tuple[str, str]]:
    """(container URN, name) of the folders a QVD's entities emit, in order."""
    with patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI"):
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(config),
            PipelineContext(run_id="test-qvd-lineage"),
        )
    return [
        (wu.get_urn(), properties.name)
        for wu in source._gen_file_entities(
            {_urn(path): path}, DatasetSubTypes.QLIK_QVD
        )
        if (properties := wu.get_aspect_of_type(ContainerPropertiesClass))
    ]


@pytest.mark.parametrize(
    "path, folders",
    [
        ("//nas/qvd/raw/orders.qvd", ["nas", "qvd", "raw"]),
        ("d:/data/sales/orders.qvd", ["d:", "data", "sales"]),
        # Relative to a Directory statement, so the root is unknown.
        ("orders.qvd", []),
        ("data/orders.qvd", []),
    ],
)
def test_qvd_folders_start_at_a_share_or_drive(path: str, folders: List[str]) -> None:
    assert [name for _, name in _qvd_folders(ONPREM_CONFIG, path)] == folders


def test_qvd_folder_keys_include_the_platform_instance() -> None:
    path = "//nas/qvd/raw/orders.qvd"
    plain = _qvd_folders(ONPREM_CONFIG, path)
    with_instance = _qvd_folders({**ONPREM_CONFIG, "platform_instance": "onprem"}, path)
    # The same folders, under other keys, so two sites never share a folder.
    assert [name for _, name in with_instance] == [name for _, name in plain]
    assert not {urn for urn, _ in with_instance} & {urn for urn, _ in plain}


def _removed_columns(aspects: Dict[str, Dict[str, Any]], urn: str) -> List[str]:
    return [
        parts[4]
        for kind, parts in aspects[urn].get("lineage_ops", [])
        if kind == "remove" and parts[0] == "fineGrainedLineages"
    ]


def test_stale_qvd_edges_of_known_apps_are_removed_while_another_is_unknown() -> None:
    _, aspects = _run(
        [
            _app("etl-app", ETL_SCRIPT),
            _app("report-app", REPORT_SCRIPT),
            _app("unreadable-app", None),
        ],
        current=UpstreamLineageClass(
            upstreams=[
                # A table of an app read this run that no longer stores here.
                UpstreamClass(
                    dataset=_urn("report-app.orders"),
                    type=DatasetLineageTypeClass.COPY,
                ),
                # The unreadable app may still store here.
                UpstreamClass(
                    dataset=_urn("unreadable-app.orders"),
                    type=DatasetLineageTypeClass.COPY,
                ),
            ]
        ),
    )
    assert _upstreams(aspects, CLEAN, "remove") == [_urn("report-app.orders")]


def test_store_with_unknown_fields_keeps_its_column_edges() -> None:
    orders = _urn("etl-app.orders")
    _, aspects = _run(
        [
            _app(
                "etl-app",
                "Orders: LOAD * FROM [lib://QVD Data/raw/orders.qvd] (qvd);\n"
                "STORE Orders INTO [lib://QVD Data/clean/orders.qvd] (qvd);\n",
            )
        ],
        current=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=orders, type=DatasetLineageTypeClass.COPY)
            ],
            fineGrainedLineages=[
                FineGrainedLineageClass(
                    upstreamType="FIELD_SET",
                    upstreams=[make_schema_field_urn(orders, "id")],
                    downstreamType="FIELD",
                    downstreams=[make_schema_field_urn(CLEAN, "id")],
                )
            ],
        ),
    )
    # LOAD * hides which fields the STORE writes, so the edge an earlier run
    # wrote stays.
    assert not _removed_columns(aspects, CLEAN)
    assert not _upstreams(aspects, CLEAN, "remove")


def test_qvd_paths_use_the_variables_of_the_last_reload() -> None:
    # Set with LET, or in an included script, so only the Engine knows them.
    parsed = parse_script(
        "Orders: LOAD id FROM [lib://$(vPrefix)QVD Data/raw/orders.qvd] (qvd);\n"
        "Lots: LOAD id FROM [$(vShare)/lots.qvd] (qvd);\n"
        "Gone: LOAD id FROM [$(vMissing)/gone.qvd] (qvd);",
        app_variables={
            "vPrefix": "",
            "vShare": "lib://$(vPrefix)QVD Data/staging",
        },
    )

    assert [load.file for load in parsed.qvd_loads] == [
        QvdReference(connection="QVD Data", path="raw/orders.qvd"),
        QvdReference(connection="QVD Data", path="staging/lots.qvd"),
    ]
    assert parsed.unresolved_qvd_paths == ["$(vMissing)/gone.qvd"]


def test_lib_path_may_separate_the_connection_with_a_backslash() -> None:
    parsed = parse_script("Lots: LOAD id FROM [lib://QVD Data\\gold\\lots.qvd] (qvd);")

    [load] = parsed.qvd_loads
    assert load.file.connection == "QVD Data"
    resolver = QvdPathResolver(
        folders={"QVD Data": "//nas/qvd"}, ignored_connections=[], folder_aliases={}
    )
    assert resolver.resolve(load.file) == "//nas/qvd/gold/lots.qvd"


def test_unset_variable_in_a_connection_name_expands_to_nothing() -> None:
    # An empty prefix is not in the app's variables, as setting a variable to
    # nothing deletes it, and Qlik expands an unknown variable to nothing.
    parsed = parse_script(
        "Orders: LOAD id FROM [lib://$(vPrefix)QVD Data/raw/orders.qvd] (qvd);\n"
        "Other: LOAD id FROM [lib://$(vPrefix)Unknown/raw/x.qvd] (qvd);",
        app_variables={"vOther": "x"},
    )
    resolver = QvdPathResolver(
        folders={"QVD Data": "//nas/qvd"}, ignored_connections=[], folder_aliases={}
    )

    resolved = [resolver.resolve(load.file) for load in parsed.qvd_loads]

    # Only taken when what is left is a folder connection.
    assert resolved == ["//nas/qvd/raw/orders.qvd", None]
    assert resolver.unresolved == ["lib://$(vPrefix)Unknown/raw/x.qvd"]


def test_prefix_is_not_taken_as_unset_when_the_variables_were_not_read() -> None:
    parsed = parse_script(
        "Orders: LOAD 1 AS id AUTOGENERATE 1;\n"
        "STORE Orders INTO [lib://$(vPrefix)QVD Data/orders.qvd] (qvd);"
    )
    resolver = QvdPathResolver(
        folders={"QVD Data": "//nas/qvd"},
        ignored_connections=[],
        folder_aliases={},
        variables_known=False,
    )
    lineage = app_qvd_lineage(
        "app-1", parsed, resolver, lambda path: path, table_fields={}
    )

    # The prefix may name another connection, so the STORE stays unresolved
    # and the app's QVD writes are not taken as all known.
    assert not lineage.qvds.writes
    assert lineage.has_unresolved_store


@pytest.mark.parametrize(
    "connection, ignored, variables_known",
    [
        pytest.param("Lake", ["lake"], True, id="ignored_connection"),
        pytest.param("$(vPrefix)Lake", [], False, id="prefix_with_variables_unread"),
    ],
)
def test_storage_connection_qvd_takes_no_file_dataset(
    connection: str, ignored: List[str], variables_known: bool
) -> None:
    resolver = QvdPathResolver(
        folders={},
        ignored_connections=ignored,
        folder_aliases={},
        variables_known=variables_known,
        storage_urns={"Lake": lambda path: f"file:{path}"},
    )
    assert (
        resolver.storage_urn(QvdReference(connection=connection, path="x.qvd")) is None
    )


@pytest.mark.parametrize(
    "folder, expected",
    [
        (
            "'lib://QVD Data/raw/'",
            QvdReference(connection="QVD Data", path="raw/orders.qvd"),
        ),
        (
            '"lib://QVD Data/raw/"',
            QvdReference(connection="QVD Data", path="raw/orders.qvd"),
        ),
        ("'//nas/qvd/raw/'", QvdReference(path="//nas/qvd/raw/orders.qvd")),
        ("'//nas/o'brien/'", QvdReference(path="//nas/o'brien/orders.qvd")),
    ],
)
def test_path_from_a_quoted_variable_drops_its_quotes(
    folder: str, expected: QvdReference
) -> None:
    # SET keeps the quotes in the variable's value.
    parsed = parse_script(
        "Orders: LOAD id FROM [$(vFolder)orders.qvd] (qvd);",
        app_variables={"vFolder": folder},
    )

    [load] = parsed.qvd_loads
    assert load.file == expected


def test_qvd_named_only_by_a_variable_is_a_qvd_load() -> None:
    parsed = parse_script(
        "Orders: LOAD id FROM [$(vFile)];",
        app_variables={"vFile": "lib://QVD Data/raw/orders.qvd"},
    )

    [load] = parsed.qvd_loads
    assert load.file == QvdReference(connection="QVD Data", path="raw/orders.qvd")


def test_expression_variable_is_not_expanded_in_a_qvd_path() -> None:
    parsed = parse_script(
        "Orders: LOAD id FROM [lib://QVD Data/$(vDir)/orders.qvd] (qvd);",
        app_variables={"vDir": "=Year(Today())"},
    )
    assert not parsed.qvd_loads
    assert parsed.unresolved_qvd_paths == ["lib://QVD Data/$(vDir)/orders.qvd"]


@pytest.mark.parametrize(
    "script",
    [
        pytest.param(
            "FOR EACH vTable IN 'Orders', 'Customers'\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\nNEXT vTable;",
            id="loop variable",
        ),
        pytest.param(
            "FOR i = 1 TO 2\nLET vTable = TableName($(i));\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\nNEXT i;",
            id="LET in a loop",
        ),
        pytest.param(
            "SUB L(vTable)\nT: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\n"
            "END SUB;\nCALL L(Lower(vSource));",
            id="SUB parameter from an expression",
        ),
        pytest.param(
            "IF vEnv = 'A' THEN\nLET vTable = Lower('Orders' & vRegion);\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\nEND IF;",
            id="LET in the IF branch that reads it",
        ),
        pytest.param(
            "IF vEnv = 'A' THEN\nSET vTable = 'Orders';\nELSE\n"
            "SET vTable = 'Hist';\nEND IF;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\n"
            "SET vTable = 'Old';",
            id="IF branches that disagree, set again later",
        ),
        pytest.param(
            "IF vRegion = 'EU' THEN\nIF vEnv = 'A' THEN\nSET vTable = 'Orders';\n"
            "ELSE\nSET vTable = 'Hist';\nEND IF;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\nEND IF;",
            id="IF branches that disagree, inside an outer IF",
        ),
        pytest.param(
            "LET vTable = Lower('Orders' & vRegion);\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\n"
            "LET vTable = Lower('Hist' & vRegion);",
            id="LET set again later",
        ),
        pytest.param(
            "FOR EACH vT IN 'Orders', 'Customers'\nSET vTable = $(vT);\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\nNEXT vT;",
            id="SET copying a loop variable",
        ),
        pytest.param(
            "FOR EACH vTable IN 'Orders', 'Customers'\n"
            "$(Include=lib://Scripts/common.qvs);\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\nNEXT vTable;",
            id="loop variable after an include",
        ),
        pytest.param(
            "LET vTable = Lower('Orders' & vRegion);\n"
            "IF vEnv = 'A' THEN\nSET vOther = 'x';\nEND IF;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\n"
            "LET vTable = Lower('Hist' & vRegion);",
            id="unknown before an IF that doesn't set it",
        ),
    ],
)
def test_qvd_path_from_a_varying_variable_stays_unresolved(script: str) -> None:
    # The app keeps only the last value, which names one of several files.
    parsed = parse_script(script, app_variables={"vTable": "Customers"})
    assert not parsed.qvd_loads
    assert parsed.unresolved_qvd_paths == ["lib://QVD Data/$(vTable).qvd"]


def test_qvd_path_from_a_let_expression_takes_the_reload_value() -> None:
    parsed = parse_script(
        "LET vTable = Lower('Orders' & vRegion);\n"
        "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
        app_variables={"vTable": "orders_eu"},
    )
    [load] = parsed.qvd_loads
    assert load.file == QvdReference(connection="QVD Data", path="orders_eu.qvd")


@pytest.mark.parametrize(
    "script, path",
    [
        pytest.param(
            "/* LET vTable = Lower('Old'); */\n"
            "LET vTable = Lower('Orders' & vRegion);\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            "Orders.qvd",
            id="second LET in a comment",
        ),
        pytest.param(
            "SUB Unused\nLET vTable = Lower('Old');\nEND SUB;\n"
            "LET vTable = Lower('Orders' & vRegion);\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            "Orders.qvd",
            id="second LET in a SUB never called",
        ),
        pytest.param(
            "LET vTable = Lower('Orders' & vRegion);\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\n"
            "EXIT SCRIPT;\nLET vTable = Lower('Old');",
            "Orders.qvd",
            id="second LET after EXIT SCRIPT",
        ),
        pytest.param(
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\n"
            "LET vTable = Lower('Orders' & vRegion);",
            "Orders.qvd",
            id="before the script sets it",
        ),
        pytest.param(
            "FOR EACH vTable IN 'Orders', 'Customers'\nNEXT vTable;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            "Orders.qvd",
            id="after the loop",
        ),
        pytest.param(
            "FOR EACH Year IN 2024, 2025\nNEXT Year;\nSET vTable = Year;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            # SET holds text, not an expression that reads Year.
            "Year.qvd",
            id="SET value naming a loop variable",
        ),
        pytest.param(
            "FOR EACH vT IN 'a', 'b'\nIF vT = 'a' THEN\nSET vTable = 'Orders';\n"
            "ELSE\nSET vTable = 'Customers';\nEND IF;\nNEXT vT;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            "Orders.qvd",
            id="after a loop whose branches disagree",
        ),
        pytest.param(
            "IF vEnv = 'A' THEN\nLET vTable = Lower('Orders' & vRegion);\nELSE\n"
            "LET vTable = Lower('Hist' & vRegion);\nEND IF;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            "Orders.qvd",
            id="after LET in IF branches",
        ),
        pytest.param(
            "IF vRegion = 'EU' THEN\nIF vEnv = 'A' THEN\nSET vTable = 'Orders';\n"
            "ELSE\nSET vTable = 'Hist';\nEND IF;\nEND IF;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            "Orders.qvd",
            id="after nested IF branches that disagree",
        ),
        pytest.param(
            "IF vEnv = 'A' THEN\nSET vTable = 'Hist';\nELSE\nSET vTable = 'Old';\n"
            "END IF;\nIF vRegion = 'EU' THEN\nSET vTable = 'Orders';\nELSE\n"
            "SET vTable = 'Customers';\nEND IF;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            "Orders.qvd",
            id="after a second IF that disagrees",
        ),
        pytest.param(
            "SWITCH vEnv\nCASE 'A'\nSET vTable = 'Hist';\nCASE 'B'\n"
            "SET vTable = 'Customers';\nDEFAULT\nSET vTable = 'Old';\nEND SWITCH;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            "Orders.qvd",
            id="after SWITCH cases that disagree",
        ),
        pytest.param(
            "LET vTable = Lower('Hist' & vRegion);\nIF vEnv = 'A' THEN\n"
            "SET vTable = 'Orders';\nEND IF;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            "Orders.qvd",
            id="after an IF that overrides a LET",
        ),
        pytest.param(
            "LET vTable = Lower('Hist' & vRegion);\nFOR EACH vT IN 'a', 'b'\n"
            "IF vT = 'a' THEN\nSET vTable = 'Orders';\nELSE\n"
            "SET vTable = 'Customers';\nEND IF;\nNEXT vT;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            "Orders.qvd",
            id="after a loop whose IF overrides a LET",
        ),
    ],
)
def test_qvd_path_from_a_variable_known_at_that_point_resolves(
    script: str, path: str
) -> None:
    # The app keeps a variable's last value, which is also its value before
    # the script first sets it, as variables persist between reloads.
    parsed = parse_script(script, app_variables={"vTable": "Orders"})
    assert [load.file.path for load in parsed.qvd_loads] == [path]


def test_sub_called_twice_takes_the_app_value_after_its_last_call() -> None:
    parsed = parse_script(
        "SUB PickTable\nIF vEnv = 'A' THEN\nSET vTable = 'Hist';\nELSE\n"
        "SET vTable = 'Customers';\nEND IF;\nEND SUB;\n"
        "CALL PickTable;\n"
        "First: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);\n"
        "CALL PickTable;\n"
        "Second: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
        app_variables={"vTable": "Orders"},
    )
    # The second CALL assigns vTable again, so after the first it can differ.
    assert parsed.unresolved_qvd_paths == ["lib://QVD Data/$(vTable).qvd"]
    assert [(load.label, load.file.path) for load in parsed.qvd_loads] == [
        ("Second", "Orders.qvd")
    ]


def test_store_through_a_connection_prefix_set_in_branches_resolves() -> None:
    parsed = parse_script(
        "IF DocumentPath() LIKE '*Dev*' THEN\nSET vPrefix = 'Dev_';\nELSE\n"
        "SET vPrefix = '';\nEND IF;\n"
        "Orders: LOAD 1 AS id AUTOGENERATE 1;\n"
        "STORE Orders INTO [lib://$(vPrefix)QVD Data/orders.qvd] (qvd);",
        app_variables={"vPrefix": "Dev_"},
    )
    assert [store.file for store in parsed.qvd_stores] == [
        QvdReference(connection="Dev_QVD Data", path="orders.qvd")
    ]
    assert not parsed.has_unresolved_store


def test_connection_prefix_set_in_branches_the_app_lacks_stays_unresolved() -> None:
    # Expanding the prefix to nothing would name the wrong folder connection.
    parsed = parse_script(
        "IF DocumentPath() LIKE '*Dev*' THEN\nSET vPrefix = 'Dev_';\nELSE\n"
        "SET vPrefix = 'Test_';\nEND IF;\n"
        "Orders: LOAD 1 AS id AUTOGENERATE 1;\n"
        "STORE Orders INTO [lib://$(vPrefix)QVD Data/orders.qvd] (qvd);",
    )
    assert not parsed.qvd_stores
    assert parsed.has_unresolved_store


@pytest.mark.parametrize(
    "app_variables, path",
    [
        pytest.param({"vTable": "Orders"}, "Orders.qvd", id="app has it"),
        pytest.param({}, None, id="app lacks it"),
    ],
)
def test_if_without_else_setting_an_unset_variable(
    app_variables: Dict[str, str], path: Optional[str]
) -> None:
    parsed = parse_script(
        "IF vEnv = 'A' THEN\nSET vTable = 'Orders';\nEND IF;\n"
        "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
        app_variables=app_variables,
    )
    assert [load.file.path for load in parsed.qvd_loads] == ([path] if path else [])
    assert bool(parsed.unresolved_qvd_paths) == (path is None)


@pytest.mark.parametrize(
    "app_variables, path",
    [
        pytest.param({"vTable": "Customers"}, "Customers.qvd", id="app has it"),
        pytest.param({}, None, id="app lacks it"),
    ],
)
def test_include_in_one_branch_leaves_known_variables_to_the_app(
    app_variables: Dict[str, str], path: Optional[str]
) -> None:
    # As after an include outside a block: the included script may set it.
    parsed = parse_script(
        "SET vTable = 'Orders';\nIF vEnv = 'A' THEN\n"
        "$(Include=lib://Scripts/common.qvs);\nEND IF;\n"
        "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
        app_variables=app_variables,
    )
    assert [load.file.path for load in parsed.qvd_loads] == ([path] if path else [])
    assert bool(parsed.unresolved_qvd_paths) == (path is None)


def test_connection_prefix_set_in_a_loop_the_app_lacks_stays_unresolved() -> None:
    parsed = parse_script(
        "FOR EACH vEnv IN 'Dev', 'Test'\nSET vPrefix = '$(vEnv)_';\nNEXT vEnv;\n"
        "Orders: LOAD 1 AS id AUTOGENERATE 1;\n"
        "STORE Orders INTO [lib://$(vPrefix)QVD Data/orders.qvd] (qvd);",
    )
    assert not parsed.qvd_stores
    assert parsed.has_unresolved_store


def test_loop_connection_prefix_is_not_an_unset_prefix() -> None:
    parsed = parse_script(
        "Orders: LOAD 1 AS id AUTOGENERATE 1;\n"
        "FOR EACH vPrefix IN 'Dev ', 'Prod '\n"
        "STORE Orders INTO [lib://$(vPrefix)QVD Data/orders.qvd] (qvd);\n"
        "NEXT vPrefix;",
        app_variables={},
    )
    # Unresolved, so the app's QVD writes are not taken as all known.
    assert not parsed.qvd_stores
    assert parsed.has_unresolved_store
    assert parsed.unresolved_qvd_paths == ["lib://$(vPrefix)QVD Data/orders.qvd"]


MODEL_SCRIPT = """
Orders: LOAD id, amount FROM [lib://QVD Data/raw/orders.qvd] (qvd);
Regions: LOAD * INLINE [region
north];
"""
FRONT_SCRIPT = """
Binary [lib://Apps/Sales Model.qvf];
Calendar: LOAD * INLINE [day
1];
"""
ORDERS_MODEL = {"Orders": ["id", "amount"], "Regions": ["region"]}
FRONT_MODEL = {**ORDERS_MODEL, "Calendar": ["day"]}
APP_FILE = _urn("//nas/apps/sales model.qvf")
MAPPED = {"binary_file_to_app": {"lib://Apps/sales model.qvf": "model-app"}}


def _written_upstreams(aspects: Dict[str, Dict[str, Any]], urn: str) -> List[str]:
    lineage = aspects.get(urn, {}).get("UpstreamLineageClass")
    return [upstream.dataset for upstream in lineage.upstreams] if lineage else []


@pytest.mark.parametrize("loader_first", [True, False])
def test_binary_load_mapped_to_an_app_links_its_tables_and_columns(
    loader_first: bool,
) -> None:
    model = _app("model-app", MODEL_SCRIPT, ORDERS_MODEL)
    front = _app("front-app", FRONT_SCRIPT, FRONT_MODEL)
    source_run, aspects = _run(
        [front, model] if loader_first else [model, front], on_prem=MAPPED
    )

    orders = _urn("front-app.orders")
    source = _urn("model-app.orders")
    lineage = aspects[orders]["UpstreamLineageClass"]
    assert [u.dataset for u in lineage.upstreams] == [source]
    assert {fg.downstreams[0]: fg.upstreams for fg in lineage.fineGrainedLineages} == {
        make_schema_field_urn(orders, field): [make_schema_field_urn(source, field)]
        for field in ["id", "amount"]
    }
    # The model app ingests no Regions table to link to.
    regions = aspects[_urn("front-app.regions")]
    assert "lineage_ops" not in regions
    assert "UpstreamLineageClass" not in regions
    [entry] = source_run.reporter.binary_loads_without_lineage
    assert entry.startswith("front-app:")
    # The script creates Calendar itself, and the file is not linked.
    assert "lineage_ops" not in aspects.get(_urn("front-app.calendar"), {})
    assert APP_FILE not in aspects


def test_binary_load_reports_a_source_model_table_its_app_does_not_ingest() -> None:
    model_script = MODEL_SCRIPT + "Totals: LOAD id RESIDENT Orders;"
    model = _app("model-app", model_script, {**ORDERS_MODEL, "Totals": ["id"]})
    front = _app("front-app", FRONT_SCRIPT, {**FRONT_MODEL, "Totals": ["id"]})
    source, aspects = _run([model, front], on_prem=MAPPED)

    assert "UpstreamLineageClass" not in aspects[_urn("front-app.totals")]
    entries = [
        entry
        for entry in source.reporter.binary_loads_without_lineage
        if "Totals" in entry
    ]
    # Not a mapping problem: the model app builds Totals with RESIDENT.
    assert len(entries) == 1
    assert "RESIDENT" in entries[0]


def test_binary_load_mapped_to_an_app_writes_a_script_only_table_whole() -> None:
    model = _app("model-app", MODEL_SCRIPT, ORDERS_MODEL)
    front = _app(
        "front-app",
        FRONT_SCRIPT + "Extra: LOAD id FROM [lib://QVD Data/raw/extra.qvd] (qvd);",
        {**FRONT_MODEL, "Extra": ["id"]},
    )
    source, aspects = _run([model, front], on_prem=MAPPED)

    # The model app has no Extra, so it is the script's own table, and an
    # upstream that changes in the script replaces the old one.
    extra = _urn("front-app.extra")
    assert "lineage_ops" not in aspects[extra]
    assert _written_upstreams(aspects, extra) == [_urn("//nas/qvd/raw/extra.qvd")]
    assert not [
        entry
        for entry in source.reporter.binary_loads_without_lineage
        if "Extra" in entry
    ]


@pytest.mark.parametrize(
    "extra_script, script_table",
    [
        pytest.param(
            "Notes: LOAD * INLINE [note\nx];\nRENAME TABLE Notes TO Remarks;",
            "Remarks",
            id="a renamed script table",
        ),
        pytest.param(
            "Calendar: LOAD * INLINE [week\n1];",
            "Calendar-1",
            id="a script table Qlik renamed as its name was taken",
        ),
    ],
)
def test_binary_load_does_not_link_a_script_table_qlik_holds_under_another_name(
    extra_script: str, script_table: str
) -> None:
    front = _app(
        "front-app",
        FRONT_SCRIPT + extra_script,
        {**FRONT_MODEL, script_table: ["note"]},
    )
    _, aspects = _run([front])

    assert _written_upstreams(aspects, _urn("front-app.orders")) == [APP_FILE]
    assert _urn(f"front-app.{script_table.lower()}") not in aspects


@pytest.mark.parametrize(
    "extra_script, linked",
    [
        pytest.param(
            "Orders: LOAD id, amount FROM [lib://QVD Data/raw/more.qvd] (qvd);",
            True,
            id="a load that adds to the table",
        ),
        pytest.param(
            "DROP TABLE [Orders];\n"
            "Orders: LOAD id, amount FROM [lib://QVD Data/raw/more.qvd] (qvd);",
            False,
            id="a table dropped and loaded again",
        ),
        pytest.param(
            "Orders: Replace LOAD id, amount FROM [lib://QVD Data/raw/more.qvd] (qvd);",
            False,
            id="a Replace load",
        ),
        pytest.param(
            "Orders: Replace LOAD id, amount;\n"
            "LOAD id, amount FROM [lib://QVD Data/raw/more.qvd] (qvd);",
            False,
            id="a Replace preceding load",
        ),
        pytest.param(
            "Replace Concatenate (Orders) LOAD id, amount "
            "FROM [lib://QVD Data/raw/more.qvd] (qvd);",
            False,
            id="a Replace load that adds to the table",
        ),
        pytest.param(
            "Concatenate (Orders) LOAD id, amount "
            "FROM [lib://QVD Data/raw/more.qvd] (qvd);",
            True,
            id="a load that adds to the table by name",
        ),
    ],
)
def test_binary_load_mapped_to_an_app_keeps_a_table_the_script_loads_into(
    extra_script: str, linked: bool
) -> None:
    model = _app("model-app", MODEL_SCRIPT, ORDERS_MODEL)
    front = _app("front-app", FRONT_SCRIPT + extra_script, FRONT_MODEL)
    _, aspects = _run([model, front], on_prem=MAPPED)

    orders = _urn("front-app.orders")
    upstreams = _written_upstreams(aspects, orders)
    assert (_urn("model-app.orders") in upstreams) is linked
    # The script's own load stays in the lineage written with the link.
    assert _urn("//nas/qvd/raw/more.qvd") in upstreams


@pytest.mark.parametrize(
    "front_script",
    [
        pytest.param(
            FRONT_SCRIPT
            + "Replace LOAD id, amount FROM [lib://QVD Data/raw/orders.qvd] (qvd);",
            id="an unlabelled Replace load",
        ),
        pytest.param(
            FRONT_SCRIPT
            + "$(vTable): Replace LOAD id, amount FROM [lib://QVD Data/raw/more.qvd] (qvd);",
            id="a Replace load labelled with a variable",
        ),
        pytest.param(
            FRONT_SCRIPT + "DROP TABLE $(vTable);\n"
            "Orders: LOAD id, amount FROM [lib://QVD Data/raw/more.qvd] (qvd);",
            id="a DROP of a variable",
        ),
        pytest.param(
            FRONT_SCRIPT + "DROP TABLE Orders;\nRENAME TABLE Calendar TO $(vTable);",
            id="a rename to a variable that may name a dropped table",
        ),
        pytest.param(
            "Binary [lib://Apps/Sales Model.qvf];\n"
            "LIB CONNECT TO 'db';\n"
            "Replace Concatenate SQL SELECT id, amount FROM db.orders;",
            id="a Replace Concatenate SELECT with no table to add to",
        ),
    ],
)
def test_binary_load_mapped_to_an_app_whose_script_drops_unnamed_tables_gets_no_lineage(
    front_script: str,
) -> None:
    model = _app("model-app", MODEL_SCRIPT, ORDERS_MODEL)
    front = _app("front-app", front_script, FRONT_MODEL)
    source, aspects = _run([model, front], on_prem=MAPPED)

    assert _urn("model-app.orders") not in _written_upstreams(
        aspects, _urn("front-app.orders")
    )
    [entry] = source.reporter.binary_loads_without_lineage
    assert entry.startswith("front-app:")


@pytest.mark.parametrize(
    "extra_script",
    [
        pytest.param("$(Include=lib://Scripts/extra.qvs);", id="an include"),
        pytest.param("CALL LoadExtra;", id="a SUB the script does not define"),
    ],
)
def test_binary_load_mapped_to_an_app_whose_script_includes_others_is_only_added_to(
    extra_script: str,
) -> None:
    model = _app("model-app", MODEL_SCRIPT, ORDERS_MODEL)
    front = _app("front-app", FRONT_SCRIPT + extra_script, FRONT_MODEL)
    source_run, aspects = _run([model, front], on_prem=MAPPED)

    orders = _urn("front-app.orders")
    source = _urn("model-app.orders")
    assert "UpstreamLineageClass" not in aspects[orders]
    assert _upstreams(aspects, orders, "add") == [source]
    assert not _upstreams(aspects, orders, "remove")
    assert _added_columns(aspects, orders) == {
        make_schema_field_urn(orders, field): [make_schema_field_urn(source, field)]
        for field in ["id", "amount"]
    }
    assert any(
        w.title == "Qlik BINARY tables may be changed by an included script"
        and "front-app" in (w.context or [])
        for w in source_run.reporter.warnings
    )


def test_binary_load_with_an_include_only_adds_to_a_script_only_table() -> None:
    model = _app("model-app", MODEL_SCRIPT, ORDERS_MODEL)
    front = _app(
        "front-app",
        FRONT_SCRIPT
        + "Extra: LOAD id FROM [lib://QVD Data/raw/extra.qvd] (qvd);\n"
        + "$(Include=lib://Scripts/extra.qvs);",
        {**FRONT_MODEL, "Extra": ["id"]},
    )
    _, aspects = _run([model, front], on_prem=MAPPED)

    # The included script may load into Extra, so earlier edges stay.
    extra = _urn("front-app.extra")
    assert "UpstreamLineageClass" not in aspects[extra]
    assert _upstreams(aspects, extra, "add") == [_urn("//nas/qvd/raw/extra.qvd")]
    assert not _upstreams(aspects, extra, "remove")


SUFFIXED_MODEL_SCRIPT = (
    MODEL_SCRIPT + "[Calendar-2024]: LOAD day FROM [lib://QVD Data/raw/cal.qvd] (qvd);"
)


@pytest.mark.parametrize(
    "model_tables, linked",
    [
        pytest.param({"Calendar-2024": ["day"]}, True, id="source app has the table"),
        pytest.param({}, False, id="source app lacks the table"),
    ],
)
def test_binary_load_links_a_suffixed_table_only_when_the_source_app_has_it(
    model_tables: Dict[str, List[str]], linked: bool
) -> None:
    model = _app("model-app", SUFFIXED_MODEL_SCRIPT, {**ORDERS_MODEL, **model_tables})
    front = _app("front-app", FRONT_SCRIPT, {**FRONT_MODEL, "Calendar-2024": ["day"]})
    source, aspects = _run([model, front], on_prem=MAPPED)

    calendar = _urn("front-app.calendar-2024")
    source_table = _urn("model-app.calendar-2024")
    # Otherwise it is the script's own table, whose lineage is written whole,
    # so a COPY edge from the source app an earlier run wrote is dropped.
    assert "UpstreamLineageClass" in aspects[calendar]
    assert _written_upstreams(aspects, calendar) == ([source_table] if linked else [])
    reported = [
        entry
        for entry in source.reporter.binary_loads_without_lineage
        if "Calendar-2024" in entry
    ]
    assert bool(reported) is not linked


@pytest.mark.parametrize(
    "model_tables, written_whole",
    [
        pytest.param(ORDERS_MODEL, True, id="source app lacks the table"),
        pytest.param({}, False, id="source app's data model unknown"),
    ],
)
def test_binary_load_keeps_script_lineage_of_a_table_qlik_may_have_renamed(
    model_tables: Dict[str, List[str]], written_whole: bool
) -> None:
    model = _app("model-app", MODEL_SCRIPT, model_tables)
    front = _app(
        "front-app",
        FRONT_SCRIPT
        + "[Calendar-2024]: LOAD day FROM [lib://QVD Data/raw/cal.qvd] (qvd);",
        {**FRONT_MODEL, "Calendar-2024": ["day"]},
    )
    _, aspects = _run([model, front], on_prem=MAPPED)

    calendar = _urn("front-app.calendar-2024")
    cal_qvd = _urn("//nas/qvd/raw/cal.qvd")
    if written_whole:
        assert _written_upstreams(aspects, calendar) == [cal_qvd]
    else:
        assert "UpstreamLineageClass" not in aspects[calendar]
        assert _upstreams(aspects, calendar, "add") == [cal_qvd]
    assert _urn("model-app.calendar-2024") not in str(aspects[calendar])


def test_unread_source_app_keeps_script_lineage_of_a_table_qlik_may_have_renamed() -> (
    None
):
    front = _app(
        "front-app",
        FRONT_SCRIPT
        + "[Calendar-2024]: LOAD day FROM [lib://QVD Data/raw/cal.qvd] (qvd);",
        {**FRONT_MODEL, "Calendar-2024": ["day"]},
    )
    source, aspects = _run([front], on_prem=MAPPED)

    calendar = _urn("front-app.calendar-2024")
    assert "UpstreamLineageClass" not in aspects[calendar]
    assert _upstreams(aspects, calendar, "add") == [_urn("//nas/qvd/raw/cal.qvd")]
    assert not _upstreams(aspects, calendar, "remove")
    assert any(
        "model-app" in entry for entry in source.reporter.binary_loads_without_lineage
    )


def test_unmapped_binary_load_reports_a_table_qlik_may_have_renamed() -> None:
    front = _app(
        "front-app",
        FRONT_SCRIPT + "Calendar: LOAD * INLINE [week\n1];",
        {**FRONT_MODEL, "Calendar-1": ["week"]},
    )
    source, aspects = _run([front])

    assert _urn("front-app.calendar-1") not in aspects
    assert _written_upstreams(aspects, _urn("front-app.orders")) == [APP_FILE]
    [entry] = source.reporter.binary_loads_without_lineage
    assert "Calendar-1" in entry


def test_binary_load_links_its_tables_to_the_app_file() -> None:
    source, aspects = _run([_app("front-app", FRONT_SCRIPT, FRONT_MODEL)])

    orders = _urn("front-app.orders")
    assert [u.dataset for u in aspects[orders]["UpstreamLineageClass"].upstreams] == [
        APP_FILE
    ]
    assert aspects[APP_FILE]["SubTypesClass"].typeNames == ["Qlik App File"]
    assert aspects[APP_FILE]["DatasetPropertiesClass"].name == "sales model.qvf"
    assert _urn("front-app.calendar") not in aspects
    assert source.reporter.binary_loads_found == 1
    assert not source.reporter.binary_loads_without_lineage


@pytest.mark.parametrize(
    "script, data_model, on_prem",
    [
        pytest.param(
            FRONT_SCRIPT + "LOAD * INLINE [note\nx];",
            FRONT_MODEL,
            None,
            id="a table Qlik names",
        ),
        pytest.param(
            FRONT_SCRIPT + "[$(vTable)]: LOAD * INLINE [note\nx];",
            FRONT_MODEL,
            None,
            id="a bracketed variable label",
        ),
        pytest.param(
            FRONT_SCRIPT + "$(vTable): LOAD * INLINE [note\nx];",
            FRONT_MODEL,
            None,
            id="a variable label",
        ),
        pytest.param(
            FRONT_SCRIPT + "$(Include=lib://Scripts/extra.qvs);",
            FRONT_MODEL,
            None,
            id="an include",
        ),
        pytest.param(
            "Binary [lib://Unknown/Sales Model.qvf];",
            FRONT_MODEL,
            None,
            id="an unknown data connection",
        ),
        pytest.param(
            FRONT_SCRIPT,
            FRONT_MODEL,
            {"binary_file_to_app": {"lib://Apps/sales model.qvf": "front-app"}},
            id="mapped to the app itself",
        ),
        pytest.param(
            FRONT_SCRIPT + "Attributes: Generic LOAD *;\nSQL SELECT * FROM db.t;",
            FRONT_MODEL,
            None,
            id="a Generic preceding load",
        ),
        pytest.param(FRONT_SCRIPT, {}, None, id="no data model"),
        pytest.param("Binary sales.qvf;", FRONT_MODEL, None, id="a relative path"),
    ],
)
def test_binary_load_whose_tables_are_not_known_gets_no_lineage(
    script: str,
    data_model: Dict[str, List[str]],
    on_prem: Optional[Dict[str, Any]],
) -> None:
    source, aspects = _run([_app("front-app", script, data_model)], on_prem=on_prem)

    assert _urn("front-app.orders") not in aspects
    assert APP_FILE not in aspects
    assert len(source.reporter.binary_loads_without_lineage) == 1


def test_binary_load_from_a_storage_connection_links_to_the_stored_file() -> None:
    _, aspects = _run(
        [_app("front-app", FRONT_SCRIPT, FRONT_MODEL)],
        file_connection_to_storage={
            "Apps": {"storage_uri": "abfss://lake@account.dfs.core.windows.net/apps"}
        },
    )

    assert _written_upstreams(aspects, _urn("front-app.orders")) == [
        make_dataset_urn("abs", "lake/apps/Sales Model.qvf")
    ]
    assert APP_FILE not in aspects


def test_binary_load_from_an_app_the_run_does_not_read_is_reported() -> None:
    script = (
        FRONT_SCRIPT
        + "Orders: LOAD id, amount FROM [lib://QVD Data/raw/more.qvd] (qvd);"
    )
    source, aspects = _run(
        [_app("front-app", script, FRONT_MODEL)],
        on_prem={"binary_file_to_app": {"//nas/apps/sales model.qvf": "other-app"}},
    )

    # The script's own upstream is only added, so the edge an earlier run
    # wrote to the source app stays.
    orders = _urn("front-app.orders")
    assert "UpstreamLineageClass" not in aspects[orders]
    assert _upstreams(aspects, orders, "add") == [_urn("//nas/qvd/raw/more.qvd")]
    assert not _upstreams(aspects, orders, "remove")
    [entry] = source.reporter.binary_loads_without_lineage
    assert entry.startswith("front-app:")
    assert "other-app" in entry


@pytest.mark.parametrize(
    "statement, file",
    [
        (
            "Binary [lib://Apps/Sales Model.qvf]",
            QvdReference(connection="Apps", path="Sales Model.qvf"),
        ),
        ("BINARY c:\\qv\\customer.qvw", QvdReference(path="c:\\qv\\customer.qvw")),
        # Relative to the working directory, so no known file.
        ("Binary customer.qvf", None),
    ],
)
def test_binary_statement_names_the_app_file(
    statement: str, file: Optional[QvdReference]
) -> None:
    binary = parse_script(f"{statement};").binary
    assert binary is not None
    assert binary.file == file
    assert not binary.uses_variables


@pytest.mark.parametrize(
    "script, labels, unnamed",
    [
        pytest.param(
            """
Calendar: LOAD * INLINE [day
1];
Concatenate (Orders) LOAD * FROM [lib://QVD Data/more.qvd] (qvd);
Map: MAPPING LOAD a, b RESIDENT Calendar;
Extra:
LOAD a;
SQL SELECT a FROM db.t;
""",
            ["Calendar", "Extra"],
            False,
            # Concatenate adds to a table, and a mapping table is dropped.
            id="labelled loads",
        ),
        pytest.param(
            "Attributes: Generic LOAD * RESIDENT T;", [], True, id="a Generic load"
        ),
        pytest.param(
            "Attributes: Generic LOAD *;\nSQL SELECT * FROM db.t;",
            [],
            True,
            id="a Generic preceding load",
        ),
        pytest.param(
            "[$(vTable)]: LOAD * INLINE [a\n1];",
            [],
            True,
            id="a bracketed variable label",
        ),
        pytest.param(
            "$(vTable): LOAD * INLINE [a\n1];", [], True, id="a variable label"
        ),
    ],
)
def test_tables_the_script_creates_are_known(
    script: str, labels: List[str], unnamed: bool
) -> None:
    parsed = parse_script(script)
    assert parsed.table_labels == labels
    assert parsed.has_unnamed_tables is unnamed


@pytest.mark.parametrize(
    "script",
    [
        pytest.param(
            "Orders: Replace LOAD * FROM [lib://QVD Data/orders.qvd] (qvd);",
            id="on the load",
        ),
        pytest.param(
            "Orders: Replace LOAD id;\nLOAD id;\nSQL SELECT id FROM db.orders;",
            id="on the top preceding load",
        ),
    ],
)
def test_replace_load_drops_and_creates_its_table(script: str) -> None:
    parsed = parse_script(script)
    assert parsed.table_labels == ["Orders"]
    assert parsed.dropped_tables == ["Orders"]


def test_table_named_by_an_unknown_bracketed_variable_has_no_name() -> None:
    parsed = parse_script("LIB CONNECT TO 'db';\n[$(vT)]: SQL SELECT a FROM s.t;")
    # Not a table literally named $(vT), which would be a dataset of its own.
    assert [table.label_unknown for table in parsed.tables] == [True]
    assert parsed.has_unnamed_tables


@pytest.mark.parametrize(
    "script, labels",
    [
        pytest.param(
            "$(vT): SQL SELECT a FROM s.t;", ["s.u", "table_2"], id="on a SELECT"
        ),
        pytest.param(
            "$(vT): LOAD a;\nSQL SELECT a FROM s.t;",
            ["s.t", "s.u", "table_3"],
            id="on a preceding LOAD",
        ),
    ],
)
def test_bare_variable_label_keeps_the_table_names(
    script: str, labels: List[str]
) -> None:
    parsed = parse_script(
        f"LIB CONNECT TO 'db';\n{script}\nSQL SELECT b FROM s.u;\nSQL SELECT 1 AS c;"
    )
    # As before such labels were read, so later unlabelled tables keep their
    # table_<n> names, and so their URNs.
    assert [table.label for table in parsed.tables] == labels
    assert parsed.has_unnamed_tables


def test_tables_the_script_drops_are_known() -> None:
    parsed = parse_script("DROP TABLES [Order Lines], 'Regions', Calendar;")
    assert parsed.dropped_tables == ["Order Lines", "Regions", "Calendar"]
    assert not parsed.drops_unnamed_tables


@pytest.mark.parametrize(
    "script",
    [
        pytest.param("Replace LOAD * INLINE [a\n1];", id="an unlabelled Replace load"),
        pytest.param("$(vT): Replace LOAD * INLINE [a\n1];", id="a variable label"),
        pytest.param(
            "$(vT): Replace LOAD id;\nSQL SELECT id FROM db.orders;",
            id="a Replace preceding load labelled with a variable",
        ),
        pytest.param(
            "Replace LOAD id;\n$(vT): SQL SELECT id FROM db.orders;",
            id="a Replace preceding load above a variable-labelled SELECT",
        ),
        pytest.param(
            "Replace LOAD id;\n$(vT): LOAD id FROM [lib://Data/orders.qvd] (qvd);",
            id="a Replace preceding load above a variable-labelled file load",
        ),
        pytest.param("DROP TABLES Regions, [$(vT)];", id="a variable DROP"),
        pytest.param("RENAME TABLE Orders TO $(vT);", id="a variable RENAME target"),
        pytest.param(
            "Binary [lib://Apps/Sales Model.qvf];\n"
            "Replace Concatenate SQL SELECT id FROM db.orders;",
            id="a Replace Concatenate SELECT with no table to add to",
        ),
        pytest.param(
            "Binary [lib://Apps/Sales Model.qvf];\n"
            "Replace Concatenate LOAD id;\nSQL SELECT id FROM db.orders;",
            id="a Replace Concatenate preceding load with no table to add to",
        ),
    ],
)
def test_tables_the_script_drops_without_a_name_are_flagged(script: str) -> None:
    parsed = parse_script(script)
    assert parsed.drops_unnamed_tables
    assert all("$(" not in name for name in parsed.dropped_tables)
