import json
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import App, UnreadItem
from datahub.ingestion.source.qlik_sense.qlik_base_api import UNREAD_DATA_CONNECTIONS
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.qvd_lineage import (
    AppQvdLineage,
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
from tests.unit.qlik_sense.qlik_fakes import ONPREM_CONFIG


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
}
RAW = _urn("//nas/qvd/raw/orders.qvd")
CLEAN = _urn("//nas/qvd/clean/orders.qvd")


def _app(app_id: str, script: Optional[str]) -> App:
    return App.model_validate(
        {
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
        api.get_user_name.return_value = None
        api.get_data_connections.return_value = (
            {} if connections_unread else CONNECTIONS
        )
        api.get_items.return_value = apps
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(ONPREM_CONFIG),
            PipelineContext(run_id="test-qvd-lineage", graph=graph),
        )
        aspects: Dict[str, Dict[str, Any]] = {}
        for wu in source.get_workunits_internal():
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

    def folder_urns(lineage: AppQvdLineage) -> List[str]:
        return [
            wu.get_urn()
            for wu in source._gen_qvd_entities(lineage)
            if wu.get_aspect_of_type(ContainerPropertiesClass)
        ]

    first = folder_urns(AppQvdLineage(paths={CLEAN: "//nas/qvd/clean/orders.qvd"}))
    second = folder_urns(AppQvdLineage(paths={RAW: "//nas/qvd/raw/orders.qvd"}))

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
        for wu in source._gen_qvd_entities(AppQvdLineage(paths={_urn(path): path}))
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
            "IF vEnv = 'A' THEN\nLET vTable = Lower('Orders' & vRegion);\nELSE\n"
            "LET vTable = Lower('Hist' & vRegion);\nEND IF;\n"
            "T: LOAD id FROM [lib://QVD Data/$(vTable).qvd] (qvd);",
            id="LET in IF branches",
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
    ],
)
def test_qvd_path_from_a_variable_known_at_that_point_resolves(
    script: str, path: str
) -> None:
    # The app keeps a variable's last value, which is also its value before
    # the script first sets it, as variables persist between reloads.
    parsed = parse_script(script, app_variables={"vTable": "Orders"})
    assert [load.file.path for load in parsed.qvd_loads] == [path]


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
