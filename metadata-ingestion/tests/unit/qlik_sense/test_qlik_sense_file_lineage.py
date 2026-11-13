import json
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    LineageTable,
    ListedDataset,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.file_lineage import (
    FileStorage,
    file_storage_of_connection,
    file_storage_of_uri,
    qri_part_id,
)
from datahub.ingestion.source.qlik_sense.qlik_base_api import UNREAD_DATA_CONNECTIONS
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import (
    DataConnectionInfo,
    FileLoad,
    parse_script,
)
from datahub.metadata.schema_classes import (
    MetadataChangeProposalClass,
    UpstreamLineageClass,
)

CLOUD_CONFIG = {"tenant_hostname": "tenant.example.com", "api_key": "key"}
CONNECTION = "Sales Space:Lake"

SCRIPT = """
LET vPrefix = '';
LET vLake = 'lib://$(vPrefix)Sales Space:Lake/';
Orders:
LOAD * FROM [$(vLake)/landing/orders.csv]
(txt, utf8, embedded labels, delimiter is ',');
Regions:
LOAD * FROM [lib://Sales Space:Lake/ref/regions.xlsx] (ooxml, embedded labels);
Targets:
LOAD * FROM [lib://Lake/ref/targets.csv] (txt);
"""


def _file_qri(scheme: str, connection_part: str, path: str) -> str:
    # As Qlik Cloud's lineage graph names a file: each part hashed.
    return (
        f"qri:file:{scheme}://{qri_part_id(connection_part)}"
        f"#{qri_part_id(path)}#{qri_part_id('item')}"
    )


def _source(
    file_connection_to_storage: Dict, connections: Dict[str, DataConnectionInfo]
) -> QlikSenseSource:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(
            {
                **CLOUD_CONFIG,
                "file_connection_to_storage": file_connection_to_storage,
            }
        ),
        PipelineContext(run_id="test-file-lineage"),
    )
    source._data_connections = connections
    source.qlik_api.spaces = {"space-1": "Sales Space"}
    app = App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "description": "",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            "script": SCRIPT,
            # The Engine holds LET variables as the last reload set them.
            "variables": {
                "vPrefix": "",
                "vLake": "lib://Sales Space:Lake/",
            },
        }
    )
    source._file_sources_by_app[app.id] = source._file_source_resolver(
        app, parse_script(SCRIPT)
    )
    source._app_space_ids[app.id] = app.spaceId
    return source


def test_script_lists_every_file_a_load_reads() -> None:
    # Paths as the script's own variables expand them, and as written, for
    # the app's variables from its last reload to expand.
    assert parse_script(SCRIPT).file_loads == [
        FileLoad(
            expanded="lib://Sales Space:Lake//landing/orders.csv",
            as_written="$(vLake)/landing/orders.csv",
        ),
        FileLoad(
            expanded="lib://Sales Space:Lake/ref/regions.xlsx",
            as_written="lib://Sales Space:Lake/ref/regions.xlsx",
        ),
        FileLoad(
            expanded="lib://Lake/ref/targets.csv",
            as_written="lib://Lake/ref/targets.csv",
        ),
    ]


def test_mapping_and_added_loads_list_their_files() -> None:
    parsed = parse_script(
        "Map: MAPPING LOAD a, b FROM [lib://Lake/map.csv] (txt);\n"
        "Orders: LOAD * FROM [lib://Lake/orders.csv] (txt);\n"
        "Concatenate (Orders) LOAD * FROM [lib://Lake/more.csv] (txt);\n"
    )
    assert [load.expanded for load in parsed.file_loads] == [
        "lib://Lake/map.csv",
        "lib://Lake/orders.csv",
        "lib://Lake/more.csv",
    ]
    assert not parsed.has_includes


@pytest.mark.parametrize(
    "fields",
    ["From AS Start, To AS End", "From & '-' & To AS Range, id"],
)
def test_field_named_from_is_not_the_file(fields: str) -> None:
    parsed = parse_script(
        f"Ranges: LOAD {fields} FROM [lib://Lake/ranges.csv] (txt) WHERE id > 1;\n"
        f"Orders: LOAD {fields} FROM [lib://QVD Data/orders.qvd] (qvd);\n"
    )
    assert [load.expanded for load in parsed.file_loads] == [
        "lib://Lake/ranges.csv",
        "lib://QVD Data/orders.qvd",
    ]
    assert [load.label for load in parsed.qvd_loads] == ["Orders"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_path_resolves_with_the_variables_of_the_last_reload(
    mock_api_cls: MagicMock,
) -> None:
    source = _source(
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        {},
    )
    # The script was edited after its last reload, which Qlik's graph reflects.
    script = (
        "SET vDir = 'lib://Sales Space:Lake/new';\nX: LOAD * FROM [$(vDir)/f.csv];\n"
    )
    app = App.model_validate(
        {
            "id": "app-4",
            "qTitle": "Sales",
            "description": "",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            "variables": {"vDir": "lib://Sales Space:Lake/old"},
        }
    )
    source._file_sources_by_app[app.id] = source._file_source_resolver(
        app, parse_script(script)
    )
    source._app_space_ids[app.id] = app.spaceId

    assert (
        source._resolve_file_source("app-4", _file_qri("adls", "unknown", "old/f.csv"))
        == "urn:li:dataset:(urn:li:dataPlatform:abs,lake/old/f.csv,PROD)"
    )


@pytest.mark.parametrize(
    "statement, expected",
    [
        (
            'CUSTOM CONNECT TO "provider=QvWebStorageProviderConnectorPackage.exe;'
            'sourceType=File_AzureStorageConnector;blobContainerPath=lake/sales;"',
            FileStorage(
                platform="abs",
                location="lake/sales",
                qri_connection_id=qri_part_id("lake/sales"),
            ),
        ),
        (
            'CUSTOM CONNECT TO "provider=QvWebStorageProviderConnectorPackage.exe;'
            'sourceType=File_AmazonS3ConnectorV2;region=eu-west-1;bucketName=sales;"',
            FileStorage(
                platform="s3",
                location="sales",
                qri_connection_id=qri_part_id("eu-west-1/sales"),
            ),
        ),
        # A connection that does not say where its files are.
        (
            'CUSTOM CONNECT TO "provider=QvWebStorageProviderConnectorPackage.exe;'
            'sourceType=File_GoogleCloudStorageConnector;"',
            None,
        ),
        ('CUSTOM CONNECT TO "provider=qix-datafiles.exe;path=mydatafiles;"', None),
    ],
)
def test_file_storage_of_connection(
    statement: str, expected: Optional[FileStorage]
) -> None:
    assert file_storage_of_connection(statement) == expected


@pytest.mark.parametrize(
    "uri, expected",
    [
        (
            "abfss://lake@account.dfs.core.windows.net/sales/",
            FileStorage(platform="abs", location="lake/sales"),
        ),
        ("s3://sales-bucket", FileStorage(platform="s3", location="sales-bucket")),
        ("gs://sales/raw", FileStorage(platform="gcs", location="sales/raw")),
        (
            "S3://sales-bucket/raw",
            FileStorage(platform="s3", location="sales-bucket/raw"),
        ),
        ("https://example.com/sales", None),
        # An Azure URI with no container.
        ("abfss://@account.dfs.core.windows.net/sales", None),
        ("https://account.blob.core.windows.net/", None),
    ],
)
def test_file_storage_of_uri(uri: str, expected: Optional[FileStorage]) -> None:
    storage = file_storage_of_uri(uri)
    if storage is not None:
        # Only used to tell candidates apart; covered by the branch test.
        storage.likely_qri_connection_id = None
    assert storage == expected


def test_unsupported_storage_uri_is_rejected() -> None:
    with pytest.raises(ValidationError):
        QlikSourceConfig.model_validate(
            {
                **CLOUD_CONFIG,
                "file_connection_to_storage": {
                    CONNECTION: {"storage_uri": "ftp://files"}
                },
            }
        )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_resolves_through_the_mapped_storage(mock_api_cls: MagicMock) -> None:
    source = _source(
        {
            CONNECTION: {
                "storage_uri": "abfss://lake@account.dfs.core.windows.net/sales",
                "platform_instance": "lake_account",
            }
        },
        {},
    )

    # The path is built from variables with a doubled separator, which Qlik's
    # QRI does not have.
    urn = source._resolve_file_source(
        "app-1", _file_qri("adls", "unknown", "landing/orders.csv")
    )

    assert urn == (
        "urn:li:dataset:(urn:li:dataPlatform:abs,"
        "lake_account.lake/sales/landing/orders.csv,PROD)"
    )
    assert source.reporter.file_sources_resolved == 1


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_resolves_through_the_visible_connection(
    mock_api_cls: MagicMock,
) -> None:
    storage = FileStorage(
        platform="abs",
        location="lake",
        qri_connection_id=qri_part_id("lake"),
    )
    source = _source(
        {},
        {CONNECTION: DataConnectionInfo(name="Lake", file_storage=storage)},
    )

    assert (
        source._resolve_file_source(
            "app-1", _file_qri("adls", "lake", "ref/regions.xlsx")
        )
        == "urn:li:dataset:(urn:li:dataPlatform:abs,lake/ref/regions.xlsx,PROD)"
    )
    # Same path, but the QRI's connection is another container.
    assert (
        source._resolve_file_source(
            "app-1", _file_qri("adls", "other", "ref/regions.xlsx")
        )
        is None
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_without_known_storage_is_reported(mock_api_cls: MagicMock) -> None:
    source = _source({}, {})

    assert (
        source._resolve_file_source(
            "app-1", _file_qri("adls", "lake", "ref/regions.xlsx")
        )
        is None
    )
    # A file the script does not read is not the connection's to report.
    assert (
        source._resolve_file_source("app-1", _file_qri("adls", "lake", "other.csv"))
        is None
    )
    assert list(source.reporter.unresolved_file_connections) == [CONNECTION]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_bare_connection_name_is_the_app_spaces(mock_api_cls: MagicMock) -> None:
    source = _source(
        {CONNECTION: {"storage_uri": "s3://sales-bucket"}},
        {},
    )

    assert (
        source._resolve_file_source(
            "app-1", _file_qri("s3", "unknown", "ref/targets.csv")
        )
        == "urn:li:dataset:(urn:li:dataPlatform:s3,sales-bucket/ref/targets.csv,PROD)"
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_mapping_without_env_uses_the_recipe_env(
    mock_api_cls: MagicMock,
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(
            {
                **CLOUD_CONFIG,
                "env": "DEV",
                "file_connection_to_storage": {
                    CONNECTION: {"storage_uri": "gs://sales"}
                },
            }
        ),
        PipelineContext(run_id="test-file-lineage"),
    )
    source._data_connections = {}
    source.qlik_api.spaces = {"space-1": "Sales Space"}
    source._file_sources_by_app["app-1"] = source._file_source_resolver(
        App.model_validate(
            {
                "id": "app-1",
                "qTitle": "Sales",
                "description": "",
                "createdDate": "2024-01-09T18:05:36.000Z",
                "modifiedDate": "2024-01-09T18:05:36.000Z",
                "spaceId": "space-1",
            }
        ),
        parse_script(SCRIPT),
    )
    source._app_space_ids["app-1"] = "space-1"

    assert (
        source._resolve_file_source(
            "app-1", _file_qri("gcs", "sales", "ref/regions.xlsx")
        )
        == "urn:li:dataset:(urn:li:dataPlatform:gcs,sales/ref/regions.xlsx,DEV)"
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_mapped_storage_is_not_trusted_for_another_scheme_or_shared_path(
    mock_api_cls: MagicMock,
) -> None:
    source = _source(
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        {},
    )
    # An S3 file with the same path is not on the mapped Azure container.
    assert (
        source._resolve_file_source("app-1", _file_qri("s3", "eu/b", "ref/targets.csv"))
        is None
    )

    script = (
        SCRIPT + "Other:\nLOAD * FROM [lib://Other Lake/ref/regions.xlsx] (ooxml);\n"
    )
    app = App.model_validate(
        {
            "id": "app-2",
            "qTitle": "Sales",
            "description": "",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
        }
    )
    source._file_sources_by_app[app.id] = source._file_source_resolver(
        app, parse_script(script)
    )
    source._app_space_ids[app.id] = app.spaceId
    # Another connection the script reads the same path through, whose storage
    # is not known, could hold the file as well.
    assert (
        source._resolve_file_source(
            "app-2", _file_qri("adls", "unknown", "ref/regions.xlsx")
        )
        is None
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_prod_file_is_not_linked_when_dev_reads_the_same_path(
    mock_api_cls: MagicMock,
) -> None:
    source = _source({"Prod": {"storage_uri": "s3://prod-bucket"}}, {})
    source._file_sources_by_app["app-1"] = source._file_source_resolver(
        App.model_validate(
            {
                "id": "app-1",
                "qTitle": "Sales",
                "createdDate": "2024-01-09T18:05:36.000Z",
                "modifiedDate": "2024-01-09T18:05:36.000Z",
                "spaceId": "space-1",
            }
        ),
        parse_script(
            "A: LOAD * FROM [lib://Prod/x.csv] (txt);\n"
            "B: LOAD * FROM [lib://Dev/x.csv] (txt);\n"
        ),
    )

    assert (
        source._resolve_file_source("app-1", _file_qri("s3", "eu/dev", "x.csv")) is None
    )


@pytest.mark.parametrize(
    "connection_part, expected",
    [
        pytest.param("lake/prod", "lake/prod/x.csv", id="matches one storage"),
        pytest.param("lake-prod", None, id="matches neither"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_branch_loads_are_told_apart_by_the_storage_location(
    mock_api_cls: MagicMock, connection_part: str, expected: Optional[str]
) -> None:
    source = _source(
        {
            "Prod": {"storage_uri": "abfss://lake@account.dfs.core.windows.net/prod"},
            "Dev": {"storage_uri": "abfss://lake@account.dfs.core.windows.net/dev"},
        },
        {},
    )
    source._file_sources_by_app["app-1"] = source._file_source_resolver(
        App.model_validate(
            {
                "id": "app-1",
                "qTitle": "Sales",
                "createdDate": "2024-01-09T18:05:36.000Z",
                "modifiedDate": "2024-01-09T18:05:36.000Z",
                "spaceId": "space-1",
            }
        ),
        parse_script(
            "IF vEnv = 'prod' THEN\nA: LOAD * FROM [lib://Prod/x.csv] (txt);\n"
            "ELSE\nA: LOAD * FROM [lib://Dev/x.csv] (txt);\nEND IF;\n"
        ),
    )
    source._app_space_ids["app-1"] = "space-1"

    resolved = source._resolve_file_source(
        "app-1", _file_qri("adls", connection_part, "x.csv")
    )
    # Without a match, either branch may have run, so neither is linked.
    assert resolved == (
        f"urn:li:dataset:(urn:li:dataPlatform:abs,{expected},PROD)"
        if expected
        else None
    )


@pytest.mark.parametrize(
    "script",
    [
        pytest.param(
            "FOR EACH vFile IN FileList('lib://Lake/m/*.csv')\n"
            "X: LOAD * FROM [$(vFile)] (txt);\nNEXT vFile;",
            id="loop variable",
        ),
        pytest.param(
            "FOR EACH vFile IN FileList('lib://Lake/m/*.csv')\n"
            "SET vPath = $(vFile);\nX: LOAD * FROM [$(vPath)] (txt);\nNEXT vFile;",
            id="copy of a loop variable",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_list_loop_load_is_not_named_by_the_last_value(
    mock_api_cls: MagicMock, script: str
) -> None:
    source = _source({}, {})
    app = App.model_validate(
        {
            "id": "app-7",
            "qTitle": "Sales",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            "variables": {
                "vFile": "lib://Lake/m/last.csv",
                "vPath": "lib://Lake/m/last.csv",
            },
        }
    )
    parsed = parse_script(script, app.variables)
    [load] = parsed.file_loads
    assert load.uses_unknown_variable
    # Unnamed, so the files the graph names are only added to the lineage.
    source._file_source_resolver(app, parsed)
    assert app.id in source._apps_file_paths_unknown


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_load_with_a_wildcard_spelling_marks_the_app_incomplete(
    mock_api_cls: MagicMock,
) -> None:
    source = _source({}, {})
    app = App.model_validate(
        {
            "id": "app-8",
            "qTitle": "Sales",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            # The last reload read every file of the folder.
            "variables": {"vFile": "*.csv"},
        }
    )
    parsed = parse_script(
        "SET vFile = 'orders.csv';\nX: LOAD * FROM [lib://Lake/$(vFile)] (txt);\n"
    )
    resolver = source._file_source_resolver(app, parsed)

    # The concrete spelling is still a reference, but the app is incomplete.
    assert resolver.candidates(_file_qri("adls", "x", "orders.csv")).references
    assert app.id in source._apps_file_paths_unknown


RATES_QRI = "qri:qdf:space://AbC#Rates"
RATES = make_dataset_urn("qlik-sense", "space-1.rates.qvd")


def _graph_app_run(
    mock_api_cls: MagicMock,
    file_connection_to_storage: Dict,
    unread: List[UnreadItem],
    variables_read_failed: bool = False,
    script: str = SCRIPT,
    datasets_listed: bool = True,
) -> Dict[str, str]:
    """Each table's upstreams as a whole UpstreamLineage write lists them, or
    "patch:" and those a patch adds."""
    api = mock_api_cls.return_value
    api.unread_items = unread
    api.list_datasets.return_value = (
        [ListedDataset(space_id="space-1", name="rates.qvd", secure_qri=RATES_QRI)]
        if datasets_listed
        else None
    )
    api.get_user_name.return_value = None
    api.spaces = {"space-1": "Sales Space"}
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(
            {**CLOUD_CONFIG, "file_connection_to_storage": file_connection_to_storage}
        ),
        PipelineContext(run_id="test-file-lineage"),
    )
    source._allowed_space_ids = {"space-1"}
    source._data_connections = {}
    app = App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            "script": script,
            "variables": {}
            if variables_read_failed
            else {"vLake": "lib://Sales Space:Lake/"},
            "variables_read_failed": variables_read_failed,
            "lineage_tables": [
                LineageTable(
                    qri="qri:app:sense://app-1#Orders",
                    label="Orders",
                    source_qris=[
                        _file_qri("adls", "unknown", "landing/orders.csv"),
                        f"{RATES_QRI}#Rates",
                    ],
                )
            ],
        }
    )
    writes: Dict[str, str] = {}
    for wu in source._gen_app_workunit(app):
        lineage = wu.get_aspect_of_type(UpstreamLineageClass)
        if lineage:
            writes[wu.get_urn()] = ",".join(u.dataset for u in lineage.upstreams)
        elif (
            isinstance(wu.metadata, MetadataChangeProposalClass)
            and wu.metadata.aspectName == UpstreamLineageClass.ASPECT_NAME
            and wu.metadata.aspect is not None
        ):
            writes[wu.get_urn()] = "patch:" + ",".join(
                op["value"]["dataset"]
                for op in json.loads(wu.metadata.aspect.value)
                if op["op"] == "add" and op["path"].startswith("/upstreams/")
            )
    return writes


ORDERS = make_dataset_urn("qlik-sense", "app-1.orders")


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_graph_table_links_to_the_file_its_script_loads(
    mock_api_cls: MagicMock,
) -> None:
    writes = _graph_app_run(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
    )

    assert writes[ORDERS] == (
        f"urn:li:dataset:(urn:li:dataPlatform:abs,lake/landing/orders.csv,PROD),{RATES}"
    )


@pytest.mark.parametrize(
    "listing_failed, variables_failed, written_whole",
    [(True, False, False), (False, True, False), (False, False, True)],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unresolved_file_keeps_lineage_when_its_inputs_were_not_read(
    mock_api_cls: MagicMock,
    listing_failed: bool,
    variables_failed: bool,
    written_whole: bool,
) -> None:
    # Without the connections, or the variables its path is built from, the
    # file may be one the last run linked, so the table's lineage is only
    # added to; with both read, the unresolved file is known to be unlinked.
    writes = _graph_app_run(
        mock_api_cls,
        {},
        [UnreadItem(name=UNREAD_DATA_CONNECTIONS, cause="timeout")]
        if listing_failed
        else [],
        variables_read_failed=variables_failed,
    )

    # The table's resolved source is still added when its file is not.
    assert writes[ORDERS] == (RATES if written_whole else f"patch:{RATES}")


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unresolved_file_of_an_app_with_includes_only_adds(
    mock_api_cls: MagicMock,
) -> None:
    # The file may be loaded by the included script, which is not read.
    writes = _graph_app_run(
        mock_api_cls,
        {},
        [],
        script="$(Must_Include=lib://Lake/common.qvs);\n" + SCRIPT,
    )

    assert writes[ORDERS] == f"patch:{RATES}"


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_path_from_a_quoted_set_variable_resolves(mock_api_cls: MagicMock) -> None:
    source = _source(
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        {},
    )
    app = App.model_validate(
        {
            "id": "app-5",
            "qTitle": "Sales",
            "description": "",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            # SET keeps the quotes in the variable's value.
            "variables": {"vDir": "'lib://Sales Space:Lake/old/'"},
        }
    )
    source._file_sources_by_app[app.id] = source._file_source_resolver(
        app, parse_script("X: LOAD * FROM [$(vDir)f.csv];\n")
    )
    source._app_space_ids[app.id] = app.spaceId

    assert app.id not in source._apps_file_paths_unknown
    assert (
        source._resolve_file_source("app-5", _file_qri("adls", "unknown", "old/f.csv"))
        == "urn:li:dataset:(urn:li:dataPlatform:abs,lake/old/f.csv,PROD)"
    )


@pytest.mark.parametrize(
    "script",
    [
        pytest.param(
            "SUB L(p)\nX: LOAD * FROM [lib://Lake/$(p)] (txt);\nEND SUB;\n"
            "CALL L('a.csv');\n",
            id="sub parameter",
        ),
        pytest.param(
            "SET vDir = 'lib://Lake/d/';\nX: LOAD * FROM [$(vDir)a.csv] (txt);\n"
            "SET vDir = ;\n",
            id="variable cleared at the end",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_load_named_by_the_script_variables_is_not_unnamed(
    mock_api_cls: MagicMock, script: str
) -> None:
    # The Engine's variables can't name the path as written, but the script's
    # own expansion does.
    source = _source({}, {})
    app = App.model_validate(
        {
            "id": "app-6",
            "qTitle": "Sales",
            "description": "",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            "variables": {"vDir": ""},
        }
    )
    source._file_source_resolver(app, parse_script(script))

    assert app.id not in source._apps_file_paths_unknown
    assert not [w for w in source.reporter.warnings if "can't name" in str(w.title)]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unresolved_file_of_an_app_loading_by_wildcard_only_adds(
    mock_api_cls: MagicMock,
) -> None:
    # The wildcard may read the file the graph names.
    writes = _graph_app_run(
        mock_api_cls,
        {},
        [],
        script=SCRIPT + "Daily:\nLOAD * FROM [lib://Lake/daily/*.csv] (txt);\n",
    )

    assert writes[ORDERS] == f"patch:{RATES}"


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unresolved_file_of_an_app_calling_an_undefined_sub_only_adds(
    mock_api_cls: MagicMock,
) -> None:
    # The SUB, such as one from an include, may load the file.
    writes = _graph_app_run(mock_api_cls, {}, [], script=SCRIPT + "CALL LoadRates;\n")

    assert writes[ORDERS] == f"patch:{RATES}"


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_resolves_when_datasets_could_not_be_listed(
    mock_api_cls: MagicMock,
) -> None:
    # The file needs only the script and the mapping; the dataset, which
    # needs the list, keeps the lineage patched.
    writes = _graph_app_run(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        datasets_listed=False,
    )

    assert writes[ORDERS] == (
        "patch:urn:li:dataset:(urn:li:dataPlatform:abs,lake/landing/orders.csv,PROD)"
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_visible_connection_of_another_kind_does_not_block_the_file(
    mock_api_cls: MagicMock,
) -> None:
    source = _source(
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        # Qlik's own data files, which can't be on Azure Storage.
        {"Sales Space:Files": DataConnectionInfo(name="Files")},
    )
    script = SCRIPT + "Copy:\nLOAD * FROM [lib://Sales Space:Files/ref/regions.xlsx];\n"
    app = App.model_validate(
        {
            "id": "app-3",
            "qTitle": "Sales",
            "description": "",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
        }
    )
    source._file_sources_by_app[app.id] = source._file_source_resolver(
        app, parse_script(script)
    )
    source._app_space_ids[app.id] = app.spaceId

    assert (
        source._resolve_file_source(
            "app-3", _file_qri("adls", "unknown", "ref/regions.xlsx")
        )
        == "urn:li:dataset:(urn:li:dataPlatform:abs,lake/ref/regions.xlsx,PROD)"
    )
