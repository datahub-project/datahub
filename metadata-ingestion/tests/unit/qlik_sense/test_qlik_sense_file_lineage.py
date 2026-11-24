import json
from typing import Dict, Iterable, List, Optional, Set, Tuple
from unittest.mock import MagicMock, patch

import pytest
import requests_mock as requests_mock_module
from pydantic import ValidationError

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.app_state import AppCheckpoint
from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    LineageTable,
    ListedDataset,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.file_lineage import (
    FilePlatform,
    FileStorage,
    file_storage_of_connection,
    file_storage_of_uri,
    qri_part_id,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI as RealQlikAPI
from datahub.ingestion.source.qlik_sense.qlik_base_api import UNREAD_DATA_CONNECTIONS
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import (
    DataConnectionInfo,
    FileLoad,
    parse_script,
)
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    ContainerClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    MetadataChangeProposalClass,
    UpstreamClass,
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
    file_connection_to_storage: Dict,
    connections: Dict[str, DataConnectionInfo],
    script: str = SCRIPT,
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
            "script": script,
            # The Engine holds LET variables as the last reload set them.
            "variables": {
                "vPrefix": "",
                "vLake": "lib://Sales Space:Lake/",
            },
        }
    )
    source._file_sources_by_app[app.id] = source._file_source_resolver(
        app, parse_script(script)
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
            "sourceType=File_AzureStorageConnector;"
            'blobContainerPath=https://account.blob.core.windows.net/lake;"',
            FileStorage(
                platform="abs",
                location="lake",
                qri_connection_id=qri_part_id(
                    "https://account.blob.core.windows.net/lake"
                ),
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
        # A location on another platform than the connector's is not trusted.
        (
            'CUSTOM CONNECT TO "provider=QvWebStorageProviderConnectorPackage.exe;'
            'sourceType=File_AzureStorageConnector;blobContainerPath=s3://sales;"',
            None,
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

    # A bare name is the app's own space's connection, as Qlik Cloud resolves it.
    assert source._resolve_file_source(
        "app-1", _file_qri("adls", "lake", "ref/targets.csv")
    ) == make_dataset_urn("qlik-sense", "sales space:lake/ref/targets.csv")
    # A file the script does not read is not the connection's to report.
    assert (
        source._resolve_file_source("app-1", _file_qri("adls", "lake", "other.csv"))
        is None
    )
    assert source.reporter.unresolved_file_connections == ["Lake"]


@pytest.mark.parametrize("space_ingested", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_without_known_storage_is_named_by_its_connection(
    mock_api_cls: MagicMock, space_ingested: bool
) -> None:
    source = _source({}, {})
    if space_ingested:
        source._allowed_space_ids = {"space-1"}
    regions = make_dataset_urn("qlik-sense", "sales space:lake/ref/regions.xlsx")

    # Named with its space, the connection is the same in every app, so the
    # apps that write and read the file join on it.
    assert (
        source._resolve_file_source(
            "app-1", _file_qri("adls", "lake", "ref/regions.xlsx")
        )
        == regions
    )
    assert source.reporter.unresolved_file_connections == [CONNECTION]
    entities = list(source._gen_unmapped_file_entities())
    assert {wu.get_urn() for wu in entities} == {regions}
    properties = next(
        p for wu in entities if (p := wu.get_aspect_of_type(DatasetPropertiesClass))
    )
    assert properties.qualifiedName == "lib://Sales Space:Lake/ref/regions.xlsx"
    # In its space, under its connection and folders, rather than at the dots
    # of its URN; a space the run doesn't ingest gets no made-up container.
    space_urn = source._gen_space_key("space-1").as_urn()
    browse_path = next(
        b for wu in entities if (b := wu.get_aspect_of_type(BrowsePathsV2Class))
    )
    assert [(e.id, e.urn) for e in browse_path.path] == [
        (space_urn, space_urn) if space_ingested else ("Sales Space", None),
        ("Lake", None),
        ("ref", None),
    ]
    containers = [
        c.container for wu in entities if (c := wu.get_aspect_of_type(ContainerClass))
    ]
    assert containers == ([space_urn] if space_ingested else [])


@pytest.mark.parametrize(
    "spaces, expected",
    [
        ({"space-1": "SALES SPACE"}, "space-1"),
        # Not told apart by case, so placed in neither.
        ({"space-1": "Sales Space", "space-2": "sales space"}, None),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_lib_path_space_is_matched_regardless_of_case(
    mock_api_cls: MagicMock, spaces: Dict[str, str], expected: Optional[str]
) -> None:
    source = _source({}, {})
    source.qlik_api.spaces = spaces
    source._allowed_space_ids = set(spaces)

    assert source._ingested_space_id("Sales Space") == expected


@pytest.mark.parametrize(
    "connection, path, space_id",
    [
        # A personal space has no name another app could write the path with.
        ("Lake", "ref/regions.xlsx", Constant.PERSONAL_SPACE_ID),
        # Holds Qlik's own datasets.
        ("Sales Space:DataFiles", "rates.qvd", "space-1"),
        ("DataFiles", "rates.qvd", "space-1"),
        # Characters a dataset URN can't hold.
        ("Sales Space:Lake", "ref/a,b.csv", "space-1"),
        ("Sales Space:Lake", "ref/rates (old).csv", "space-1"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_that_cant_be_named_by_its_connection(
    mock_api_cls: MagicMock, connection: str, path: str, space_id: str
) -> None:
    source = _source({}, {})
    source.qlik_api.spaces[Constant.PERSONAL_SPACE_ID] = Constant.PERSONAL_SPACE_NAME

    assert source._unmapped_file_urn(connection, path, space_id) is None


@pytest.mark.parametrize(
    "connection",
    ["Sales Space:Lake", "Lake", "SALES SPACE:lake", "[Sales Space]:[Lake]"],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_on_one_connection_gets_one_urn_however_it_is_spelled(
    mock_api_cls: MagicMock, connection: str
) -> None:
    # A bare name is the app's own space's connection, and Qlik matches
    # space and connection names regardless of case.
    assert _source({}, {})._unmapped_file_urn(
        connection, "/Ref//Regions.xlsx", "space-1"
    ) == make_dataset_urn("qlik-sense", "sales space:lake/ref/regions.xlsx")


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_display_name_does_not_depend_on_app_order(
    mock_api_cls: MagicMock,
) -> None:
    references = [("sales space:lake", "ref/x.qvd"), ("Lake", "Ref/X.qvd")]
    names: Set[str] = set()
    for ordered in (references, references[::-1]):
        source = _source({}, {})
        urns = {
            source._unmapped_file_urn(connection, path, "space-1")
            for connection, path in ordered
        }
        assert len(urns) == 1
        names.update(source._unmapped_files.values())

    assert names == {"lib://Sales Space:Lake/Ref/X.qvd"}


@pytest.mark.parametrize("data_files", ["Other Space:DataFiles", "DataFiles"])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_named_by_its_connection_ignores_data_files_on_the_same_path(
    mock_api_cls: MagicMock, data_files: str
) -> None:
    # DataFiles hold Qlik datasets, never a file on external storage, even
    # when not listed.
    script = (
        "Rates: LOAD * FROM [lib://Sales Space:Lake/ref/rates.csv] (txt);\n"
        f"Cached: LOAD * FROM [lib://{data_files}/ref/rates.csv] (txt);\n"
    )
    source = _source({}, {}, script)

    assert source._resolve_file_source(
        "app-1", _file_qri("adls", "lake", "ref/rates.csv")
    ) == make_dataset_urn("qlik-sense", "sales space:lake/ref/rates.csv")
    assert source.reporter.unresolved_file_connections == [CONNECTION]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_named_by_its_connection_or_on_mapped_storage_is_ambiguous(
    mock_api_cls: MagicMock,
) -> None:
    # Only one branch ran, and either connection could hold the file.
    script = (
        "IF vEnv = 'prod' THEN\n"
        "Rates: LOAD * FROM [lib://Sales Space:Store/ref/rates.csv] (txt);\n"
        "ELSE\n"
        "Rates: LOAD * FROM [lib://Sales Space:Lake/ref/rates.csv] (txt);\n"
        "END IF\n"
    )
    source = _source(
        {
            "Sales Space:Store": {
                "storage_uri": "abfss://lake@account.dfs.core.windows.net"
            }
        },
        {},
        script,
    )

    assert (
        source._resolve_file_source("app-1", _file_qri("adls", "lake", "ref/rates.csv"))
        is None
    )
    assert not list(source._gen_unmapped_file_entities())


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
    # The recipe can't set a loop variable, so it is not suggested.
    [warning] = source.reporter.warnings
    assert "script_variables" not in str(warning.message)


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_load_path_variable_nothing_sets_points_to_script_variables(
    mock_api_cls: MagicMock,
) -> None:
    source = _source({}, {})
    app = App.model_validate(
        {
            "id": "app-9",
            "qTitle": "Sales",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
        }
    )
    source._file_source_resolver(
        app,
        parse_script(
            "X: LOAD * FROM [lib://Lake/$(vYear)/a.csv] (txt);\n"
            "Y: LOAD * FROM [lib://Lake/m/*.csv] (txt);\n"
        ),
    )

    [warning] = source.reporter.warnings
    assert "script_variables" in str(warning.message)
    # Only the load a variable can name is listed as settable.
    settable = str(warning.context).split("settable: ")[1]
    assert "$(vYear)" in settable
    assert "*.csv" not in settable


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
    tables: Optional[List[Dict]] = None,
    file_path: str = "landing/orders.csv",
) -> Dict[str, str]:
    return _graph_app_writes(
        *_graph_app_source(
            mock_api_cls,
            file_connection_to_storage,
            unread,
            variables_read_failed,
            script,
            datasets_listed,
            tables,
            file_path=file_path,
        )
    )


def _graph_app_source(
    mock_api_cls: MagicMock,
    file_connection_to_storage: Dict,
    unread: List[UnreadItem],
    variables_read_failed: bool = False,
    script: str = SCRIPT,
    datasets_listed: bool = True,
    tables: Optional[List[Dict]] = None,
    data_connections: Optional[Dict[str, DataConnectionInfo]] = None,
    file_path: str = "landing/orders.csv",
) -> Tuple[QlikSenseSource, App]:
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
    source._data_connections = data_connections or {}
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
            "tables": tables or [],
            "lineage_tables": [
                LineageTable(
                    qri="qri:app:sense://app-1#Orders",
                    label="Orders",
                    source_qris=[
                        _file_qri("adls", "unknown", file_path),
                        f"{RATES_QRI}#Rates",
                    ],
                )
            ],
        }
    )
    return source, app


def _graph_app_writes(source: QlikSenseSource, app: App) -> Dict[str, str]:
    """Each table's upstreams as a whole UpstreamLineage write lists them, or
    "patch:" and those a patch adds."""
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
            writes[wu.get_urn()] = (
                ("" if wu.is_primary_source else "not primary ")
                + "patch:"
                + ",".join(
                    op["value"]["dataset"]
                    for op in json.loads(wu.metadata.aspect.value)
                    if op["op"] == "add" and op["path"].startswith("/upstreams/")
                )
            )
    return writes


ORDERS = make_dataset_urn("qlik-sense", "app-1.orders")
# A file the script doesn't load by name.
UNRESOLVED_FILE = "ref/unread.csv"


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


UNMAPPED_ORDERS_CSV = make_dataset_urn(
    "qlik-sense", "sales space:lake/landing/orders.csv"
)
UNMAPPED_ORDERS_QVD = make_dataset_urn("qlik-sense", "sales space:lake/qvd/orders.qvd")

STORE_SCRIPT = """
Orders:
LOAD * FROM [lib://Sales Space:Lake/landing/orders.csv] (txt);
STORE Orders INTO [lib://Sales Space:Lake//qvd/orders.qvd] (qvd);
STORE Scratch INTO [lib://Sales Space:Lake/qvd/scratch.qvd] (qvd);
"""


@pytest.mark.parametrize("mapped", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_qvd_on_mapped_storage_links_the_stored_table(
    mock_api_cls: MagicMock, mapped: bool
) -> None:
    writes = _graph_app_run(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}}
        if mapped
        else {},
        [],
        script=STORE_SCRIPT,
    )

    stored = {urn: upstreams for urn, upstreams in writes.items() if "qvd," in urn}
    # Scratch is not a table the app ingests, so its QVD has nothing to link.
    # Without the storage, the QVD is only linked at the end of the run.
    assert stored == (
        {
            "urn:li:dataset:(urn:li:dataPlatform:abs,lake/qvd/orders.qvd,PROD)": (
                # Not removed as stale: it is the storage's dataset.
                f"not primary patch:{ORDERS}"
            )
        }
        if mapped
        else {}
    )


@pytest.mark.parametrize("stored_label", ["Customers", "AllCustomers"])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_qvd_links_the_load_model_table_by_its_qri(
    mock_api_cls: MagicMock, stored_label: str
) -> None:
    customers_qri = "qri:db:sqlserver://sales#dbo.Customers"
    writes = _graph_app_run(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script=f"STORE {stored_label} INTO [lib://Sales Space:Lake/qvd/customers.qvd] (qvd);",
        tables=[
            {
                "tableName": "Customers",
                "boxType": "blackbox",
                "tableAlias": "AllCustomers",
                "dataconnectorName": "Sales SQL",
                "connectionInfo": {"id": "conn-1", "sourceConnectorID": "sqlserver"},
                "spaceId": "space-1",
                "fields": [],
                "tableQri": customers_qri,
            }
        ],
    )

    assert writes[
        "urn:li:dataset:(urn:li:dataPlatform:abs,lake/qvd/customers.qvd,PROD)"
    ] == ("not primary patch:" + make_dataset_urn("qlik-sense", customers_qri.lower()))


def _model_table(name: str, alias: str, qri: str) -> Dict:
    return {
        "tableName": name,
        "boxType": "blackbox",
        "tableAlias": alias,
        "dataconnectorName": "Sales SQL",
        "connectionInfo": {"id": "conn-1", "sourceConnectorID": "sqlserver"},
        "spaceId": "space-1",
        "fields": [],
        "tableQri": qri,
    }


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stores_of_a_table_by_name_and_alias_in_competing_branches_are_not_linked(
    mock_api_cls: MagicMock,
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="""
IF vEnv = 'prod' THEN
  STORE Customers INTO [lib://Sales Space:Lake/prod/customers.qvd] (qvd);
ELSE
  STORE AllCustomers INTO [lib://Sales Space:Lake/dev/customers.qvd] (qvd);
END IF
""",
        tables=[
            _model_table(
                "Customers", "AllCustomers", "qri:db:sqlserver://sales#dbo.Customers"
            )
        ],
    )

    assert _stored_files(_graph_app_writes(source, app)) == {}
    assert len(source.reporter.qvd_stores_in_competing_branches) == 2


def _stored_files(writes: Dict[str, str]) -> Dict[str, str]:
    return {urn: upstreams for urn, upstreams in writes.items() if "abs," in urn}


def _store_app(tables: List[Dict]) -> App:
    return App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            "script": STORE_SCRIPT,
            "tables": tables,
        }
    )


@pytest.mark.parametrize("reader", ["read", "kept", None])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_qvd_on_unknown_storage_is_linked_only_when_an_app_reads_it(
    mock_api_cls: MagicMock, reader: Optional[str]
) -> None:
    source = _source(
        {},
        {CONNECTION: DataConnectionInfo(name="Lake", file_platform=FilePlatform.ABS)},
        script="Orders: LOAD * FROM [lib://Sales Space:Lake/qvd/orders.qvd] (qvd);",
    )
    storing_app = _store_app([]).model_copy(update={"id": "app-2"})
    stored = make_dataset_urn("qlik-sense", "app-2.orders")

    assert not list(
        source._gen_file_store_lineage(
            storing_app, parse_script(STORE_SCRIPT), {stored: []}
        )
    )
    # A later app's lineage graph reads the file, so it exists.
    if reader == "read":
        assert source._resolve_file_source(
            "app-1", _file_qri("adls", "lake", "qvd/orders.qvd")
        )
    # Or the reader's last read, which linked it, is kept this run.
    with patch.object(
        source.app_state,
        "previous",
        return_value=AppCheckpoint(urns=[UNMAPPED_ORDERS_QVD]),
    ):
        if reader == "kept":
            assert source._keep_previous_read("app-1")
        workunits = list(source._gen_unmapped_file_entities())

    assert source.reporter.unresolved_file_connections == [CONNECTION]
    lineage = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == UpstreamLineageClass.ASPECT_NAME
    ]
    if reader:
        # Named by its connection and path.
        assert {wu.get_urn() for wu in workunits} == {UNMAPPED_ORDERS_QVD}
        assert [mcp.entityUrn for mcp in lineage] == [UNMAPPED_ORDERS_QVD]
        assert stored in str(lineage[0].aspect)
        # In the storing app's checkpoint too.
        assert UNMAPPED_ORDERS_QVD in source._app_end_of_run_urns["app-2"]
        assert not source.reporter.unread_stored_files
    else:
        assert not workunits
        assert "app-2" not in source._app_end_of_run_urns
        assert list(source.reporter.unread_stored_files) == [
            "lib://Sales Space:Lake/qvd/orders.qvd"
        ]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_qvd_on_unknown_storage_is_named_without_its_extension(
    mock_api_cls: MagicMock,
) -> None:
    source = _source(
        {},
        {CONNECTION: DataConnectionInfo(name="Lake", file_platform=FilePlatform.ABS)},
        script="Orders: LOAD * FROM [lib://Sales Space:Lake/qvd/Orders.QVD] (qvd);",
    )
    list(
        source._gen_file_store_lineage(
            _store_app([]).model_copy(update={"id": "app-2"}),
            parse_script(
                "STORE Orders INTO [lib://Sales Space:Lake/qvd/Orders.QVD] (qvd);"
            ),
            {make_dataset_urn("qlik-sense", "app-2.orders"): []},
        )
    )
    assert source._resolve_file_source(
        "app-1", _file_qri("adls", "lake", "qvd/Orders.QVD")
    )

    names = [
        props.name
        for wu in source._gen_unmapped_file_entities()
        if (props := wu.get_aspect_of_type(DatasetPropertiesClass))
    ]

    assert names == ["Orders"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
@pytest.mark.parametrize(
    "folder, connections",
    [
        pytest.param(
            "Sales Space:Some Folder",
            {"Sales Space:Some Folder": DataConnectionInfo(name="Some Folder")},
            id="a visible folder connection",
        ),
        pytest.param("Other Space:DataFiles", {}, id="an unlisted DataFiles"),
    ],
)
def test_stored_qvd_on_a_connection_without_file_storage_is_skipped(
    mock_api_cls: MagicMock, folder: str, connections: Dict[str, DataConnectionInfo]
) -> None:
    source = _source({}, connections)

    assert not list(
        source._gen_file_store_lineage(
            _store_app([]),
            parse_script(f"STORE Orders INTO [lib://{folder}/x.qvd] (qvd);"),
            {ORDERS: []},
        )
    )
    assert not list(source._gen_unmapped_file_entities())
    # Not a file connection, so there is no storage to map.
    assert not source.reporter.unresolved_file_connections
    assert not source.reporter.unread_stored_files


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_label_naming_two_load_model_tables_links_neither(
    mock_api_cls: MagicMock,
) -> None:
    writes = _graph_app_run(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="STORE Customers INTO [lib://Sales Space:Lake/qvd/customers.qvd] (qvd);",
        tables=[
            _model_table(
                "Customers", "AllCustomers", "qri:db:sqlserver://sales#dbo.Customers"
            ),
            _model_table(
                "CustomersArchive",
                "Customers",
                "qri:db:sqlserver://sales#dbo.CustomersArchive",
            ),
        ],
    )

    assert _stored_files(writes) == {}


@pytest.mark.parametrize(
    "script, linked",
    [
        pytest.param(
            """
IF vEnv = 'prod' THEN
  STORE Orders INTO [lib://Sales Space:Lake/prod/orders.qvd] (qvd);
ELSE
  STORE Orders INTO [lib://Sales Space:Lake/dev/orders.qvd] (qvd);
END IF
""",
            [],
            id="IF and ELSE store into different files",
        ),
        pytest.param(
            """
SWITCH vEnv
CASE 'prod'
  STORE Orders INTO [lib://Sales Space:Lake/prod/orders.qvd] (qvd);
DEFAULT
  STORE Orders INTO [lib://Sales Space:Lake/dev/orders.qvd] (qvd);
END SWITCH
""",
            [],
            id="SWITCH cases store into different files",
        ),
        pytest.param(
            """
IF vEnv = 'prod' THEN
  STORE Orders INTO [lib://Sales Space:Lake/prod/orders.qvd] (qvd);
END IF
""",
            ["prod"],
            id="single IF",
        ),
        pytest.param(
            """
IF vEnv = 'prod' THEN
  STORE Orders INTO [lib://Sales Space:Lake/prod/orders.qvd] (qvd);
ELSE
  STORE Orders INTO [lib://Sales Space:Lake/prod/orders.qvd] (qvd);
END IF
STORE Orders INTO [lib://Sales Space:Lake/dev/orders.qvd] (qvd);
""",
            ["prod", "dev"],
            id="branches agree and a STORE outside the block",
        ),
        pytest.param(
            """
IF vEnv = 'prod' THEN
  STORE Orders INTO [lib://Sales Space:Lake//prod/orders.qvd] (qvd);
ELSE
  STORE Orders INTO [lib://sales space:lake/prod\\orders.qvd] (qvd);
END IF
""",
            ["prod"],
            id="branches write the same file under different spellings",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stores_in_competing_branches_are_not_linked(
    mock_api_cls: MagicMock, script: str, linked: List[str]
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script=f"Orders:\nLOAD * FROM [lib://Sales Space:Lake/landing/orders.csv] (txt);\n{script}",
    )

    assert _stored_files(_graph_app_writes(source, app)) == {
        f"urn:li:dataset:(urn:li:dataPlatform:abs,lake/{env}/orders.qvd,PROD)": (
            f"not primary patch:{ORDERS}"
        )
        for env in linked
    }
    competing = not linked
    assert bool(source.reporter.qvd_stores_in_competing_branches) == competing
    assert (
        "Qlik STOREs in competing branches are not linked"
        in [w.title for w in source.reporter.warnings]
    ) == competing


@pytest.mark.parametrize(
    "listing_failed, variables_failed, file_path, expected",
    [
        (True, False, UNRESOLVED_FILE, f"patch:{RATES}"),
        (False, True, UNRESOLVED_FILE, f"patch:{RATES}"),
        # The script's own variables still name this file, so every source is
        # known.
        (False, True, "landing/orders.csv", f"{UNMAPPED_ORDERS_CSV},{RATES}"),
        (False, False, UNRESOLVED_FILE, RATES),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unresolved_file_keeps_lineage_when_its_inputs_were_not_read(
    mock_api_cls: MagicMock,
    listing_failed: bool,
    variables_failed: bool,
    file_path: str,
    expected: str,
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
        file_path=file_path,
    )

    # The table's resolved source is still added when its file is not.
    assert writes[ORDERS] == expected


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_is_not_named_by_its_connection_when_connections_were_not_listed(
    mock_api_cls: MagicMock,
) -> None:
    # The connection may be one whose storage the listing would have given,
    # so the last run's edges stay rather than move to a connection-named file.
    source, app = _graph_app_source(
        mock_api_cls,
        {},
        [UnreadItem(name=UNREAD_DATA_CONNECTIONS, cause="timeout")],
        script=STORE_SCRIPT,
    )
    writes = _graph_app_writes(source, app)

    assert writes[ORDERS] == f"patch:{RATES}"
    assert not [urn for urn in writes if "sales space:lake" in urn]
    # Not reported as named by its connection; the unread listing is reported.
    assert not source.reporter.unresolved_file_connections


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
        file_path=UNRESOLVED_FILE,
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
        file_path=UNRESOLVED_FILE,
    )

    assert writes[ORDERS] == f"patch:{RATES}"


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unresolved_file_of_an_app_calling_an_undefined_sub_only_adds(
    mock_api_cls: MagicMock,
) -> None:
    # The SUB, such as one from an include, may load the file.
    writes = _graph_app_run(
        mock_api_cls,
        {},
        [],
        script=SCRIPT + "CALL LoadRates;\n",
        file_path=UNRESOLVED_FILE,
    )

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


@pytest.mark.parametrize(
    "keys, expected",
    [
        pytest.param(["Sales Space:Lake"], "abs", id="one key in another case"),
        # Two keys told apart only by case, neither spelled as the script does.
        pytest.param(["Sales Space:Lake", "sales space:lake"], None, id="ambiguous"),
    ],
)
@pytest.mark.parametrize("source_of_storage", ["visible", "mapped"])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_connection_is_found_regardless_of_case(
    mock_api_cls: MagicMock,
    keys: List[str],
    expected: Optional[str],
    source_of_storage: str,
) -> None:
    storage_uri = "abfss://lake@account.dfs.core.windows.net"
    if source_of_storage == "mapped":
        source = _source({key: {"storage_uri": storage_uri} for key in keys}, {})
    else:
        source = _source(
            {},
            {
                key: DataConnectionInfo(
                    name="Lake",
                    file_platform=FilePlatform.ABS,
                    file_storage=FileStorage(platform="abs", location="lake"),
                )
                for key in keys
            },
        )

    storage, _ = source._file_storage("SALES SPACE:LAKE", "space-1")

    assert (storage.platform if storage else None) == expected


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_writer_and_reader_spelling_a_file_in_different_case_join(
    mock_api_cls: MagicMock,
) -> None:
    source = _source(
        {},
        {},
        script="Orders: LOAD * FROM [lib://sales space:lake/QVD/Orders.qvd] (qvd);",
    )
    stored = make_dataset_urn("qlik-sense", "app-2.orders")
    list(
        source._gen_file_store_lineage(
            _store_app([]).model_copy(update={"id": "app-2"}),
            parse_script("STORE Orders INTO [lib://Lake/qvd/orders.qvd] (qvd);"),
            {stored: []},
        )
    )

    assert (
        source._resolve_file_source(
            "app-1", _file_qri("adls", "lake", "QVD/Orders.qvd")
        )
        == UNMAPPED_ORDERS_QVD
    )
    lineage = [
        wu.get_urn()
        for wu in source._gen_unmapped_file_entities()
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == UpstreamLineageClass.ASPECT_NAME
    ]
    assert lineage == [UNMAPPED_ORDERS_QVD]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_on_a_connector_without_storage_mapping_is_not_reported(
    mock_api_cls: MagicMock,
) -> None:
    # Such as SFTP, which `file_connection_to_storage` can't map.
    source = _source(
        {}, {}, script="X: LOAD * FROM [lib://Sales Space:Ftp/x.csv] (txt);"
    )

    assert source._resolve_file_source(
        "app-1", _file_qri("sftp", "ftp", "x.csv")
    ) == make_dataset_urn("qlik-sense", "sales space:ftp/x.csv")
    assert not source.reporter.unresolved_file_connections


def _upstream_ops(workunits: Iterable[MetadataWorkUnit]) -> Dict[str, List[str]]:
    """`<op> <path>` of each lineage patch op, by dataset, with table edges
    as `<op> upstream <dataset>`."""
    ops: Dict[str, List[str]] = {}
    for wu in workunits:
        if (
            isinstance(wu.metadata, MetadataChangeProposalClass)
            and wu.metadata.aspectName == UpstreamLineageClass.ASPECT_NAME
            and wu.metadata.aspect is not None
        ):
            for op in json.loads(wu.metadata.aspect.value):
                parts = op["path"].split("/")
                ops.setdefault(wu.get_urn(), []).append(
                    f"{op['op']} upstream {parts[2].replace('~1', '/')}"
                    if parts[1] == "upstreams"
                    else f"{op['op']} {op['path'].replace('~1', '/')}"
                )
    return ops


@pytest.mark.parametrize(
    "storage_uri, qri_scheme, file_urn",
    [
        (
            "abfss://lake@account.dfs.core.windows.net",
            "adls",
            "urn:li:dataset:(urn:li:dataPlatform:abs,lake/qvd/orders.qvd,PROD)",
        ),
        (
            "s3://sales-bucket",
            "s3",
            "urn:li:dataset:(urn:li:dataPlatform:s3,sales-bucket/qvd/orders.qvd,PROD)",
        ),
        (
            "gs://sales-bucket",
            "gcs",
            "urn:li:dataset:(urn:li:dataPlatform:gcs,sales-bucket/qvd/orders.qvd,PROD)",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_qvd_is_the_file_another_app_loads(
    mock_api_cls: MagicMock, storage_uri: str, qri_scheme: str, file_urn: str
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": storage_uri}},
        [],
        script=STORE_SCRIPT,
    )
    stored = _graph_app_writes(source, app)
    reader = App.model_validate(
        {
            "id": "app-2",
            "qTitle": "Reader",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
        }
    )
    source._file_sources_by_app[reader.id] = source._file_source_resolver(
        reader,
        parse_script("X: LOAD * FROM [lib://Sales Space:Lake/qvd/orders.qvd] (qvd);"),
    )
    source._app_space_ids[reader.id] = reader.spaceId

    assert stored[file_urn] == f"not primary patch:{ORDERS}"
    assert (
        source._resolve_file_source(
            reader.id, _file_qri(qri_scheme, "unknown", "qvd/orders.qvd")
        )
        == file_urn
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_qvd_on_an_https_azure_connection_is_named_from_the_container(
    mock_api_cls: MagicMock, requests_mock: requests_mock_module.Mocker
) -> None:
    base_url = "https://tenant.example.com/api/v1"
    requests_mock.get(f"{base_url}/api-keys", json={})
    requests_mock.get(
        f"{base_url}/data-connections",
        json={
            "data": [
                {
                    "qName": "Lake",
                    "space": "space-1",
                    "datasourceID": "File_AzureStorageConnector",
                    "qConnectStatement": 'CUSTOM CONNECT TO "provider='
                    "QvWebStorageProviderConnectorPackage.exe;"
                    "sourceType=File_AzureStorageConnector;"
                    'blobContainerPath=https://account.blob.core.windows.net/lake;"',
                }
            ]
        },
    )
    api = RealQlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    api.spaces = {"space-1": "Sales Space"}
    source, app = _graph_app_source(
        mock_api_cls,
        {},
        [],
        script=STORE_SCRIPT,
        data_connections=api.get_data_connections(),
    )

    assert _stored_files(_graph_app_writes(source, app)) == {
        "urn:li:dataset:(urn:li:dataPlatform:abs,lake/qvd/orders.qvd,PROD)": (
            f"not primary patch:{ORDERS}"
        )
    }


@pytest.mark.parametrize(
    "store, file_urn",
    [
        (
            "STORE Orders INTO [lib://Sales Space:Lake/out/orders.parquet] (parquet);",
            "urn:li:dataset:(urn:li:dataPlatform:abs,lake/out/orders.parquet,PROD)",
        ),
        (
            "STORE Orders INTO [lib://Sales Space:Lake/out/orders.csv] "
            "(txt, delimiter is ',');",
            "urn:li:dataset:(urn:li:dataPlatform:abs,lake/out/orders.csv,PROD)",
        ),
        # Written as a QVD whatever its extension, as no format is given.
        (
            "STORE Orders INTO [lib://Sales Space:Lake/out/orders.csv];",
            "urn:li:dataset:(urn:li:dataPlatform:abs,lake/out/orders.csv,PROD)",
        ),
        ("STORE Orders INTO [lib://Sales Space:Lake/out/orders.json] (json);", None),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_text_and_parquet_stores_are_linked_and_other_formats_are_not(
    mock_api_cls: MagicMock, store: str, file_urn: Optional[str]
) -> None:
    writes = _graph_app_run(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="Orders:\nLOAD * FROM [lib://Sales Space:Lake/landing/orders.csv] "
        f"(txt);\n{store}\n",
    )

    assert _stored_files(writes) == (
        {file_urn: f"not primary patch:{ORDERS}"} if file_urn else {}
    )


@pytest.mark.parametrize(
    "catalogued_connection, linked",
    [
        pytest.param(None, True, id="not catalogued"),
        pytest.param("unknown", False, id="catalogued on any connection"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_qvd_qlik_catalogues_is_not_linked_on_storage(
    mock_api_cls: MagicMock, catalogued_connection: Optional[str], linked: bool
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script=STORE_SCRIPT,
    )
    if catalogued_connection:
        # Loads of the file resolve to the Qlik dataset, which its own lineage
        # graph links the storing table into.
        catalogued = _file_qri("adls", catalogued_connection, "qvd/orders.qvd")
        mock_api_cls.return_value.list_datasets.return_value = [
            ListedDataset(
                space_id="space-1",
                name="orders.qvd",
                secure_qri=catalogued.rpartition("#")[0],
            )
        ]

    assert bool(_stored_files(_graph_app_writes(source, app))) == linked


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_qvd_is_not_linked_when_qlik_datasets_cant_be_listed(
    mock_api_cls: MagicMock,
) -> None:
    writes = _graph_app_run(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script=STORE_SCRIPT,
        datasets_listed=False,
    )

    assert _stored_files(writes) == {}


DEV_ORDERS = "urn:li:dataset:(urn:li:dataPlatform:abs,lake/dev/orders.qvd,PROD)"
PROD_ORDERS = "urn:li:dataset:(urn:li:dataPlatform:abs,lake/prod/orders.qvd,PROD)"
OTHER_APP_TABLE = make_dataset_urn("qlik-sense", "app-2.orders")


@pytest.mark.parametrize("complete", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_moved_store_removes_only_the_apps_own_edge_from_the_old_file(
    mock_api_cls: MagicMock, complete: bool
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="Orders:\nLOAD * FROM [lib://Sales Space:Lake/landing/orders.csv] "
        "(txt);\nSTORE Orders INTO [lib://Sales Space:Lake/prod/orders.qvd];\n"
        + ("" if complete else "$(Must_Include=lib://Lake/common.qvs);\n"),
    )
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=ORDERS, type=DatasetLineageTypeClass.COPY),
            UpstreamClass(dataset=OTHER_APP_TABLE, type=DatasetLineageTypeClass.COPY),
        ]
    )
    source.ctx.graph = graph
    previous = AppCheckpoint(urns=[ORDERS], stored_file_urns=[DEV_ORDERS])

    with patch.object(QlikSenseSource, "_usable_checkpoint", return_value=previous):
        ops = _upstream_ops(source._gen_app_workunit(app))

    # The app's edge into the file it no longer stores into goes; another
    # app's edge stays. With an included script, the app may still store
    # into it, so it is only added to.
    assert ops.get(DEV_ORDERS) == ([f"remove upstream {ORDERS}"] if complete else None)
    assert f"add upstream {ORDERS}" in ops[PROD_ORDERS]
    assert not [op for op in ops[PROD_ORDERS] if op.startswith("remove")]
    assert source._app_stored_files[app.id] == (
        [PROD_ORDERS] if complete else [DEV_ORDERS, PROD_ORDERS]
    )


CUSTOMERS_QRI = "qri:db:sqlserver://sales#dbo.Customers"
CUSTOMERS = make_dataset_urn("qlik-sense", CUSTOMERS_QRI.lower())
DEV_CUSTOMERS = "urn:li:dataset:(urn:li:dataPlatform:abs,lake/dev/customers.qvd,PROD)"


@pytest.mark.parametrize("other_app_seen", ["this run", "last run"])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_moved_store_keeps_the_edge_of_a_table_another_app_shares(
    mock_api_cls: MagicMock, other_app_seen: str
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="STORE Customers INTO [lib://Sales Space:Lake/prod/customers.qvd];",
        tables=[_model_table("Customers", "Customers", CUSTOMERS_QRI)],
    )
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=CUSTOMERS, type=DatasetLineageTypeClass.COPY)]
    )
    source.ctx.graph = graph
    other_app = AppCheckpoint(
        urns=[CUSTOMERS], table_urns_by_qri={CUSTOMERS_QRI: CUSTOMERS}
    )
    if other_app_seen == "this run":
        source._table_urns_by_app["app-2"] = {CUSTOMERS}
    previous = AppCheckpoint(urns=[CUSTOMERS], stored_file_urns=[DEV_CUSTOMERS])

    with (
        patch.object(QlikSenseSource, "_usable_checkpoint", return_value=previous),
        patch.object(
            source.app_state,
            "previous_apps",
            return_value={"app-2": other_app} if other_app_seen == "last run" else {},
        ),
    ):
        ops = _upstream_ops(source._gen_app_workunit(app))

    # Another app loading the same table may be what stores it into the old
    # file, so its edge stays.
    assert not [op for op in ops.get(DEV_CUSTOMERS, []) if op.startswith("remove")]


@pytest.mark.parametrize("read", ["no graph", "read error"])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_old_file_whose_lineage_cant_be_read_stays_in_the_checkpoint(
    mock_api_cls: MagicMock, read: str
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="Orders:\nLOAD * FROM [lib://Sales Space:Lake/landing/orders.csv] "
        "(txt);\nSTORE Orders INTO [lib://Sales Space:Lake/prod/orders.qvd];\n",
    )
    if read == "read error":
        graph = MagicMock()
        graph.get_aspect.side_effect = RuntimeError("unavailable")
        source.ctx.graph = graph
    else:
        source.ctx.graph = None
    previous = AppCheckpoint(urns=[ORDERS], stored_file_urns=[DEV_ORDERS])

    with patch.object(QlikSenseSource, "_usable_checkpoint", return_value=previous):
        ops = _upstream_ops(source._gen_app_workunit(app))

    assert DEV_ORDERS not in ops
    # A later run that can read its lineage still removes the app's edge.
    assert source._app_stored_files[app.id] == [DEV_ORDERS, PROD_ORDERS]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stores_through_two_names_of_one_connection_do_not_compete(
    mock_api_cls: MagicMock,
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="""
Orders:
LOAD * FROM [lib://Sales Space:Lake/landing/orders.csv] (txt);
IF vEnv = 'prod' THEN
  STORE Orders INTO [lib://Sales Space:Lake/qvd/orders.qvd] (qvd);
ELSE
  STORE Orders INTO [lib://Lake/qvd/orders.qvd] (qvd);
END IF
""",
    )

    assert _stored_files(_graph_app_writes(source, app)) == {
        "urn:li:dataset:(urn:li:dataPlatform:abs,lake/qvd/orders.qvd,PROD)": (
            f"not primary patch:{ORDERS}"
        )
    }
    assert not source.reporter.qvd_stores_in_competing_branches


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_fields_get_column_lineage(mock_api_cls: MagicMock) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="Orders:\nLOAD * FROM [lib://Sales Space:Lake/landing/orders.csv] "
        "(txt);\nSTORE OrderId, Amount AS Total FROM Orders INTO "
        "[lib://Sales Space:Lake/clean/orders.parquet] (parquet);\n",
    )

    # A QVD on storage gets table lineage only; see the storage QVD tests.
    ops = _upstream_ops(source._gen_app_workunit(app))[
        "urn:li:dataset:(urn:li:dataPlatform:abs,lake/clean/orders.parquet,PROD)"
    ]

    column_edges = [op for op in ops if op.startswith("add /fineGrainedLineages/")]
    assert len(column_edges) == 2
    assert any(
        make_schema_field_urn(ORDERS, "Amount") in op and ",Total)" in op
        for op in column_edges
    )
    assert source.reporter.file_stores_linked == 1
