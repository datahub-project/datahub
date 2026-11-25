import json
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple
from unittest.mock import MagicMock, call, patch

import pytest
import requests_mock as requests_mock_module
from pydantic import ValidationError

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.app_state import (
    AppCheckpoint,
    UnmappedFileStore,
)
from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    LineageTable,
    ListedDataset,
    SchemaField,
    UnchangedApp,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.file_lineage import (
    FilePlatform,
    FileStorage,
    file_storage_of_connection,
    file_storage_of_uri,
    qri_part_id,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_base_api import UNREAD_DATA_CONNECTIONS
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import (
    DataConnectionInfo,
    FileLoad,
    IncludeRead,
    ParsedScript,
    inline_includes,
    parse_script,
)
from datahub.ingestion.source.qlik_sense.websocket_connection import EngineError
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
        source._file_source_resolution(
            "app-4", _file_qri("adls", "unknown", "old/f.csv")
        ).urn
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
    urn = source._file_source_resolution(
        "app-1", _file_qri("adls", "unknown", "landing/orders.csv")
    ).urn

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
        source._file_source_resolution(
            "app-1", _file_qri("adls", "lake", "ref/regions.xlsx")
        ).urn
        == "urn:li:dataset:(urn:li:dataPlatform:abs,lake/ref/regions.xlsx,PROD)"
    )
    # Same path, but the QRI's connection is another container.
    assert (
        source._file_source_resolution(
            "app-1", _file_qri("adls", "other", "ref/regions.xlsx")
        ).urn
        is None
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_without_known_storage_is_reported(mock_api_cls: MagicMock) -> None:
    source = _source({}, {})

    # A bare name is the app's own space's connection, as Qlik Cloud resolves it.
    assert source._file_source_resolution(
        "app-1", _file_qri("adls", "lake", "ref/targets.csv")
    ).urn == make_dataset_urn("qlik-sense", "sales space:lake/ref/targets.csv")
    # A file the script does not read is not the connection's to report.
    assert (
        source._file_source_resolution(
            "app-1", _file_qri("adls", "lake", "other.csv")
        ).urn
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
        source._file_source_resolution(
            "app-1", _file_qri("adls", "lake", "ref/regions.xlsx")
        ).urn
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


@pytest.mark.parametrize(
    "connection, space, name",
    [
        ("[Team: Sales]:[Lake]", "Team: Sales", "Lake"),
        ("[Team: Sales]:Lake", "Team: Sales", "Lake"),
        ("Sales Space:[Lake: Raw]", "Sales Space", "Lake: Raw"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_bracketed_name_holding_the_separator_is_one_space_or_connection(
    mock_api_cls: MagicMock, connection: str, space: str, name: str
) -> None:
    source = _source({}, {})

    urn = source._unmapped_file_urn(connection, "ref/regions.xlsx", "space-1")

    qualified = f"[{space}]:{name}" if ":" in space else f"{space}:[{name}]"
    assert urn == make_dataset_urn(
        "qlik-sense", f"{qualified.lower()}/ref/regions.xlsx"
    )
    assert source._unmapped_files[urn] == f"lib://{qualified}/ref/regions.xlsx"
    browse_path = next(
        b
        for wu in source._gen_lib_path_location(urn, source._unmapped_files[urn])
        if (b := wu.get_aspect_of_type(BrowsePathsV2Class))
    )
    assert [e.id for e in browse_path.path] == [space, name, "ref"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_whose_space_cant_be_told_from_its_connection_is_not_placed(
    mock_api_cls: MagicMock,
) -> None:
    # Qlik records loads without the brackets around a name holding a colon.
    source = _source({}, {})
    path = "lib://team: sales:lake/ref/regions.xlsx"
    urn = make_dataset_urn("qlik-sense", "team: sales:lake/ref/regions.xlsx")

    assert not list(source._gen_lib_path_location(urn, path))

    # A script spelling the space with brackets tells them apart.
    source._unmapped_file_urn("[Team: Sales]:Lake", "ref/regions.xlsx", "space-1")
    browse_path = next(
        b
        for wu in source._gen_lib_path_location(urn, path)
        if (b := wu.get_aspect_of_type(BrowsePathsV2Class))
    )
    assert [e.id for e in browse_path.path] == ["Team: Sales", "Lake", "ref"]


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

    assert source._file_source_resolution(
        "app-1", _file_qri("adls", "lake", "ref/rates.csv")
    ).urn == make_dataset_urn("qlik-sense", "sales space:lake/ref/rates.csv")
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
        source._file_source_resolution(
            "app-1", _file_qri("adls", "lake", "ref/rates.csv")
        ).urn
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
        source._file_source_resolution(
            "app-1", _file_qri("s3", "unknown", "ref/targets.csv")
        ).urn
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
        source._file_source_resolution(
            "app-1", _file_qri("gcs", "sales", "ref/regions.xlsx")
        ).urn
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
        source._file_source_resolution(
            "app-1", _file_qri("s3", "eu/b", "ref/targets.csv")
        ).urn
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
        source._file_source_resolution(
            "app-2", _file_qri("adls", "unknown", "ref/regions.xlsx")
        ).urn
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
        source._file_source_resolution("app-1", _file_qri("s3", "eu/dev", "x.csv")).urn
        is None
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

    resolved = source._file_source_resolution(
        "app-1", _file_qri("adls", connection_part, "x.csv")
    ).urn
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


def _app_and_end_of_run(
    source: QlikSenseSource, app: App
) -> Iterable[MetadataWorkUnit]:
    yield from source._gen_app_workunit(app)
    yield from source._gen_pending_table_lineage()


def _graph_app_writes(source: QlikSenseSource, app: App) -> Dict[str, str]:
    """Each table's upstreams as a whole UpstreamLineage write lists them, or
    "patch:" and those a patch adds."""
    writes: Dict[str, str] = {}
    for wu in _app_and_end_of_run(source, app):
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


def _store_lineage(
    source: QlikSenseSource,
    app: App,
    parsed: ParsedScript,
    table_fields: Dict[str, List[SchemaField]],
) -> List[MetadataWorkUnit]:
    source._queue_file_store_lineage(app, parsed, table_fields)
    return list(source._gen_pending_table_lineage())


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

    assert not _store_lineage(
        source, storing_app, parse_script(STORE_SCRIPT), {stored: []}
    )
    # A later app's lineage graph reads the file, so it exists.
    if reader == "read":
        assert source._file_source_resolution(
            "app-1", _file_qri("adls", "lake", "qvd/orders.qvd")
        ).urn
    # Or the reader's last read, which linked it, is kept this run.
    with patch.object(
        source.app_state,
        "previous",
        return_value=AppCheckpoint(
            urns=[UNMAPPED_ORDERS_QVD], read_unmapped_file_urns=[UNMAPPED_ORDERS_QVD]
        ),
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
def test_read_app_records_its_stores_into_files_on_unknown_storage(
    mock_api_cls: MagicMock,
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {},
        [],
        script="STORE Customers INTO [lib://Sales Space:Lake/qvd/customers.qvd] (qvd);",
        tables=[
            _model_table(
                "Customers", "Customers", "qri:db:sqlserver://sales#dbo.Customers"
            )
        ],
        data_connections={
            CONNECTION: DataConnectionInfo(name="Lake", file_platform=FilePlatform.ABS)
        },
    )
    source._fingerprint_salt = "salt"

    with patch.object(source.app_state, "record") as record:
        list(source._gen_recorded_app_workunit(app))

    file_urn = make_dataset_urn("qlik-sense", "sales space:lake/qvd/customers.qvd")
    stores = record.call_args.args[1].unmapped_file_stores
    assert list(stores) == [file_urn]
    assert stores[file_urn].path == "lib://Sales Space:Lake/qvd/customers.qvd"
    assert len(stores[file_urn].table_urns) == 1


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_kept_writer_of_a_file_on_unknown_storage_links_it_when_an_app_reads_it(
    mock_api_cls: MagicMock,
) -> None:
    source = _source(
        {},
        {CONNECTION: DataConnectionInfo(name="Lake", file_platform=FilePlatform.ABS)},
        script="Orders: LOAD * FROM [lib://Sales Space:Lake/qvd/orders.qvd] (qvd);",
    )
    source._fingerprint_salt = "salt"
    stored = make_dataset_urn("qlik-sense", "app-2.orders")
    # Its STORE was recorded on a run where no app read the file.
    previous = AppCheckpoint(
        urns=[stored],
        unmapped_file_stores={
            UNMAPPED_ORDERS_QVD: UnmappedFileStore(
                path="lib://Sales Space:Lake/qvd/orders.qvd", table_urns=[stored]
            )
        },
        read_unmapped_file_urns=[
            make_dataset_urn("qlik-sense", "sales space:lake/ref/rates.csv")
        ],
    )
    with (
        patch.object(source.app_state, "previous", return_value=previous),
        patch.object(source.app_state, "record") as record,
    ):
        source._keep_unchanged_app(UnchangedApp(id="app-2", fingerprint="f"))
    assert source._file_source_resolution(
        "app-1", _file_qri("adls", "lake", "qvd/orders.qvd")
    ).urn

    workunits = list(source._gen_unmapped_file_entities())

    lineage = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == UpstreamLineageClass.ASPECT_NAME
    ]
    assert [mcp.entityUrn for mcp in lineage] == [UNMAPPED_ORDERS_QVD]
    assert stored in str(lineage[0].aspect)
    assert UNMAPPED_ORDERS_QVD in source._app_end_of_run_urns["app-2"]
    # Carried forward, so a later run that keeps it again still links it.
    recorded = record.call_args.args[1]
    assert recorded.unmapped_file_stores == previous.unmapped_file_stores
    assert recorded.read_unmapped_file_urns == previous.read_unmapped_file_urns
    # Read by app-1, which records it; the writer did not read it.
    with patch.object(source.app_state, "add_read_unmapped_files") as add_reads:
        source._record_end_of_run_urns()
    assert add_reads.call_args_list == [call("app-1", {UNMAPPED_ORDERS_QVD})]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_kept_writer_alone_does_not_count_as_a_reader_of_its_file(
    mock_api_cls: MagicMock,
) -> None:
    source = _source({}, {})
    source._fingerprint_salt = "salt"
    stored = make_dataset_urn("qlik-sense", "app-2.orders")
    # A run that linked the file recorded it among the writer's entities too.
    previous = AppCheckpoint(
        urns=[stored, UNMAPPED_ORDERS_QVD],
        unmapped_file_stores={
            UNMAPPED_ORDERS_QVD: UnmappedFileStore(
                path="lib://Sales Space:Lake/qvd/orders.qvd", table_urns=[stored]
            )
        },
    )
    with (
        patch.object(source.app_state, "previous", return_value=previous),
        patch.object(source.app_state, "record"),
    ):
        source._keep_unchanged_app(UnchangedApp(id="app-2", fingerprint="f"))
        workunits = list(source._gen_unmapped_file_entities())

    assert not workunits
    assert list(source.reporter.unread_stored_files) == [
        "lib://Sales Space:Lake/qvd/orders.qvd"
    ]


ABS_ORDERS_QVD = "urn:li:dataset:(urn:li:dataPlatform:abs,lake/qvd/orders.qvd,PROD)"


def _keep_writer_of_unmapped_store(
    source: QlikSenseSource,
) -> Tuple[AppCheckpoint, List[MetadataWorkUnit], List[str]]:
    stored = make_dataset_urn("qlik-sense", "app-2.orders")
    previous = AppCheckpoint(
        urns=[stored, UNMAPPED_ORDERS_QVD],
        unmapped_file_stores={
            UNMAPPED_ORDERS_QVD: UnmappedFileStore(
                path="lib://Sales Space:Lake/qvd/orders.qvd", table_urns=[stored]
            )
        },
    )
    assert source.stale_entity_removal_handler is not None
    with (
        patch.object(source.app_state, "previous", return_value=previous),
        patch.object(source.app_state, "record") as record,
        patch.object(
            source.stale_entity_removal_handler, "add_entity_to_state"
        ) as add_to_state,
    ):
        source._keep_unchanged_app(UnchangedApp(id="app-2", fingerprint="f"))
        # A fully read app's lineage graph reads the file.
        source._file_source_resolution(
            "app-1", _file_qri("adls", "lake", "qvd/orders.qvd")
        )
        workunits = list(source._gen_unmapped_file_entities())
    pinned = [args.args[1] for args in add_to_state.call_args_list]
    return record.call_args.args[1], workunits, pinned


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_kept_writer_links_a_file_whose_storage_is_mapped_since_on_storage(
    mock_api_cls: MagicMock,
) -> None:
    mock_api_cls.return_value.list_datasets.return_value = []
    source = _source(
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        {},
    )
    source._fingerprint_salt = "salt"
    stored = make_dataset_urn("qlik-sense", "app-2.orders")

    recorded, workunits, pinned = _keep_writer_of_unmapped_store(source)

    assert UNMAPPED_ORDERS_QVD not in pinned
    assert {wu.get_urn() for wu in workunits} == {ABS_ORDERS_QVD}
    assert all(not wu.is_primary_source for wu in workunits)
    assert stored in str(workunits[0].metadata)
    assert not source.reporter.unread_stored_files
    assert recorded.urns == [stored]
    assert recorded.unmapped_file_stores == {}
    # So a later full read can remove the edge once the app stops storing.
    assert recorded.stored_file_urns == [ABS_ORDERS_QVD]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_kept_writer_keeps_its_unmapped_store_when_datasets_cannot_be_listed(
    mock_api_cls: MagicMock,
) -> None:
    mock_api_cls.return_value.list_datasets.return_value = None
    source = _source(
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        {},
    )
    source._fingerprint_salt = "salt"

    recorded, workunits, pinned = _keep_writer_of_unmapped_store(source)

    assert UNMAPPED_ORDERS_QVD in pinned
    assert ABS_ORDERS_QVD not in {wu.get_urn() for wu in workunits}
    assert list(recorded.unmapped_file_stores) == [UNMAPPED_ORDERS_QVD]
    assert UNMAPPED_ORDERS_QVD in recorded.urns


@pytest.mark.parametrize("listing_failed", [False, True])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_kept_writer_of_a_file_still_on_unknown_storage_keeps_it_unmapped(
    mock_api_cls: MagicMock, listing_failed: bool
) -> None:
    mock_api_cls.return_value.list_datasets.return_value = []
    if listing_failed:
        mock_api_cls.return_value.unread_items = [
            UnreadItem(name=UNREAD_DATA_CONNECTIONS, cause="timeout")
        ]
    source = _source(
        {},
        {}
        if listing_failed
        else {
            CONNECTION: DataConnectionInfo(name="Lake", file_platform=FilePlatform.ABS)
        },
    )
    source._fingerprint_salt = "salt"

    recorded, workunits, pinned = _keep_writer_of_unmapped_store(source)

    assert UNMAPPED_ORDERS_QVD in pinned
    assert ABS_ORDERS_QVD not in {wu.get_urn() for wu in workunits}
    assert list(recorded.unmapped_file_stores) == [UNMAPPED_ORDERS_QVD]
    assert UNMAPPED_ORDERS_QVD in recorded.urns


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_qvd_on_unknown_storage_is_named_without_its_extension(
    mock_api_cls: MagicMock,
) -> None:
    source = _source(
        {},
        {CONNECTION: DataConnectionInfo(name="Lake", file_platform=FilePlatform.ABS)},
        script="Orders: LOAD * FROM [lib://Sales Space:Lake/qvd/Orders.QVD] (qvd);",
    )
    _store_lineage(
        source,
        _store_app([]).model_copy(update={"id": "app-2"}),
        parse_script(
            "STORE Orders INTO [lib://Sales Space:Lake/qvd/Orders.QVD] (qvd);"
        ),
        {make_dataset_urn("qlik-sense", "app-2.orders"): []},
    )
    assert source._file_source_resolution(
        "app-1", _file_qri("adls", "lake", "qvd/Orders.QVD")
    ).urn

    names = [
        props.name
        for wu in source._gen_unmapped_file_entities()
        if (props := wu.get_aspect_of_type(DatasetPropertiesClass))
    ]

    assert names == ["Orders"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_qvd_on_data_files_is_skipped(mock_api_cls: MagicMock) -> None:
    source = _source({}, {})

    assert not _store_lineage(
        source,
        _store_app([]),
        parse_script("STORE Orders INTO [lib://Other Space:DataFiles/x.qvd] (qvd);"),
        {ORDERS: []},
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
        source._file_source_resolution(
            "app-5", _file_qri("adls", "unknown", "old/f.csv")
        ).urn
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
        source._file_source_resolution(
            "app-3", _file_qri("adls", "unknown", "ref/regions.xlsx")
        ).urn
        == "urn:li:dataset:(urn:li:dataPlatform:abs,lake/ref/regions.xlsx,PROD)"
    )


def test_included_scripts_are_read_into_the_script() -> None:
    files = {
        "lib://Common:DataFiles/env.qvs": "SET vLake = 'lib://Sales Space:Lake/';\n"
        "$(Include=lib://$(vCommon)DataFiles/loads.qvs);\n",
        "lib://Common:DataFiles/loads.qvs": "Rates: LOAD * FROM [$(vLake)rates.csv];\n",
    }
    script = (
        "$(Must_Include=lib://Common:DataFiles/env.qvs);\n"
        "$(Include=lib://Common:DataFiles/missing.qvs);\n"
    )
    inlined = inline_includes(script, {"vCommon": "'Common:'"}, _reader(files))
    parsed = parse_script(inlined)

    assert [load.expanded for load in parsed.file_loads] == [
        "lib://Sales Space:Lake/rates.csv"
    ]
    # The include that could not be read still leaves the script incomplete.
    assert parsed.has_includes
    assert not parse_script(
        inline_includes(script.split("\n")[0], {"vCommon": "'Common:'"}, _reader(files))
    ).has_includes


@pytest.mark.parametrize("separator", [";", ""])
def test_include_with_a_variable_in_its_path_keeps_the_next_label(
    separator: str,
) -> None:
    parsed = parse_script(
        f"$(Include=lib://$(vPrefix)Common/x.qvs){separator}\n"
        "Orders: LOAD *; SQL SELECT * FROM sales.orders;"
    )

    assert [table.label for table in parsed.tables] == ["Orders"]
    assert parsed.has_includes


def test_include_path_with_parentheses_is_read() -> None:
    files = {
        "lib://Common:DataFiles/loads (v2).qvs": "X: LOAD * FROM [lib://S:Lake/x.csv];"
    }
    parsed = parse_script(
        inline_includes(
            "$(Include=lib://$(vCommon)DataFiles/loads (v2).qvs);",
            {"vCommon": "'Common:'"},
            _reader(files),
        )
    )

    assert [load.expanded for load in parsed.file_loads] == ["lib://S:Lake/x.csv"]
    assert not parsed.has_includes


def test_unread_include_path_with_parentheses_forgets_the_connection() -> None:
    script = (
        "LIB CONNECT TO 'A';\n"
        "$(Include=lib://Common:DataFiles/loads (v2).qvs);\n"
        "Orders: SQL SELECT * FROM sales.orders;"
    )
    files: Dict[str, str] = {}
    parsed = parse_script(inline_includes(script, {}, _reader(files)))

    assert [(table.label, table.connection) for table in parsed.tables] == [
        ("Orders", None)
    ]
    assert parsed.has_includes


@pytest.mark.parametrize(
    "path",
    [
        "[lib://$(vCommon)DataFiles/loads.qvs]",
        "'lib://$(vCommon)DataFiles/loads.qvs'",
        '"lib://Common:DataFiles/loads.qvs"',
        "$(vQuotedPath)",
    ],
)
def test_bracketed_or_quoted_include_path_is_read(path: str) -> None:
    read_paths: List[str] = []
    files = {"lib://Common:DataFiles/loads.qvs": "X: LOAD * FROM [lib://S:Lake/x.csv];"}

    def read(include_path: str) -> IncludeRead:
        read_paths.append(include_path)
        return _reader(files)(include_path)

    parsed = parse_script(
        inline_includes(
            f"$(Must_Include={path});",
            {
                "vCommon": "'Common:'",
                "vQuotedPath": "[lib://Common:DataFiles/loads.qvs]",
            },
            read,
        )
    )

    assert [load.expanded for load in parsed.file_loads] == ["lib://S:Lake/x.csv"]
    assert not parsed.has_includes
    assert read_paths == ["lib://Common:DataFiles/loads.qvs"]


def test_include_outside_a_lib_path_is_not_read() -> None:
    files = {"common.qvs": "X: LOAD * FROM [lib://S:Lake/x.csv];"}
    parsed = parse_script(inline_includes("$(Include=common.qvs);", {}, _reader(files)))

    assert not parsed.file_loads
    assert parsed.has_includes


def _include_api() -> QlikAPI:
    with patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session"):
        return QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))


def _engine(*replies: object) -> MagicMock:
    engine = MagicMock()
    engine.websocket_send_request.side_effect = list(replies)
    return engine


def test_included_script_is_read_once_a_run_per_space() -> None:
    api = _include_api()
    engine = _engine(
        {"qContent": "X: LOAD 1 AS a AUTOGENERATE 1;"},
        {"qContent": "Y: LOAD 2 AS a AUTOGENERATE 1;"},
    )

    for _ in range(2):
        assert api._read_include(
            engine, "space-1", "lib://DataFiles/x.qvs", {}
        ) == IncludeRead(content="X: LOAD 1 AS a AUTOGENERATE 1;")
    # A path without a space resolves in the reading app's space.
    assert api._read_include(
        engine, "space-2", "lib://DataFiles/x.qvs", {}
    ) == IncludeRead(content="Y: LOAD 2 AS a AUTOGENERATE 1;")
    assert engine.websocket_send_request.call_count == 2


@pytest.mark.parametrize(
    "reply",
    [EngineError({"code": 5, "message": "Access denied"}), {}],
)
def test_include_the_engine_refuses_is_not_read_again(reply: object) -> None:
    api = _include_api()
    engine = _engine(reply)

    # A new app each time, so only the run's cache can stop the second read.
    for _ in range(2):
        result = api._read_include(engine, "space-1", "lib://B:DataFiles/y.qvs", {})
        assert result.content is None and result.error
    assert engine.websocket_send_request.call_count == 1


def test_include_read_failing_outside_the_engine_is_read_again_for_the_next_app() -> (
    None
):
    api = _include_api()
    engine = _engine(
        ConnectionError("socket closed"),
        {"qContent": "W: LOAD 3 AS a AUTOGENERATE 1;"},
    )
    path = "lib://DataFiles/w.qvs"
    first_app: Dict[str, IncludeRead] = {}

    for _ in range(2):
        assert api._read_include(engine, "space-1", path, first_app).error
    assert api._read_include(engine, "space-1", path, {}).content
    assert engine.websocket_send_request.call_count == 2


def _reader(files: Dict[str, str]) -> Callable[[str], IncludeRead]:
    return lambda path: (
        IncludeRead(content=files[path])
        if path in files
        else IncludeRead(error="not found")
    )


_INCLUDED_LOADS = {
    "lib://S:DataFiles/a.qvs": "A: LOAD * FROM [lib://S:Lake/a.csv];",
    "lib://S:DataFiles/b.qvs": "B: LOAD * FROM [lib://S:Lake/b.csv];",
    "lib://S:DataFiles/setter.qvs": "SET vF = 'lib://S:DataFiles/b.qvs';",
}


@pytest.mark.parametrize(
    "script, loads",
    [
        (
            "SET vF = 'lib://S:DataFiles/b.qvs';\n$(Include=$(vF));",
            ["lib://S:Lake/b.csv"],
        ),
        (
            "SET vF = 'lib://S:DataFiles/a.qvs';\n$(Include=$(vF));\n"
            "SET vF = 'lib://S:DataFiles/b.qvs';\n$(Include=$(vF));",
            [],
        ),
        (
            "FOR EACH vF IN 'lib://S:DataFiles/a.qvs', 'lib://S:DataFiles/b.qvs'\n"
            "$(Include=$(vF));\nNEXT vF",
            [],
        ),
        (
            "IF vRegion = 'EU' THEN\nSET vF = 'lib://S:DataFiles/b.qvs';\nEND IF\n"
            "$(Include=$(vF));",
            [],
        ),
        (
            "SUB LoadFile(vF)\n$(Include=$(vF));\nEND SUB\n"
            "CALL LoadFile('lib://S:DataFiles/a.qvs');",
            [],
        ),
        # Set once in the script and again by the script it includes.
        (
            "SET vF = 'lib://S:DataFiles/a.qvs';\n"
            "$(Include=lib://S:DataFiles/setter.qvs);\n$(Include=$(vF));",
            [],
        ),
    ],
)
def test_include_path_from_a_varying_variable_is_not_read(
    script: str, loads: List[str]
) -> None:
    inlined = inline_includes(
        script, {"vF": "'lib://S:DataFiles/b.qvs'"}, _reader(_INCLUDED_LOADS)
    )
    parsed = parse_script(inlined)

    assert [load.expanded for load in parsed.file_loads] == loads
    assert parsed.has_includes == (not loads)


@pytest.mark.parametrize("varied_by_include", [False, True])
def test_only_read_errors_of_includes_the_final_script_reads_are_reported(
    varied_by_include: bool,
) -> None:
    files = {
        "lib://S:DataFiles/b.qvs": "SET vF = 'lib://S:DataFiles/a.qvs';\n"
        + ("SET vF = 'lib://S:DataFiles/b.qvs';\n" if varied_by_include else "")
        + "$(Include=lib://S:DataFiles/missing.qvs);"
    }
    unread: List[Tuple[str, str]] = []
    inline_includes(
        "$(Include=$(vF));",
        {"vF": "'lib://S:DataFiles/b.qvs'"},
        _reader(files),
        lambda path, reason: unread.append((path, reason)),
    )

    # Once b.qvs varies vF, the script is inlined again without reading b.qvs,
    # so the include b.qvs holds is never needed.
    assert [path for path, _ in unread] == (
        [] if varied_by_include else ["lib://S:DataFiles/missing.qvs"]
    )


def test_app_reports_no_read_error_of_an_include_its_script_does_not_read() -> None:
    api = _include_api()
    api.spaces = {"space-1": "Sales Space"}
    engine = _engine(
        {
            "qContent": "SET vF = 'lib://S:DataFiles/a.qvs';\n"
            "SET vF = 'lib://S:DataFiles/b.qvs';\n"
            "$(Include=lib://S:DataFiles/missing.qvs);"
        },
        EngineError({"code": 5, "message": "Access denied"}),
    )
    app = App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            "variables": {"vF": "'lib://S:DataFiles/b.qvs'"},
        }
    )

    api._inline_includes(engine, "app-1", app, "$(Include=$(vF));")

    assert not api.include_read_failures


@pytest.mark.parametrize("fan_out", [1, 2, 4])
def test_self_including_script_is_read_once(fan_out: int) -> None:
    path = "lib://S:DataFiles/self.qvs"
    body = "X: LOAD * FROM [lib://S:Lake/x.csv];\n"
    files = {path: body + f"$(Include={path});\n" * fan_out}
    unread: List[Tuple[str, str]] = []
    inlined = inline_includes(
        f"$(Include={path});",
        {},
        _reader(files),
        lambda path, reason: unread.append((path, reason)),
    )
    parsed = parse_script(inlined)

    assert len(inlined) < 2 * len(files[path])
    assert len(parsed.file_loads) == 1
    assert parsed.has_includes
    assert [p for p, _ in unread] == [path]


def test_mutually_including_scripts_are_each_read_once() -> None:
    a, b = "lib://S:DataFiles/a.qvs", "lib://S:DataFiles/b.qvs"
    files = {
        a: "A: LOAD * FROM [lib://S:Lake/a.csv];\n" + f"$(Include={b});\n" * 3,
        b: "B: LOAD * FROM [lib://S:Lake/b.csv];\n" + f"$(Include={a});\n" * 3,
    }
    unread: List[Tuple[str, str]] = []
    inlined = inline_includes(
        f"$(Include={a});",
        {},
        _reader(files),
        lambda path, reason: unread.append((path, reason)),
    )
    parsed = parse_script(inlined)

    assert len(inlined) < 10 * (len(files[a]) + len(files[b]))
    assert {load.expanded for load in parsed.file_loads} == {
        "lib://S:Lake/a.csv",
        "lib://S:Lake/b.csv",
    }
    assert parsed.has_includes
    assert [p for p, _ in unread] == [a]


def test_includes_past_the_size_limit_are_not_read() -> None:
    # Each level includes the next twice, so the script would double each level.
    files = {
        f"lib://S:DataFiles/{level}.qvs": "X" * 400_000
        + ";\n"
        + f"$(Include=lib://S:DataFiles/{level + 1}.qvs);\n" * 2
        for level in range(10)
    }
    unread: List[Tuple[str, str]] = []
    inlined = inline_includes(
        "$(Include=lib://S:DataFiles/0.qvs);",
        {},
        _reader(files),
        lambda path, reason: unread.append((path, reason)),
    )

    assert len(inlined) < 6_000_000
    assert unread
    assert parse_script(inlined).has_includes


def test_apostrophe_in_an_include_path_keeps_later_comments_out() -> None:
    files = {
        "lib://S:DataFiles/O'B.qvs": "A: LOAD * FROM [lib://S:Lake/a.csv];",
        "lib://S:DataFiles/old.qvs": "B: LOAD * FROM [lib://S:Lake/b.csv];",
    }
    script = (
        "$(Include=lib://S:DataFiles/O'B.qvs);\n"
        "// $(Include=lib://S:DataFiles/old.qvs);\n"
    )
    parsed = parse_script(inline_includes(script, {}, _reader(files)))

    assert [load.expanded for load in parsed.file_loads] == ["lib://S:Lake/a.csv"]


def test_unread_include_with_an_apostrophe_keeps_the_next_statement() -> None:
    parsed = parse_script(
        "$(Include=lib://S:DataFiles/O'B.qvs);\n"
        "Orders: LOAD * FROM [lib://S:Lake/orders.csv];\n"
        "// Old: LOAD * FROM [lib://S:Lake/old.csv];"
    )

    assert [load.expanded for load in parsed.file_loads] == ["lib://S:Lake/orders.csv"]
    assert parsed.has_includes


@pytest.mark.parametrize(
    "commented",
    [
        "// $(Include=lib://Common:DataFiles/loads.qvs);",
        "REM $(Include=lib://Common:DataFiles/loads.qvs);",
        "/* $(Include=lib://Common:DataFiles/loads.qvs); */",
    ],
)
def test_commented_include_is_not_read_into_the_script(commented: str) -> None:
    files = {"lib://Common:DataFiles/loads.qvs": "Rates: LOAD * FROM [lib://L/r.csv];"}
    parsed = parse_script(inline_includes(commented, {}, _reader(files)))
    assert not parsed.file_loads


def test_rem_after_an_include_without_semicolon_is_a_comment() -> None:
    files = {
        "lib://Common:DataFiles/a.qvs": "A: LOAD * FROM [lib://L/a.csv];",
        "lib://Common:DataFiles/b.qvs": "SET x = 1;\nB: LOAD * FROM [lib://L/b.csv];",
    }
    parsed = parse_script(
        inline_includes(
            "$(Include=lib://Common:DataFiles/a.qvs)\n"
            "REM $(Include=lib://Common:DataFiles/b.qvs);",
            {},
            _reader(files),
        )
    )
    assert [load.expanded for load in parsed.file_loads] == ["lib://L/a.csv"]


def test_include_path_from_a_constructed_varying_variable_is_not_read() -> None:
    # `$(v$(vSuffix))` reads `vF` once `vSuffix` expands, and the script sets
    # `vF` in a branch, so the reload's value need not be the one left.
    script = (
        "IF vRegion = 'EU' THEN\nSET vF = 'lib://S:DataFiles/a.qvs';\nEND IF\n"
        "$(Include=$(v$(vSuffix)));"
    )
    inlined = inline_includes(
        script,
        {"vSuffix": "F", "vF": "'lib://S:DataFiles/b.qvs'"},
        _reader(_INCLUDED_LOADS),
    )
    parsed = parse_script(inlined)

    assert parsed.file_loads == []
    assert parsed.has_includes


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
    _store_lineage(
        source,
        _store_app([]).model_copy(update={"id": "app-2"}),
        parse_script("STORE Orders INTO [lib://Lake/qvd/orders.qvd] (qvd);"),
        {stored: []},
    )

    assert (
        source._file_source_resolution(
            "app-1", _file_qri("adls", "lake", "QVD/Orders.qvd")
        ).urn
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

    assert source._file_source_resolution(
        "app-1", _file_qri("sftp", "ftp", "x.csv")
    ).urn == make_dataset_urn("qlik-sense", "sales space:ftp/x.csv")
    assert not source.reporter.unresolved_file_connections


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_store_into_a_visible_connector_without_storage_joins_its_reader(
    mock_api_cls: MagicMock,
) -> None:
    # Such as SFTP: listed, but no storage `file_connection_to_storage` maps.
    source = _source(
        {},
        {"Sales Space:Ftp": DataConnectionInfo(name="Ftp")},
        script="X: LOAD * FROM [lib://Sales Space:Ftp/qvd/orders.qvd] (qvd);",
    )
    stored = make_dataset_urn("qlik-sense", "app-2.orders")
    _store_lineage(
        source,
        _store_app([]).model_copy(update={"id": "app-2"}),
        parse_script("STORE Orders INTO [lib://Ftp/qvd/orders.qvd] (qvd);"),
        {stored: []},
    )
    file_urn = make_dataset_urn("qlik-sense", "sales space:ftp/qvd/orders.qvd")

    assert (
        source._file_source_resolution(
            "app-1", _file_qri("sftp", "ftp", "qvd/orders.qvd")
        ).urn
        == file_urn
    )
    lineage = [
        wu.metadata
        for wu in source._gen_unmapped_file_entities()
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == UpstreamLineageClass.ASPECT_NAME
    ]
    assert [mcp.entityUrn for mcp in lineage] == [file_urn]
    assert stored in str(lineage[0].aspect)
    assert not source.reporter.unresolved_file_connections


@pytest.mark.parametrize(
    "connection, key",
    [
        ("[Team: Sales]:Lake", "Team: Sales:Lake"),
        ("[Team: Sales]:[Lake]", "Team: Sales:Lake"),
        ("[Lake: Raw]", "Sales Space:Lake: Raw"),
        ("Sales Space:[Lake: Raw]", "Sales Space:Lake: Raw"),
    ],
)
@pytest.mark.parametrize("source_of_storage", ["visible", "mapped"])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_bracketed_connection_finds_its_storage(
    mock_api_cls: MagicMock, connection: str, key: str, source_of_storage: str
) -> None:
    if source_of_storage == "mapped":
        source = _source(
            {key: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}}, {}
        )
    else:
        source = _source(
            {},
            {
                key: DataConnectionInfo(
                    name=key,
                    file_platform=FilePlatform.ABS,
                    file_storage=FileStorage(platform="abs", location="lake"),
                )
            },
        )

    storage, _ = source._file_storage(connection, "space-1")

    assert storage is not None and storage.platform == FilePlatform.ABS


@pytest.mark.parametrize(
    "key, matches",
    [
        ("Finance:Sales SQL", True),
        # A bare key is the connection in any space.
        ("Sales SQL", True),
        ("Other:Sales SQL", False),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_mapping_matches_a_qualified_connection(
    mock_api_cls: MagicMock, key: str, matches: bool
) -> None:
    source = _source({}, {})

    entry = source._mapped_connection("Finance:Sales SQL", "space-1", {key: "entry"})

    assert (entry == "entry") is matches


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
        source._file_source_resolution(
            reader.id, _file_qri(qri_scheme, "unknown", "qvd/orders.qvd")
        ).urn
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
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
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
        ops = _upstream_ops(_app_and_end_of_run(source, app))

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


@pytest.mark.parametrize(
    "other_app_seen",
    ["this run", "later this run", "last run", "tables unknown this run"],
)
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
        workunits = list(source._gen_app_workunit(app))
        # An app read after this one, or one whose tables can't all be named.
        if other_app_seen == "later this run":
            source._table_urns_by_app["app-2"] = {CUSTOMERS}
        elif other_app_seen == "tables unknown this run":
            source._apps_tables_unknown.add("app-2")
        workunits.extend(source._gen_pending_table_lineage())
    ops = _upstream_ops(workunits)

    # Another app loading the same table may be what stores it into the old
    # file, so its edge stays.
    assert not [op for op in ops.get(DEV_CUSTOMERS, []) if op.startswith("remove")]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_store_of_an_ambiguous_table_keeps_the_edges_into_its_file(
    mock_api_cls: MagicMock,
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="STORE Customers INTO [lib://Sales Space:Lake/dev/customers.qvd];",
        tables=[
            _model_table("Customers", "AllCustomers", CUSTOMERS_QRI),
            _model_table(
                "CustomersArchive",
                "Customers",
                "qri:db:sqlserver://sales#dbo.CustomersArchive",
            ),
        ],
    )
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=CUSTOMERS, type=DatasetLineageTypeClass.COPY)]
    )
    source.ctx.graph = graph
    previous = AppCheckpoint(urns=[CUSTOMERS], stored_file_urns=[DEV_CUSTOMERS])

    with patch.object(QlikSenseSource, "_usable_checkpoint", return_value=previous):
        ops = _upstream_ops(_app_and_end_of_run(source, app))

    # The label names two tables, so either may be what the app still stores
    # into the file.
    assert DEV_CUSTOMERS not in ops
    assert source._app_stored_files[app.id] == [DEV_CUSTOMERS]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_untrusted_last_checkpoints_keep_edges_into_stored_files(
    mock_api_cls: MagicMock,
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
    previous = AppCheckpoint(urns=[CUSTOMERS], stored_file_urns=[DEV_CUSTOMERS])

    with (
        patch.object(QlikSenseSource, "_usable_checkpoint", return_value=previous),
        patch.object(source.app_state, "previous_apps", return_value=None),
    ):
        ops = _upstream_ops(_app_and_end_of_run(source, app))

    # Another app the last run recorded may share the table and store it
    # into the old file.
    assert not [op for op in ops.get(DEV_CUSTOMERS, []) if op.startswith("remove")]


@pytest.mark.parametrize("script", ["denied", "not read"])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unread_script_keeps_the_edges_into_files_it_stored_into(
    mock_api_cls: MagicMock, script: str
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="",
    )
    app.script = None
    app.script_read_failed = script == "denied"
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=ORDERS, type=DatasetLineageTypeClass.COPY)]
    )
    source.ctx.graph = graph
    previous = AppCheckpoint(urns=[ORDERS], stored_file_urns=[DEV_ORDERS])

    with patch.object(QlikSenseSource, "_usable_checkpoint", return_value=previous):
        ops = _upstream_ops(_app_and_end_of_run(source, app))

    # Without the script its STOREs are unknown, so the app may still store
    # into the file.
    assert DEV_ORDERS not in ops
    assert source._app_stored_files[app.id] == [DEV_ORDERS]


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
        ops = _upstream_ops(_app_and_end_of_run(source, app))

    assert DEV_ORDERS not in ops
    # A later run that can read its lineage still removes the app's edge.
    assert source._app_stored_files[app.id] == [DEV_ORDERS, PROD_ORDERS]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_old_file_stays_in_the_checkpoint_when_no_edges_are_removed(
    mock_api_cls: MagicMock,
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="Orders:\nLOAD * FROM [lib://Sales Space:Lake/landing/orders.csv] "
        "(txt);\nSTORE Orders INTO [lib://Sales Space:Lake/prod/orders.qvd];\n",
    )
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=ORDERS, type=DatasetLineageTypeClass.COPY)]
    )
    source.ctx.graph = graph
    # Part of the run wasn't read, so another app may share the app's tables
    # and none of its edges are removed.
    source.qlik_api.unread_items = [MagicMock()]
    previous = AppCheckpoint(urns=[ORDERS], stored_file_urns=[DEV_ORDERS])

    with patch.object(QlikSenseSource, "_usable_checkpoint", return_value=previous):
        ops = _upstream_ops(_app_and_end_of_run(source, app))

    assert not any(op.startswith("remove") for op in ops.get(DEV_ORDERS, []))
    # A later run that may remove edges still knows about the old file.
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


@pytest.mark.parametrize(
    "path, store_format, column_edges_expected",
    [
        ("clean/orders.parquet", " (parquet)", 2),
        # The format, not the file name, tells what the file holds: a STORE
        # without one writes a QVD, which the storage sources don't read.
        ("clean/orders.qvd", " (parquet)", 2),
        ("clean/orders.parquet", "", 0),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stored_fields_get_column_lineage(
    mock_api_cls: MagicMock, path: str, store_format: str, column_edges_expected: int
) -> None:
    source, app = _graph_app_source(
        mock_api_cls,
        {CONNECTION: {"storage_uri": "abfss://lake@account.dfs.core.windows.net"}},
        [],
        script="Orders:\nLOAD * FROM [lib://Sales Space:Lake/landing/orders.csv] "
        "(txt);\nSTORE OrderId, Amount AS Total FROM Orders INTO "
        f"[lib://Sales Space:Lake/{path}]{store_format};\n",
    )

    ops = _upstream_ops(_app_and_end_of_run(source, app))[
        f"urn:li:dataset:(urn:li:dataPlatform:abs,lake/{path},PROD)"
    ]

    column_edges = [op for op in ops if op.startswith("add /fineGrainedLineages/")]
    assert len(column_edges) == column_edges_expected
    assert any(
        make_schema_field_urn(ORDERS, "Amount") in op and ",Total)" in op
        for op in column_edges
    ) == bool(column_edges_expected)
    assert source.reporter.file_stores_linked == 1
