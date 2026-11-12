from typing import Dict, Optional
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import App
from datahub.ingestion.source.qlik_sense.file_lineage import (
    FileStorage,
    file_storage_of_connection,
    file_storage_of_uri,
    qri_part_id,
)
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import (
    DataConnectionInfo,
    parse_script,
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
    assert parse_script(SCRIPT).file_loads == [
        "lib://Sales Space:Lake//landing/orders.csv",
        "lib://Sales Space:Lake/ref/regions.xlsx",
        "lib://Lake/ref/targets.csv",
    ]


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
        ("https://example.com/sales", None),
    ],
)
def test_file_storage_of_uri(uri: str, expected: Optional[FileStorage]) -> None:
    assert file_storage_of_uri(uri) == expected


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
