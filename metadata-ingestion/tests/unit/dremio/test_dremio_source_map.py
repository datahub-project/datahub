from typing import List
from unittest.mock import MagicMock

from datahub.ingestion.source.dremio.dremio_config import DremioSourceMapping
from datahub.ingestion.source.dremio.dremio_entities import DremioSourceContainer
from datahub.ingestion.source.dremio.dremio_source import (
    DremioSourceMapEntry,
    build_dremio_source_map,
)


def test_build_source_map_simple():
    # write unit test
    config_mapping: List[DremioSourceMapping] = [
        DremioSourceMapping(source_name="source1", platform="S3", env="PROD"),
        DremioSourceMapping(source_name="source2", platform="redshift", env="DEV"),
    ]
    sources: List[DremioSourceContainer] = [
        DremioSourceContainer(
            container_name="source1",
            location_id="xxx",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="S3",
            root_path="/",
            database_name=None,
        ),
        DremioSourceContainer(
            container_name="source2",
            location_id="yyy",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="REDSHIFT",
            root_path="/",
            database_name="redshiftdb",
        ),
    ]
    source_map = build_dremio_source_map(sources, config_mapping)

    assert source_map == {
        "source1": DremioSourceMapEntry(
            source_name="source1",
            platform="S3",
            env="PROD",
            dremio_source_category="file_object_storage",
            root_path="/",
            database_name="",
        ),
        "source2": DremioSourceMapEntry(
            source_name="source2",
            platform="redshift",
            env="DEV",
            dremio_source_category="database",
            root_path="/",
            database_name="redshiftdb",
        ),
    }


def test_build_source_map_same_platform_multiple_sources():
    # write unit test
    config_mapping: List[DremioSourceMapping] = [
        DremioSourceMapping(source_name="source1", platform="S3", env="PROD"),
        DremioSourceMapping(source_name="source2", platform="redshift", env="DEV"),
        DremioSourceMapping(source_name="source2", platform="redshift", env="PROD"),
    ]
    sources: List[DremioSourceContainer] = [
        DremioSourceContainer(
            container_name="source1",
            location_id="xxx",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="S3",
            root_path="/",
            database_name=None,
        ),
        DremioSourceContainer(
            container_name="source2",
            location_id="yyy",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="REDSHIFT",
            root_path="/",
            database_name="redshiftdb",
        ),
        DremioSourceContainer(
            container_name="source3",
            location_id="tt",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="REDSHIFT",
            root_path="/",
            database_name="redshiftproddb",
        ),
        DremioSourceContainer(
            container_name="Source4",
            location_id="zz",
            path=[],
            api_operations=MagicMock(),  # type:ignore
            dremio_source_type="NEWSOURCE",
            root_path="/",
            database_name="somedb",
        ),
    ]
    source_map = build_dremio_source_map(sources, config_mapping)

    assert source_map == {
        "source1": DremioSourceMapEntry(
            source_name="source1",
            platform="S3",
            env="PROD",
            dremio_source_category="file_object_storage",
            root_path="/",
            database_name="",
        ),
        "source2": DremioSourceMapEntry(
            source_name="source2",
            platform="redshift",
            env="DEV",
            dremio_source_category="database",
            root_path="/",
            database_name="redshiftdb",
        ),
        "source3": DremioSourceMapEntry(
            source_name="source3",
            platform="redshift",
            env=None,
            dremio_source_category="database",
            root_path="/",
            database_name="redshiftproddb",
        ),
        "source4": DremioSourceMapEntry(
            source_name="Source4",
            platform="newsource",
            env=None,
            dremio_source_category="unknown",
            root_path="/",
            database_name="somedb",
        ),
    }
