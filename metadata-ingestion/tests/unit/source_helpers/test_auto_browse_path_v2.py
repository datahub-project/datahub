from itertools import zip_longest
from typing import List, Iterable

from acryl_datahub_cloud.metadata.schema_classes import (
    ContainerClass,
    BrowsePathsV2Class,
    BrowsePathEntryClass,
)

from datahub.emitter.mcp_builder import SchemaKey, DatabaseKey
from datahub.ingestion.api.source_helpers import auto_browse_path_v2
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_utils import gen_schema_container


def test_auto_browse_path_v2_gen_containers_threaded():
    database_key = DatabaseKey(platform="snowflake", database="db")
    schema_keys = [
        SchemaKey(platform="snowflake", database="db", schema=f"schema_{i}")
        for i in range(10)
    ]

    wus_per_schema = [
        gen_schema_container(
            schema=key.db_schema,
            database=key.database,
            sub_types=[],
            database_container_key=database_key,
            schema_container_key=key,
        )
        for key in schema_keys
    ]
    for wu in auto_browse_path_v2(_iterate_wus_round_robin(wus_per_schema)):
        aspect = wu.get_aspect_of_type(BrowsePathsV2Class)
        if aspect:
            assert aspect.path == [
                BrowsePathEntryClass(
                    id=database_key.as_urn(), urn=database_key.as_urn()
                )
            ]


def _iterate_wus_round_robin(
    mcps_per_schema: List[Iterable[MetadataWorkUnit]],
) -> Iterable[MetadataWorkUnit]:
    # Simulate a potential ordering of MCPs when using thread pool to generate MCPs
    for wus in zip_longest(*mcps_per_schema, fillvalue=None):
        for wu in wus:
            if wu is not None:
                yield wu
