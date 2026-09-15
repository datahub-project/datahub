"""Tests for Redshift profiling + container interaction when include_tables=False.

When profiling is enabled with include_tables=False, profiled datasets must
still get container and dataPlatformInstance aspects so they appear correctly
under their schema container in the UI hierarchy. (ING-2018)
"""

from typing import Dict, List, Optional
from unittest.mock import patch

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.profile import RedshiftProfiler
from datahub.ingestion.source.redshift.redshift_schema import RedshiftTable
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.sql.sql_utils import gen_schema_key
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInstanceClass,
)


def make_profiler(
    include_tables: bool = False,
    platform_instance: Optional[str] = None,
    profiling_enabled: bool = True,
) -> RedshiftProfiler:
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="test_db",
        include_tables=include_tables,
        platform_instance=platform_instance,
        profiling=GEProfilingConfig(enabled=profiling_enabled),
    )
    report = RedshiftReport()
    return RedshiftProfiler(config=config, report=report, state_handler=None)


def make_tables(
    db: str = "test_db", schema: str = "public"
) -> Dict[str, Dict[str, List[RedshiftTable]]]:
    return {
        db: {
            schema: [
                RedshiftTable(
                    name="test_table",
                    schema=schema,
                    columns=[],
                    created=None,
                    comment="",
                    type="BASE TABLE",
                    rows_count=100,
                    size_in_bytes=1024,
                ),
            ]
        }
    }


def collect_aspect_names(workunits: list[MetadataWorkUnit]) -> list[str]:
    return [
        wu.metadata.aspectName
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName is not None
    ]


def expected_dataset_urn(
    db: str = "test_db",
    schema: str = "public",
    table: str = "test_table",
    platform_instance: Optional[str] = None,
) -> str:
    # Mirrors RedshiftProfiler.get_dataset_name (lowercased) plus
    # make_dataset_urn_with_platform_instance used by
    # GenericProfiler.dataset_urn_builder. Use the production helper directly
    # so the test stays in sync if the URN format ever changes.
    return make_dataset_urn_with_platform_instance(
        platform="redshift",
        name=f"{db}.{schema}.{table}".lower(),
        platform_instance=platform_instance,
        env="PROD",
    )


def expected_schema_container_urn(
    db: str = "test_db",
    schema: str = "public",
    platform_instance: Optional[str] = None,
) -> str:
    return gen_schema_key(
        db_name=db,
        schema=schema,
        platform="redshift",
        platform_instance=platform_instance,
        env="PROD",
    ).as_urn()


def test_profiler_emits_container_aspect_when_include_tables_false():
    """When include_tables=False, the profiler should emit a container aspect
    on the profiled dataset URN, linking it to the correct schema container."""
    profiler = make_profiler(
        include_tables=False,
        platform_instance="my-instance",
    )
    tables = make_tables()

    # Patch generate_profile_workunits to avoid needing a real DB connection
    with patch.object(profiler, "generate_profile_workunits", return_value=iter([])):
        workunits = list(profiler.get_workunits(tables))

    expected_urn = expected_dataset_urn(platform_instance="my-instance")
    expected_container = expected_schema_container_urn(platform_instance="my-instance")
    container_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "container"
        and wu.metadata.entityUrn == expected_urn
    ]
    assert len(container_mcps) == 1, (
        f"Expected exactly one container aspect on {expected_urn}, "
        f"got {len(container_mcps)}"
    )
    container_aspect = container_mcps[0].aspect
    assert isinstance(container_aspect, ContainerClass)
    assert container_aspect.container == expected_container


def test_profiler_emits_dataplatforminstance_when_include_tables_false():
    """When include_tables=False with a platform_instance, profiled datasets
    should get a dataPlatformInstance aspect pointing at the configured
    instance, attached to the correct dataset URN."""
    profiler = make_profiler(
        include_tables=False,
        platform_instance="my-instance",
    )
    tables = make_tables()

    with patch.object(profiler, "generate_profile_workunits", return_value=iter([])):
        workunits = list(profiler.get_workunits(tables))

    expected_urn = expected_dataset_urn(platform_instance="my-instance")
    dpi_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "dataPlatformInstance"
        and wu.metadata.entityUrn == expected_urn
    ]
    assert len(dpi_mcps) == 1
    dpi_aspect = dpi_mcps[0].aspect
    assert isinstance(dpi_aspect, DataPlatformInstanceClass)
    assert dpi_aspect.platform == "urn:li:dataPlatform:redshift"
    assert dpi_aspect.instance == (
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:redshift,my-instance)"
    )


def test_profiler_does_not_emit_container_when_include_tables_true():
    """When include_tables=True, the profiler should NOT emit container aspects
    because gen_dataset_workunits already does that."""
    profiler = make_profiler(
        include_tables=True,
        platform_instance="my-instance",
    )
    tables = make_tables()

    with patch.object(profiler, "generate_profile_workunits", return_value=iter([])):
        workunits = list(profiler.get_workunits(tables))

    aspect_names = collect_aspect_names(workunits)
    assert "container" not in aspect_names, (
        "Profiler should not emit container aspect when include_tables=True"
    )
    assert "dataPlatformInstance" not in aspect_names, (
        "Profiler should not emit dataPlatformInstance aspect when include_tables=True"
    )


def test_profiler_container_links_to_correct_schema():
    """The container aspect should reference the URN derived from the
    profiled table's schema, not some other schema. Uses a non-default
    schema name ("seed") so a wrong schema (e.g. "public") would fail."""
    profiler = make_profiler(
        include_tables=False,
        platform_instance="my-instance",
    )
    tables = make_tables(db="test_db", schema="seed")

    with patch.object(profiler, "generate_profile_workunits", return_value=iter([])):
        workunits = list(profiler.get_workunits(tables))

    container_wus = [
        wu
        for wu in workunits
        if hasattr(wu.metadata, "aspectName") and wu.metadata.aspectName == "container"
    ]
    assert len(container_wus) == 1
    mcp = container_wus[0].metadata
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    container_aspect = mcp.aspect
    assert isinstance(container_aspect, ContainerClass)

    expected_seed_urn = expected_schema_container_urn(
        db="test_db", schema="seed", platform_instance="my-instance"
    )
    expected_public_urn = expected_schema_container_urn(
        db="test_db", schema="public", platform_instance="my-instance"
    )
    assert container_aspect.container == expected_seed_urn
    # Defensive: confirm the "seed" and "public" schema container URNs differ,
    # so the equality check above is meaningful.
    assert expected_seed_urn != expected_public_urn

    expected_dataset = expected_dataset_urn(
        db="test_db", schema="seed", platform_instance="my-instance"
    )
    assert mcp.entityUrn == expected_dataset


def test_profiler_emits_container_but_no_dpi_without_platform_instance():
    """Even without platform_instance, container aspects should be emitted
    on the dataset URN when include_tables=False, but no dataPlatformInstance
    aspect should be produced (DPI requires a configured instance)."""
    profiler = make_profiler(
        include_tables=False,
        platform_instance=None,
    )
    tables = make_tables()

    with patch.object(profiler, "generate_profile_workunits", return_value=iter([])):
        workunits = list(profiler.get_workunits(tables))

    expected_urn = expected_dataset_urn(platform_instance=None)
    expected_container = expected_schema_container_urn(platform_instance=None)
    container_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "container"
        and wu.metadata.entityUrn == expected_urn
    ]
    assert len(container_mcps) == 1
    container_aspect = container_mcps[0].aspect
    assert isinstance(container_aspect, ContainerClass)
    assert container_aspect.container == expected_container

    aspect_names = collect_aspect_names(workunits)
    assert "dataPlatformInstance" not in aspect_names


def test_profiler_does_not_emit_container_for_excluded_table():
    """Regression test: container/dataPlatformInstance MCPs must only be
    emitted for tables that are actually eligible for profiling. If a table
    is filtered out (e.g. by profile_pattern), no container or DPI MCPs
    should be emitted for it -- otherwise we create a "ghost" dataset
    entity in DataHub that is never profiled.

    This guards against a regression where the MCP emission was placed
    *before* the get_profile_request eligibility check.
    """
    profiler = make_profiler(
        include_tables=False,
        platform_instance="my-instance",
    )
    # Deny the only table via profile_pattern so get_profile_request returns None.
    profiler.config.profile_pattern = AllowDenyPattern(deny=[".*test_table.*"])
    tables = make_tables()
    # Set column_count > 0 explicitly so the get_profile_request path takes
    # the profile_pattern denial branch, not the "skip if column_count == 0"
    # branch. Without this, the test would still pass today (column_count
    # defaults to None and `None == 0` is False) but would silently start
    # asserting via the wrong code path if the default ever changed to 0.
    for db_tables in tables.values():
        for schema_tables in db_tables.values():
            for table in schema_tables:
                table.column_count = 5

    with patch.object(profiler, "generate_profile_workunits", return_value=iter([])):
        workunits = list(profiler.get_workunits(tables))

    excluded_urn = expected_dataset_urn(platform_instance="my-instance")
    ghost_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.entityUrn == excluded_urn
        and wu.metadata.aspectName in ("container", "dataPlatformInstance")
    ]
    assert ghost_mcps == [], (
        f"No container/dataPlatformInstance MCPs should be emitted for an "
        f"excluded table, but found: "
        f"{[(m.aspectName, m.entityUrn) for m in ghost_mcps]}"
    )
