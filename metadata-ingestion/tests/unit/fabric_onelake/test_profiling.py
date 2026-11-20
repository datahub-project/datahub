"""Tests for Fabric SQL Analytics Endpoint profiling."""

import sys
import types
from typing import Callable, Iterator, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.fabric.onelake.profiling import (
    PROFILING_SQL_PLATFORM,
    FabricProfileTarget,
    emit_dataset_profiles,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.ingestion.source.profiling.common import ProfilerRequest
from datahub.ingestion.source.profiling.config import ProfilingConfig
from datahub.metadata.schema_classes import DatasetProfileClass

ProfileResults = Iterator[Tuple[ProfilerRequest, Optional[DatasetProfileClass]]]
GenerateProfiles = Callable[..., ProfileResults]

_PROFILER_MODULE = "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler"


@pytest.fixture
def fake_profiler(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[GenerateProfiles], MagicMock]:
    """Install a fake SQLAlchemyProfiler whose generate_profiles is `generate`.

    emit_dataset_profiles imports the profiler lazily, so the fake goes in
    sys.modules; monkeypatch restores the real module after the test.
    """

    def install(generate: GenerateProfiles) -> MagicMock:
        profiler = MagicMock()
        profiler.generate_profiles.side_effect = generate
        profiler_cls = MagicMock(return_value=profiler)
        fake_module = types.ModuleType(_PROFILER_MODULE)
        fake_module.SQLAlchemyProfiler = profiler_cls  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, _PROFILER_MODULE, fake_module)
        return profiler_cls

    return install


def _target(table_name: str) -> FabricProfileTarget:
    return FabricProfileTarget(
        schema_name="dbo",
        table_name=table_name,
        dataset_name=f"ws-1.wh-1.dbo.{table_name}",
    )


def test_emit_dataset_profiles_uses_mssql_profiler_and_fabric_urn(
    fake_profiler: Callable[[GenerateProfiles], MagicMock],
) -> None:
    report = FabricOneLakeSourceReport()
    profile = DatasetProfileClass(timestampMillis=1, rowCount=4)

    def generate(
        requests: List[ProfilerRequest],
        max_workers: int,
        platform: Optional[str] = None,
        profiler_args: Optional[dict] = None,
    ) -> ProfileResults:
        assert platform == PROFILING_SQL_PLATFORM
        assert max_workers == 2
        assert requests[0].batch_kwargs == {"schema": "dbo", "table": "orders"}
        assert requests[0].pretty_name == "dbo.orders"
        yield requests[0], profile

    profiler_cls = fake_profiler(generate)
    engine = MagicMock()
    workunits = list(
        emit_dataset_profiles(
            engine=engine,
            report=report,
            profiling=ProfilingConfig(enabled=True, max_workers=2),
            profile_pattern=AllowDenyPattern.allow_all(),
            targets=[_target("orders")],
            platform="fabric-onelake",
            env="PROD",
            platform_instance=None,
            field_path_transform=lambda name: name.lower(),
        )
    )

    profiler_cls.assert_called_once()
    assert profiler_cls.call_args.kwargs["platform"] == PROFILING_SQL_PLATFORM
    assert profiler_cls.call_args.kwargs["conn"] is engine
    assert profiler_cls.call_args.kwargs["field_path_transform"]("CustomerId") == (
        "customerid"
    )
    assert len(workunits) == 1
    metadata = workunits[0].metadata
    assert isinstance(metadata, MetadataChangeProposalWrapper)
    assert isinstance(metadata.aspect, DatasetProfileClass)
    assert metadata.aspect.rowCount == 4
    assert metadata.entityUrn == (
        "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ws-1.wh-1.dbo.orders,PROD)"
    )
    assert report.entities_profiled == 1


def test_profile_pattern_skips_table_without_calling_profiler() -> None:
    report = FabricOneLakeSourceReport()

    workunits = list(
        emit_dataset_profiles(
            engine=MagicMock(),
            report=report,
            profiling=ProfilingConfig(enabled=True, report_dropped_profiles=True),
            profile_pattern=AllowDenyPattern(allow=["sales.customers"]),
            targets=[_target("orders")],
            platform="fabric-onelake",
            env="PROD",
            platform_instance=None,
            field_path_transform=lambda name: name,
        )
    )

    assert workunits == []
    assert report.entities_profiled == 0
    assert report.filtered


def test_failed_profile_does_not_count_as_profiled(
    fake_profiler: Callable[[GenerateProfiles], MagicMock],
) -> None:
    report = FabricOneLakeSourceReport()

    def generate(
        requests: List[ProfilerRequest],
        max_workers: int,
        platform: Optional[str] = None,
        profiler_args: Optional[dict] = None,
    ) -> ProfileResults:
        yield requests[0], None

    fake_profiler(generate)
    workunits = list(
        emit_dataset_profiles(
            engine=MagicMock(),
            report=report,
            profiling=ProfilingConfig(enabled=True),
            profile_pattern=AllowDenyPattern.allow_all(),
            targets=[_target("orders")],
            platform="fabric-onelake",
            env="PROD",
            platform_instance=None,
            field_path_transform=lambda name: name,
        )
    )

    assert workunits == []
    assert report.entities_profiled == 0


def test_profiler_failure_warns_and_keeps_earlier_profiles(
    fake_profiler: Callable[[GenerateProfiles], MagicMock],
) -> None:
    report = FabricOneLakeSourceReport()
    profile = DatasetProfileClass(timestampMillis=1, rowCount=4)

    def generate(
        requests: List[ProfilerRequest],
        max_workers: int,
        platform: Optional[str] = None,
        profiler_args: Optional[dict] = None,
    ) -> ProfileResults:
        yield requests[0], profile
        raise RuntimeError("query combiner failed")

    fake_profiler(generate)
    workunits = list(
        emit_dataset_profiles(
            engine=MagicMock(),
            report=report,
            profiling=ProfilingConfig(enabled=True),
            profile_pattern=AllowDenyPattern.allow_all(),
            targets=[_target("orders"), _target("customers")],
            platform="fabric-onelake",
            env="PROD",
            platform_instance=None,
            field_path_transform=lambda name: name,
        )
    )

    assert len(workunits) == 1
    assert report.entities_profiled == 1
    assert [w.title for w in report.warnings] == ["Profiling Failed"]
