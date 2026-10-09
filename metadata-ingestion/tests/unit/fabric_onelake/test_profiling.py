"""Tests for Fabric SQL Analytics Endpoint profiling."""

import sys
import types
from unittest.mock import MagicMock

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fabric.onelake.profiling import (
    PROFILING_SQL_PLATFORM,
    FabricProfileTarget,
    emit_dataset_profiles,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.ingestion.source.profiling.config import ProfilingConfig
from datahub.metadata.schema_classes import DatasetProfileClass


def test_emit_dataset_profiles_uses_mssql_profiler_and_fabric_urn() -> None:
    report = FabricOneLakeSourceReport()
    target = FabricProfileTarget(
        schema_name="dbo",
        table_name="orders",
        dataset_name="ws-1.wh-1.dbo.orders",
    )
    profile = DatasetProfileClass(timestampMillis=1, rowCount=4)

    def _generate(requests, max_workers, platform=None, profiler_args=None):
        assert platform == PROFILING_SQL_PLATFORM
        assert max_workers == 2
        assert requests[0].batch_kwargs == {"schema": "dbo", "table": "orders"}
        assert requests[0].pretty_name == "dbo.orders"
        yield requests[0], profile

    profiler = MagicMock()
    profiler.generate_profiles.side_effect = _generate
    profiler_cls = MagicMock(return_value=profiler)
    fake_module = types.ModuleType(
        "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler"
    )
    fake_module.SQLAlchemyProfiler = profiler_cls  # type: ignore[attr-defined]
    engine = MagicMock()
    module_name = fake_module.__name__
    previous = sys.modules.get(module_name)
    sys.modules[module_name] = fake_module
    try:
        workunits = list(
            emit_dataset_profiles(
                engine=engine,
                report=report,
                profiling=ProfilingConfig(enabled=True, max_workers=2),
                profile_pattern=AllowDenyPattern.allow_all(),
                targets=[target],
                platform="fabric-onelake",
                env="PROD",
                platform_instance=None,
                field_path_transform=lambda name: name.lower(),
            )
        )
    finally:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous

    profiler_cls.assert_called_once()
    assert profiler_cls.call_args.kwargs["platform"] == PROFILING_SQL_PLATFORM
    assert profiler_cls.call_args.kwargs["conn"] is engine
    assert profiler_cls.call_args.kwargs["field_path_transform"]("CustomerId") == (
        "customerid"
    )
    assert len(workunits) == 1
    assert workunits[0].metadata.aspect.rowCount == 4
    assert workunits[0].metadata.entityUrn == (
        "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ws-1.wh-1.dbo.orders,PROD)"
    )
    assert report.entities_profiled == 1


def test_profile_pattern_skips_table_without_calling_profiler() -> None:
    report = FabricOneLakeSourceReport()
    target = FabricProfileTarget(
        schema_name="dbo",
        table_name="orders",
        dataset_name="ws-1.wh-1.dbo.orders",
    )

    workunits = list(
        emit_dataset_profiles(
            engine=MagicMock(),
            report=report,
            profiling=ProfilingConfig(enabled=True, report_dropped_profiles=True),
            profile_pattern=AllowDenyPattern(allow=["sales.customers"]),
            targets=[target],
            platform="fabric-onelake",
            env="PROD",
            platform_instance=None,
            field_path_transform=lambda name: name,
        )
    )

    assert workunits == []
    assert report.entities_profiled == 0
    assert report.filtered


def test_failed_profile_does_not_count_as_profiled() -> None:
    report = FabricOneLakeSourceReport()
    target = FabricProfileTarget(
        schema_name="dbo",
        table_name="orders",
        dataset_name="ws-1.wh-1.dbo.orders",
    )

    def _generate(requests, max_workers, platform=None, profiler_args=None):
        yield requests[0], None

    profiler = MagicMock()
    profiler.generate_profiles.side_effect = _generate
    profiler_cls = MagicMock(return_value=profiler)
    fake_module = types.ModuleType(
        "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler"
    )
    fake_module.SQLAlchemyProfiler = profiler_cls  # type: ignore[attr-defined]
    module_name = fake_module.__name__
    previous = sys.modules.get(module_name)
    sys.modules[module_name] = fake_module
    try:
        workunits = list(
            emit_dataset_profiles(
                engine=MagicMock(),
                report=report,
                profiling=ProfilingConfig(enabled=True),
                profile_pattern=AllowDenyPattern.allow_all(),
                targets=[target],
                platform="fabric-onelake",
                env="PROD",
                platform_instance=None,
                field_path_transform=lambda name: name,
            )
        )
    finally:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous

    assert workunits == []
    assert report.entities_profiled == 0
