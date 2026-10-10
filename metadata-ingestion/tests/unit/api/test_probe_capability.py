import logging
from typing import List

import pytest

from datahub.configuration.common import ConfigModel
from datahub.ingestion.agent.introspect import describe_source
from datahub.ingestion.api.decorators import capability, config_class
from datahub.ingestion.api.source import Source, SourceCapability
from datahub.ingestion.source.sql.postgres.source import PostgresConfig, PostgresSource


def _capabilities(source_cls: type) -> List[SourceCapability]:
    assert issubclass(source_cls, Source)
    return [setting.capability for setting in source_cls.get_capabilities()]


def test_sql_source_lists_probe() -> None:
    probe = [
        setting
        for setting in PostgresSource.get_capabilities()
        if setting.capability == SourceCapability.PROBE
    ]
    assert len(probe) == 1
    assert probe[0].supported
    # Stable: the same answer on every call.
    assert _capabilities(PostgresSource) == _capabilities(PostgresSource)


def test_source_without_a_provider_does_not_list_probe() -> None:
    @config_class(ConfigModel)
    @capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
    class NoProbeSource(Source):
        pass

    assert _capabilities(NoProbeSource) == [SourceCapability.DESCRIPTIONS]


def test_explicit_unsupported_declaration_wins() -> None:
    @config_class(PostgresConfig)
    @capability(SourceCapability.PROBE, "Not offered here", supported=False)
    class HiddenProbeSource(Source):
        pass

    probe = [
        setting
        for setting in HiddenProbeSource.get_capabilities()
        if setting.capability == SourceCapability.PROBE
    ]
    assert len(probe) == 1
    assert not probe[0].supported


def test_failing_provider_lookup_leaves_probe_absent_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class BrokenProbeConfig(ConfigModel):
        @classmethod
        def probe_provider_class(cls) -> type:
            raise RuntimeError("provider failed to load")

    @config_class(BrokenProbeConfig)
    @capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
    class BrokenProbeSource(Source):
        pass

    with caplog.at_level(logging.WARNING):
        assert _capabilities(BrokenProbeSource) == [SourceCapability.DESCRIPTIONS]
    # A defective connector must not drop Probe silently: `probe run` reports
    # the same defect loudly.
    assert any(
        record.levelno == logging.WARNING and "BrokenProbeSource" in record.getMessage()
        for record in caplog.records
    )


def test_subclass_derives_probe_from_its_own_config() -> None:
    # Inherits PostgresSource's declarations, but not its provider.
    @config_class(ConfigModel)
    class NoProbeSubclass(PostgresSource):
        pass

    caps = _capabilities(NoProbeSubclass)
    assert SourceCapability.PROBE not in caps
    assert SourceCapability.PLATFORM_INSTANCE in caps


def test_undecorated_source_gets_probe_from_the_base_class() -> None:
    @config_class(PostgresConfig)
    class UndecoratedSource(Source):
        pass

    assert _capabilities(UndecoratedSource) == [SourceCapability.PROBE]


def test_describe_lists_probe_only_for_a_probeable_source() -> None:
    def names(source_type: str) -> List[object]:
        return [c["capability"] for c in describe_source(source_type).capabilities]

    assert "Probe" in names("postgres")
    assert "Probe" not in names("file")
