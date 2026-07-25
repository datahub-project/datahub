from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.agent.probe import resolve_pattern_field
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


class _Cfg(ConfigModel):
    schema_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    view_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    topic_patterns: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    # A same-named field that is NOT a pattern — must never be resolved to.
    table_pattern: str = "not-an-allow-deny-pattern"


def test_resolves_by_convention():
    assert (
        resolve_pattern_field(_Cfg, DatasetContainerSubTypes.SCHEMA) == "schema_pattern"
    )
    assert resolve_pattern_field(_Cfg, DatasetSubTypes.VIEW) == "view_pattern"


def test_resolves_the_plural_form():
    assert resolve_pattern_field(_Cfg, DatasetSubTypes.TOPIC) == "topic_patterns"


def test_ignores_a_same_named_non_pattern_field():
    # `table_pattern: str` exists but is not an AllowDenyPattern.
    assert resolve_pattern_field(_Cfg, DatasetSubTypes.TABLE) is None


def test_returns_none_when_absent():
    assert resolve_pattern_field(_Cfg, DatasetContainerSubTypes.DATABASE) is None


def test_multiword_kind_collapses_to_underscores():
    class _K(ConfigModel):
        flink_job_pattern: AllowDenyPattern = Field(
            default=AllowDenyPattern.allow_all()
        )

    assert resolve_pattern_field(_K, "Flink Job") == "flink_job_pattern"
