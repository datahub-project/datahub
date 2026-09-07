"""The hook that lets a connector finish the probe's engine.

The probe builds its own engine instead of constructing the connector's Source,
which fires ingestion telemetry and wants a PipelineContext. That keeps it cheap
and side-effect free, and it also means the probe skips whatever the Source does
to its engine afterwards. For most dialects that is nothing. Athena replaces the
dialect outright, so on the stock one the probe would answer differently from the
ingestion it exists to predict.
"""

from typing import Any, List

from datahub.ingestion.source.sql.sql_config import SQLCommonConfig


class _PlainConfig(SQLCommonConfig):
    def get_sql_alchemy_url(self) -> str:
        return "postgresql://u:p@h/db"

    @property
    def db(self) -> str:
        return "db"


def test_the_default_hook_leaves_the_engine_alone():
    """Most dialects need nothing, so the base must not require an override."""

    class _Engine:
        dialect = "untouched"

    engine = _Engine()
    _PlainConfig().probe_prepare_engine(engine)
    assert engine.dialect == "untouched"


def test_for_config_calls_the_hook_on_the_engine_it_built(monkeypatch):
    """The wiring itself: a connector that overrides the hook must see the
    provider's own engine, or the override protects nothing."""
    import sqlalchemy

    from datahub.ingestion.source.sql import sqlalchemy_probe
    from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe

    prepared: List[object] = []

    class _Engine:
        dialect = type("D", (), {"name": "postgresql"})()

        def dispose(self) -> None:
            pass

    class _Config(_PlainConfig):
        def probe_prepare_engine(self, engine: Any) -> None:
            prepared.append(engine)

    engine = _Engine()
    # create_engine is imported lazily inside for_config, so it is patched on
    # sqlalchemy; inspect is bound at module import, so it is patched there.
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda url, **kw: engine)
    monkeypatch.setattr(sqlalchemy_probe, "inspect", lambda target: object())

    SqlAlchemyMetadataProbe.for_config(_Config())

    assert prepared == [engine], "the hook did not receive the provider's engine"


def test_athena_substitutes_the_dialect_its_source_uses():
    """PyAthena's own dialect omits ICEBERG from get_table_names and mis-parses
    complex column types, which is why AthenaSource.get_inspectors replaces it.
    The probe must make the same substitution or report a different catalog."""
    from datahub.ingestion.source.sql.athena import (
        AthenaConfig,
        CustomAthenaRestDialect,
    )

    class _Engine:
        dialect: Any = "stock"

    engine = _Engine()
    config = AthenaConfig.parse_obj(
        {
            "aws_region": "us-east-1",
            "query_result_location": "s3://bucket/prefix/",
            "work_group": "primary",
        }
    )
    config.probe_prepare_engine(engine)
    assert isinstance(engine.dialect, CustomAthenaRestDialect)


def test_an_unreadable_athena_schema_fails_instead_of_looking_empty():
    """The dialect's S3 Tables fallback logs, warns and returns an empty list.

    That is right for ingestion, which emits what it can and reports the gap.
    A probe has nowhere to record the gap, and its own comment says what the
    silence costs: missing IAM permissions and expired credentials look
    identical to an empty schema. Reporting "no tables" when the truth is
    "could not read" is the confusion this interface exists to prevent, so on
    the probe path the warning has to become the failure.
    """
    import pytest

    from datahub.ingestion.source.sql.athena import (
        AthenaConfig,
        AthenaProbeReadFailed,
    )

    class _Engine:
        dialect: Any = "stock"

    engine = _Engine()
    AthenaConfig.parse_obj(
        {
            "aws_region": "us-east-1",
            "query_result_location": "s3://bucket/prefix/",
            "work_group": "primary",
        }
    ).probe_prepare_engine(engine)

    # Exactly the call the fallback's except-branch makes.
    with pytest.raises(AthenaProbeReadFailed, match="catalog=c"):
        engine.dialect._report.warning(
            message="Failed to list S3 Tables via boto3 fallback.",
            context="catalog=c, schema=s",
            exc=RuntimeError("AccessDenied"),
        )
    # Not a ValueError: nothing is wrong with the caller's arguments, the source
    # could not be read, so it must exit 3 rather than 2.
    assert not issubclass(AthenaProbeReadFailed, ValueError)
