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
    # Constructed without a report, so the S3 Tables fallback must stay silent
    # rather than raising on a None report.
    assert engine.dialect._report is None
