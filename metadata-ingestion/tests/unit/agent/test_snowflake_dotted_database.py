from types import SimpleNamespace

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.probe import ClassifyContext, Verdict
from datahub.ingestion.source.snowflake import snowflake_probe


def test_dotted_database_reaches_is_schema_allowed(monkeypatch):
    """The parent database comes from parent_path, not from splitting the fqn —
    a quoted Snowflake database may itself contain a '.'."""
    captured = {}

    def fake_is_schema_allowed(pattern, schema, db, match_fully_qualified):
        captured["db"] = db
        return True

    monkeypatch.setattr(snowflake_probe, "is_schema_allowed", fake_is_schema_allowed)
    config = SimpleNamespace(
        schema_pattern=AllowDenyPattern.allow_all(), match_fully_qualified_names=True
    )
    ctx = ClassifyContext(
        config=config,
        name="PUBLIC",
        fqn="MY.DB.PUBLIC",
        pattern_field="schema_pattern",
        parent_path=("MY.DB",),
        warn=lambda message: None,
    )
    assert snowflake_probe._classify_schema(ctx) == Verdict.include()
    assert captured["db"] == "MY.DB"  # not "MY"
