import pydantic

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.autogen_ui.hints import UISection, ui
from datahub.ingestion.autogen_ui.inference import build_form


def _field(form, name):
    return next(f for s in form.sections for f in s.fields if f.name == name)


class _WithIncludeToggle(pydantic.BaseModel):
    include_views: bool = pydantic.Field(default=True, description="Ingest views.")
    view_pattern: AllowDenyPattern = pydantic.Field(
        default_factory=AllowDenyPattern, description="Views to filter."
    )
    table_pattern: AllowDenyPattern = pydantic.Field(
        default_factory=AllowDenyPattern, description="Tables to filter."
    )


def test_pattern_auto_depends_on_sibling_include_toggle():
    form = build_form("d", "d", _WithIncludeToggle)
    vp = _field(form, "view_pattern")
    assert vp.depends_on == "include_views"
    assert vp.enabled_when is True
    # No sibling include_tables here -> no auto dependency.
    assert _field(form, "table_pattern").depends_on is None


class _ExplicitHints(pydantic.BaseModel):
    host: str = pydantic.Field(
        description="Host.",
        json_schema_extra=ui(placeholder="db.example.com:5439"),
    )
    mode: str = pydantic.Field(
        default="a",
        description="Mode.",
        json_schema_extra=ui(section=UISection.CONNECTION),
    )
    detail: str = pydantic.Field(
        default="",
        description="Detail.",
        json_schema_extra=ui(depends_on="mode", enabled_when="advanced"),
    )


def test_explicit_placeholder_and_dependency_hints():
    form = build_form("d", "d", _ExplicitHints)
    assert _field(form, "host").placeholder == "db.example.com:5439"
    detail = _field(form, "detail")
    assert detail.depends_on == "mode"
    assert detail.enabled_when == "advanced"
