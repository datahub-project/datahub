from typing import Any, Dict, Type

import pydantic

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.autogen_ui.form_model import ConnectorForm
from datahub.ingestion.autogen_ui.inference import build_form


def _sections_by_key(form: ConnectorForm) -> Dict[str, bool]:
    return {section.key: section.expanded for section in form.sections}


class SmallConfig(pydantic.BaseModel):
    host: str = pydantic.Field(description="Host name.")
    table_pattern: AllowDenyPattern = pydantic.Field(
        default_factory=AllowDenyPattern, description="Table filter."
    )


def test_small_config_keeps_scope_expanded() -> None:
    form = build_form("small", "small", SmallConfig)
    expanded = _sections_by_key(form)
    assert expanded["connection"] is True
    assert expanded["scope"] is True


def _make_large_config() -> Type[pydantic.BaseModel]:
    # pydantic.create_model's own stub types field values as `Any | tuple[str, Any]`
    # pending PEP 747 TypeForm support, so the (annotation, Field(...)) tuples here
    # can't be typed more tightly than Any without fighting that stub.
    fields: Dict[str, Any] = {
        "host": (str, pydantic.Field(description="Host name.")),
        "table_pattern": (
            AllowDenyPattern,
            pydantic.Field(
                default_factory=AllowDenyPattern, description="Table filter."
            ),
        ),
        "include_lineage": (
            bool,
            pydantic.Field(default=True, description="Whether to extract lineage."),
        ),
    }
    for i in range(45):
        fields[f"extra_field_{i}"] = (
            str,
            pydantic.Field(default="", description=f"Extra padding field {i}."),
        )
    return pydantic.create_model("LargeConfig", **fields)


def test_large_total_property_count_collapses_non_connection_sections() -> None:
    config_cls = _make_large_config()
    form = build_form("large", "large", config_cls)
    assert form.total_properties > 40

    expanded = _sections_by_key(form)
    assert expanded["connection"] is True
    assert expanded["scope"] is False
    assert expanded["enrichment"] is False


def _make_large_scope_section_config() -> Type[pydantic.BaseModel]:
    fields: Dict[str, Any] = {
        "host": (str, pydantic.Field(description="Host name.")),
    }
    for i in range(9):
        fields[f"pattern_{i}_pattern"] = (
            AllowDenyPattern,
            pydantic.Field(
                default_factory=AllowDenyPattern, description=f"Filter {i}."
            ),
        )
    return pydantic.create_model("LargeScopeConfig", **fields)


def test_large_scope_section_collapses_even_with_small_total() -> None:
    config_cls = _make_large_scope_section_config()
    form = build_form("large_scope", "large_scope", config_cls)
    assert form.total_properties <= 40

    scope_section = next(s for s in form.sections if s.key == "scope")
    assert len(scope_section.fields) > 8

    expanded = _sections_by_key(form)
    assert expanded["connection"] is True
    assert expanded["scope"] is False


def _scope_config_with_n_patterns(n: int) -> Type[pydantic.BaseModel]:
    fields: Dict[str, Any] = {
        "host": (str, pydantic.Field(description="Host name.")),
    }
    for i in range(n):
        fields[f"pattern_{i}_pattern"] = (
            AllowDenyPattern,
            pydantic.Field(
                default_factory=AllowDenyPattern, description=f"Filter {i}."
            ),
        )
    return pydantic.create_model(f"ScopeConfig{n}", **fields)


def test_scope_at_section_boundary_stays_expanded() -> None:
    # Exactly _LARGE_SECTION_THRESHOLD (8) scope fields must NOT collapse: the rule
    # is strict `> 8`. This guards against a `>`->`>=` regression. Total (9) is well
    # under the large-config threshold, so only the per-section rule is in play.
    form = build_form("boundary", "boundary", _scope_config_with_n_patterns(8))
    assert form.total_properties <= 40
    scope_section = next(s for s in form.sections if s.key == "scope")
    assert len(scope_section.fields) == 8

    expanded = _sections_by_key(form)
    assert expanded["connection"] is True
    assert expanded["scope"] is True
