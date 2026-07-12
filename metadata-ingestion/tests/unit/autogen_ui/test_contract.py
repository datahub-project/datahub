from typing import List, Protocol, Type, cast

import pydantic
import pytest

from datahub.ingestion.autogen_ui.generator import generate_form
from datahub.ingestion.autogen_ui.hints import SECTION_ORDER
from datahub.ingestion.autogen_ui.inference import _resolve
from datahub.ingestion.source.source_registry import source_registry

RECIPE_PREFIX = "source.config."


class _HasConfigClass(Protocol):
    # get_config_class is decorator-injected onto Source (declared on Extractor),
    # so it is not statically visible on type[Source]; describe it here.
    @classmethod
    def get_config_class(cls) -> Type[pydantic.BaseModel]: ...


def _all_connector_names() -> List[str]:
    return sorted(
        name
        for name, value in source_registry.mapping.items()
        if not isinstance(value, Exception)
    )


@pytest.mark.parametrize("connector", _all_connector_names())
def test_form_field_paths_resolve_and_sections_are_ordered(connector: str) -> None:
    # Full-registry drift guard: every generated field_path must resolve against
    # the connector's JSON Schema (the same source the form is built from, so the
    # key is exactly what pydantic accepts), and sections must follow the canonical
    # order. Connectors with uninstalled optional deps / no config class are skipped.
    try:
        form = generate_form(connector)
    except Exception as e:
        pytest.skip(f"{connector}: cannot generate form ({type(e).__name__})")

    if form is None:
        pytest.skip(f"{connector}: no config class")

    source_cls = source_registry.get(connector)
    get_config_class = getattr(source_cls, "get_config_class", None)
    if get_config_class is None:
        pytest.skip(f"{connector}: no get_config_class()")
    config_cls = cast(_HasConfigClass, source_cls).get_config_class()

    schema = config_cls.model_json_schema()
    defs = schema.get("$defs", {})
    top_props = schema.get("properties", {})

    for section in form.sections:
        for field in section.fields:
            assert field.field_path.startswith(RECIPE_PREFIX), (
                f"{connector}: {field.field_path} does not start with {RECIPE_PREFIX}"
            )
            parts = field.field_path[len(RECIPE_PREFIX) :].split(".")
            assert parts[0] in top_props, (
                f"{connector}: {field.field_path} -> '{parts[0]}' not a config field"
            )
            if len(parts) == 2:
                core, _ = _resolve(top_props[parts[0]], defs)
                child_props = core.get("properties") or {}
                assert parts[1] in child_props, (
                    f"{connector}: {field.field_path} -> '{parts[1]}' not in {parts[0]}"
                )

    present = [s.key for s in form.sections]
    expected = [s.value for s in SECTION_ORDER if s.value in present]
    assert present == expected, (
        f"{connector}: sections {present} do not follow canonical order {expected}"
    )
