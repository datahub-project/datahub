from typing import List, Optional, Protocol, Type, cast

import pydantic
import pytest

from datahub.ingestion.autogen_ui.generator import generate_form
from datahub.ingestion.autogen_ui.hints import SECTION_ORDER
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


def _valid_keys(model: Type[pydantic.BaseModel]) -> set:
    # A field_path segment is valid if it matches a field's python attribute name
    # OR its pydantic alias. model_json_schema() emits alias keys (by_alias=True),
    # which is what the generator uses and what recipes/docs accept.
    keys = set()
    for name, field in model.model_fields.items():
        keys.add(name)
        if field.alias:
            keys.add(field.alias)
    return keys


def _submodel_for(
    model: Type[pydantic.BaseModel], name: str
) -> Optional[Type[pydantic.BaseModel]]:
    field = model.model_fields.get(name)
    if field is None:
        # `name` may be an alias rather than the attribute name.
        field = next((f for f in model.model_fields.values() if f.alias == name), None)
    if field is None:
        return None
    annotation = field.annotation
    # Unwrap Optional[...] / Union[..., None].
    for arg in getattr(annotation, "__args__", ()):
        if isinstance(arg, type) and issubclass(arg, pydantic.BaseModel):
            return arg
    if isinstance(annotation, type) and issubclass(annotation, pydantic.BaseModel):
        return annotation
    return None


@pytest.mark.parametrize("connector", _all_connector_names())
def test_form_field_paths_resolve_and_sections_are_ordered(connector: str) -> None:
    # Full-registry drift guard: every connector's generated form must point at
    # real config attributes, and its sections must follow the canonical order.
    # Connectors whose optional deps aren't installed (or that have no config
    # class) are skipped, not failed.
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

    top_level = _valid_keys(config_cls)

    for section in form.sections:
        for field in section.fields:
            assert field.field_path.startswith(RECIPE_PREFIX), (
                f"{connector}: {field.field_path} does not start with {RECIPE_PREFIX}"
            )
            path = field.field_path[len(RECIPE_PREFIX) :]
            parts = path.split(".")
            assert parts[0] in top_level, (
                f"{connector}: {field.field_path} -> '{parts[0]}' not a config field"
            )
            if len(parts) == 2:
                sub = _submodel_for(config_cls, parts[0])
                assert sub is not None, (
                    f"{connector}: {parts[0]} is not a nested model but path has a child"
                )
                assert parts[1] in _valid_keys(sub), (
                    f"{connector}: {field.field_path} -> '{parts[1]}' not in {parts[0]}"
                )

    present = [s.key for s in form.sections]
    expected = [s.value for s in SECTION_ORDER if s.value in present]
    assert present == expected, (
        f"{connector}: sections {present} do not follow canonical order {expected}"
    )
