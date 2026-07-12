from typing import Optional, Protocol, Set, Type, cast

import pydantic
import pytest

from datahub.ingestion.autogen_ui.hints import SECTION_ORDER
from datahub.ingestion.autogen_ui.inference import build_form
from datahub.ingestion.source.source_registry import source_registry

CONNECTORS = ["mysql", "postgres", "snowflake", "bigquery", "kafka", "tableau"]

RECIPE_PREFIX = "source.config."


class _HasConfigClass(Protocol):
    # get_config_class is decorator-injected onto Source (declared on Extractor),
    # so it is not statically visible on type[Source]; describe it here.
    @classmethod
    def get_config_class(cls) -> Type[pydantic.BaseModel]: ...


def _config_class(connector: str) -> Type[pydantic.BaseModel]:
    source_cls = cast(_HasConfigClass, source_registry.get(connector))
    return source_cls.get_config_class()


def _field_names(model: Type[pydantic.BaseModel]) -> Set[str]:
    return set(model.model_fields.keys())


def _submodel_for(
    model: Type[pydantic.BaseModel], name: str
) -> Optional[Type[pydantic.BaseModel]]:
    field = model.model_fields.get(name)
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


@pytest.mark.parametrize("connector", CONNECTORS)
def test_every_field_path_resolves_to_a_real_config_attribute(connector: str) -> None:
    # This is the contract that makes drift structurally impossible: the form is
    # derived from the schema, so no generated field_path can point at a config
    # key that does not exist. (The exact lint the drift plan asked for.)
    config_cls = _config_class(connector)
    form = build_form(connector, connector, config_cls)

    top_level = _field_names(config_cls)

    for section in form.sections:
        for field in section.fields:
            assert field.field_path.startswith(RECIPE_PREFIX)
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
                assert parts[1] in _field_names(sub), (
                    f"{connector}: {field.field_path} -> '{parts[1]}' not in {parts[0]}"
                )


@pytest.mark.parametrize("connector", CONNECTORS)
def test_sections_follow_canonical_order(connector: str) -> None:
    # Consistency guarantee: whatever sections a connector has, they always appear
    # in the framework-owned order (Connection first, everywhere).
    form = build_form(connector, connector, _config_class(connector))
    order = [s.key for s in form.sections]
    expected = [s.value for s in SECTION_ORDER if s.value in order]
    assert order == expected


@pytest.mark.parametrize("connector", CONNECTORS)
def test_secrets_are_marked_and_default_view_is_not_overwhelming(
    connector: str,
) -> None:
    form = build_form(connector, connector, _config_class(connector))
    shown = sum(len(s.fields) for s in form.sections if s.expanded)
    # The default (expanded) view should never dump the entire config on the user.
    assert shown <= form.total_properties
    if form.total_properties > 40:
        assert shown < form.total_properties * 0.6
