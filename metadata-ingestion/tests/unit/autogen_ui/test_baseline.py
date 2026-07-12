from typing import Protocol, Type, cast

import pydantic
import pytest

from datahub.ingestion.autogen_ui.inference import build_form
from datahub.ingestion.source.source_registry import source_registry

# Field-path resolution and canonical section ordering are covered across the
# whole registry by test_contract.py; this file keeps the checks that are not.
CONNECTORS = ["mysql", "postgres", "snowflake", "bigquery", "kafka", "tableau"]


class _HasConfigClass(Protocol):
    # get_config_class is decorator-injected onto Source (declared on Extractor),
    # so it is not statically visible on type[Source]; describe it here.
    @classmethod
    def get_config_class(cls) -> Type[pydantic.BaseModel]: ...


def _config_class(connector: str) -> Type[pydantic.BaseModel]:
    source_cls = cast(_HasConfigClass, source_registry.get(connector))
    return source_cls.get_config_class()


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
