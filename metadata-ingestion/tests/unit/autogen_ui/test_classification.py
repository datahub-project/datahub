from typing import Protocol, Type, cast

import pydantic

from datahub.ingestion.autogen_ui.inference import build_form
from datahub.ingestion.source.source_registry import source_registry


class _HasConfigClass(Protocol):
    @classmethod
    def get_config_class(cls) -> Type[pydantic.BaseModel]: ...


def _redshift_connection_field_names() -> list:
    source_cls = cast(_HasConfigClass, source_registry.get("redshift"))
    form = build_form("redshift", "Redshift", source_cls.get_config_class())
    connection = next(s for s in form.sections if s.key == "connection")
    return [f.name for f in connection.fields]


def test_database_is_a_connection_target_not_a_scope_filter() -> None:
    # `database` selects which DB to connect to -> Connection, not Scope. (The
    # filter equivalent is `database_pattern`.) Regression guard for a
    # misclassification the Redshift old-vs-new comparison surfaced.
    names = _redshift_connection_field_names()
    assert "database" in names


def test_host_leads_credentials_in_connection_order() -> None:
    # Natural reading order: the endpoint comes before the credentials, without
    # needing a per-connector ui_order hint.
    names = _redshift_connection_field_names()
    assert names.index("host_port") < names.index("username")
    assert names.index("host_port") < names.index("password")
