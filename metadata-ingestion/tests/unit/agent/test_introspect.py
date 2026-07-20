from typing import Optional

import pytest
from pydantic import SecretStr

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.agent.introspect import _classify, describe_source
from datahub.ingestion.agent.models import FieldKind, FieldSpec


class _Nested(ConfigModel):
    inner: str = "x"


class _SampleConfig(ConfigModel):
    host_port: str
    password: SecretStr
    token: Optional[SecretStr] = None
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    nested: _Nested = _Nested()


def _spec_for(name: str) -> FieldSpec:
    field_info = _SampleConfig.model_fields[name]
    return _classify(name, field_info)


def test_classify_secret():
    assert _spec_for("password").kind == FieldKind.SECRET
    # Optional[SecretStr] is still a secret.
    assert _spec_for("token").kind == FieldKind.SECRET


def test_classify_pattern():
    assert _spec_for("table_pattern").kind == FieldKind.PATTERN


def test_classify_nested():
    assert _spec_for("nested").kind == FieldKind.NESTED


def test_classify_plain_and_required():
    spec = _spec_for("host_port")
    assert spec.kind == FieldKind.PLAIN
    assert spec.required is True


def test_secret_default_is_not_leaked():
    # secret fields must not expose a default value in output
    assert _spec_for("password").default is None


def test_describe_source_snowflake():
    # Layer A needs no connection. Skip if the snowflake extra is not installed.
    pytest.importorskip("snowflake.connector")
    spec = describe_source("snowflake")
    by_kind = {f.name: f.kind for f in spec.fields}
    # schema_pattern is an AllowDenyPattern on the snowflake config.
    assert FieldKind.PATTERN in by_kind.values()
    assert FieldKind.SECRET in by_kind.values()
    assert spec.source_type == "snowflake"


def test_describe_source_unknown_raises():
    # source_registry.get() raises KeyError/ConfigurationError on miss.
    with pytest.raises(Exception) as exc_info:
        describe_source("definitely-not-a-source")
    assert exc_info.value is not None
