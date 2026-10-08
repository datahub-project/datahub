from typing import Dict, FrozenSet, List, Optional, Tuple

import pytest
from pydantic import Field, SecretStr

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.agent.introspect import (
    _classify,
    collect_secret_field_values,
    describe_source,
    iter_model_secret_values,
)
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
    assert spec.required


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


class _PEP604Config(ConfigModel):
    """The same three fields written with "X | None" instead of Optional[X].

    Both spellings mean one thing and report different typing origins
    (types.UnionType against typing.Union), so matching only the older one
    classifies these as plain fields -- a pattern that reports no filter, and a
    secret that never gets masked.
    """

    secret: SecretStr | None = None
    table_pattern: AllowDenyPattern | None = None
    nested: _Nested | None = None


@pytest.mark.parametrize(
    "name,expected",
    [
        ("secret", FieldKind.SECRET),
        ("table_pattern", FieldKind.PATTERN),
        ("nested", FieldKind.NESTED),
    ],
)
def test_the_newer_optional_syntax_classifies_the_same(name, expected):
    assert _classify(name, _PEP604Config.model_fields[name]).kind == expected


def test_describe_source_unknown_raises():
    # source_registry.get() raises KeyError/ConfigurationError on miss.
    with pytest.raises(Exception) as exc_info:
        describe_source("definitely-not-a-source")
    assert exc_info.value is not None


class _Endpoint(ConfigModel):
    url: str
    signing_material: SecretStr = Field(alias="signingMaterial")


class _FleetConfig(ConfigModel):
    deploy_key: SecretStr
    endpoints: List[_Endpoint] = []


def test_secret_fields_are_read_in_lists_of_blocks_and_under_their_alias():
    config: Dict[str, object] = {
        "deploy_key": "top-level-value",
        "endpoints": [
            {"url": "https://a.example", "signingMaterial": "first-material"},
            {"url": "https://b.example", "signingMaterial": "second-material"},
        ],
    }
    assert collect_secret_field_values(_FleetConfig, config) == {
        "top-level-value",
        "first-material",
        "second-material",
    }


class _Keyring(ConfigModel):
    tokens: Tuple[SecretStr, ...] = ()
    keys: FrozenSet[SecretStr] = frozenset()


def test_the_recipe_walk_reads_tuple_and_set_values():
    config: Dict[str, object] = {
        "tokens": ("first-token", "second-token"),
        "keys": {"only-key"},
    }
    assert collect_secret_field_values(_Keyring, config) == {
        "first-token",
        "second-token",
        "only-key",
    }


class _GitBlock(ConfigModel):
    repo: str
    deploy_key: Optional[SecretStr] = None


class _RenamingConfig(ConfigModel):
    git_info: Optional[_GitBlock] = None
    mirrors: Dict[str, _GitBlock] = {}
    fleet: List[_FleetConfig] = []

    _github_info = pydantic_renamed_field("github_info", "git_info")


def test_a_renamed_fields_secret_is_found_only_on_the_validated_config():
    raw: Dict[str, object] = {"github_info": {"repo": "o/r", "deploy_key": "old-key"}}
    assert collect_secret_field_values(_RenamingConfig, raw) == set()
    with pytest.warns(Warning, match="github_info is deprecated"):
        validated = _RenamingConfig.model_validate(raw)
    assert list(iter_model_secret_values(validated)) == [
        ("git_info.deploy_key", "old-key")
    ]


def test_the_validated_walk_reaches_secrets_at_any_depth():
    validated = _RenamingConfig.model_validate(
        {
            "mirrors": {"eu": {"repo": "o/eu", "deploy_key": "mirror-key"}},
            "fleet": [
                {
                    "deploy_key": "fleet-key",
                    "endpoints": [
                        {"url": "https://a.example", "signingMaterial": "material"}
                    ],
                }
            ],
        }
    )
    assert dict(iter_model_secret_values(validated)) == {
        "mirrors[eu].deploy_key": "mirror-key",
        "fleet[0].deploy_key": "fleet-key",
        "fleet[0].endpoints[0].signing_material": "material",
    }
