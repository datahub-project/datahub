from pathlib import Path
from typing import Dict, List

import pytest

from datahub.cli import config_utils
from datahub.configuration.config_loader import resolve_env_variables
from datahub.ingestion.agent.secrets import (
    DatahubEnvResolver,
    EnvVarResolver,
    MappingResolver,
    SecretResolver,
    default_resolvers,
    resolve_config,
    resolve_config_collecting,
)


def test_resolves_env_ref(monkeypatch):
    monkeypatch.setenv("MY_PW", "s3cr3t")
    out = resolve_config({"password": "${MY_PW}"}, [EnvVarResolver()])
    assert out["password"] == "s3cr3t"


def test_literal_passes_through(monkeypatch):
    out = resolve_config({"password": "inline-literal"}, [EnvVarResolver()])
    assert out["password"] == "inline-literal"


def test_nested_and_list_refs(monkeypatch):
    monkeypatch.setenv("H", "host1")
    out = resolve_config(
        {"a": {"host": "${H}"}, "hosts": ["${H}", "plain"]},
        [EnvVarResolver()],
    )
    nested = out["a"]
    assert isinstance(nested, dict)
    assert nested["host"] == "host1"
    assert out["hosts"] == ["host1", "plain"]


def test_unresolved_ref_raises():
    with pytest.raises(ValueError):
        resolve_config({"password": "${NOPE_MISSING}"}, [EnvVarResolver()])


def test_an_unresolved_ref_named_like_an_expandvars_setting_is_still_named():
    """expandvars reads EXPANDVARS_RECOVER_NULL after a miss; a recipe's own
    EXPANDVARS_* reference is not that setting."""
    with pytest.raises(ValueError, match="EXPANDVARS_TOKEN"):
        resolve_config({"password": "${EXPANDVARS_TOKEN}"}, [MappingResolver({})])


def test_collecting_records_nested_ref(monkeypatch):
    monkeypatch.setenv("NESTED_PW", "nestedsecret")
    out = resolve_config_collecting(
        {"a": {"b": {"pw": "${NESTED_PW}"}}}, [EnvVarResolver()]
    )
    a = out.config["a"]
    assert isinstance(a, dict)
    b = a["b"]
    assert isinstance(b, dict)
    assert b["pw"] == "nestedsecret"
    assert "nestedsecret" in out.secret_values


def _isolated_home(monkeypatch: pytest.MonkeyPatch, home: Path) -> None:
    """The resolver reads the selected profile under HOME; never the real one."""
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("DATAHUB_PROFILE", raising=False)
    config_utils.reset_profile_cache()


def test_datahubenv_refs_resolve_through_the_gms_block(monkeypatch, tmp_path):
    """The file nests under `gms:`, so a flat top-level lookup found nothing.

    The DATAHUB_GMS_* names the CLI documents as env vars, which people carry
    into recipes out of habit, resolve through the dotted path they alias.
    """
    _isolated_home(monkeypatch, tmp_path)
    (tmp_path / ".datahubenv").write_text(
        "gms:\n  server: http://gms:8080\n  token: tok-abc\n"
    )
    resolver = DatahubEnvResolver()
    assert resolver.resolve("gms.server") == "http://gms:8080"
    assert resolver.resolve("DATAHUB_GMS_TOKEN") == "tok-abc"


def test_datahubenv_resolver_declines_what_it_cannot_supply(monkeypatch, tmp_path):
    """A miss must return None so the caller raises "unresolved ref" rather
    than substituting a subtree or a partial path."""
    _isolated_home(monkeypatch, tmp_path)
    (tmp_path / ".datahubenv").write_text("gms:\n  server: http://gms:8080\n")
    resolver = DatahubEnvResolver()
    assert resolver.resolve("gms") is None
    assert resolver.resolve("gms.server.deeper") is None
    assert resolver.resolve("nope") is None


_ENV = {"PROBE_T_FOO": "foo-val", "PROBE_T_BAR": "bar-val"}
_PIPED = {"PROBE_T_PIPED": "piped-val"}


@pytest.fixture
def _resolvers(monkeypatch: pytest.MonkeyPatch) -> List[SecretResolver]:
    """The probe's chain, minus ~/.datahubenv: what was piped in, then the
    environment."""
    for name, value in _ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("PROBE_T_UNSET", raising=False)
    return [MappingResolver(_PIPED), EnvVarResolver()]


def _ingest_environ() -> Dict[str, str]:
    """The same values as one environment, as `datahub ingest` would see them
    with the piped values exported."""
    return {**_ENV, **_PIPED}


@pytest.mark.parametrize(
    "value",
    [
        "${PROBE_T_FOO}",
        "$PROBE_T_FOO",
        "${PROBE_T_UNSET:-dflt}",
        "pre-${PROBE_T_FOO}-post",
        "pre-$PROBE_T_FOO",
        "$PROBE_T_UNSET",
        "$$PROBE_T_FOO",
        "${PROBE_T_UNSET:=dflt}",
        "${PROBE_T_PIPED}",
        {"block": {"pw": "${PROBE_T_FOO}"}, "hosts": ["$PROBE_T_BAR", "plain", 3]},
    ],
)
def test_a_reference_resolves_as_ingestion_resolves_it(
    _resolvers: List[SecretResolver], value: object
) -> None:
    config: Dict[str, object] = {"value": value}
    probe = resolve_config_collecting(config, _resolvers).config
    assert probe == resolve_env_variables(config, _ingest_environ())


def test_an_unset_reference_without_a_default_fails_by_name(
    _resolvers: List[SecretResolver],
) -> None:
    with pytest.raises(ValueError, match=r"\$\{PROBE_T_UNSET\}"):
        resolve_config_collecting({"value": "pre-${PROBE_T_UNSET}"}, _resolvers)


def test_the_unset_reference_is_named_not_an_earlier_defaulted_one(
    _resolvers: List[SecretResolver],
) -> None:
    # Named from the lookup that missed, not from expandvars' message text.
    with pytest.raises(ValueError, match=r"\$\{PROBE_T_UNSET\}"):
        resolve_config_collecting(
            {"value": "${PROBE_T_DEFAULTED:-x}-${PROBE_T_UNSET}"}, _resolvers
        )


def test_a_reference_expansion_cannot_parse_is_the_callers_input(
    _resolvers: List[SecretResolver],
) -> None:
    """Ingestion raises expandvars' own KeyError or SyntaxError; here it is a
    ValueError, so it exits as bad input rather than as a defect."""
    with pytest.raises(ValueError):
        resolve_config_collecting({"value": "${PROBE_T_UNSET:?say why}"}, _resolvers)


def test_only_what_a_resolver_supplied_is_collected(
    _resolvers: List[SecretResolver],
) -> None:
    """An inline default is recipe text, not a secret; a reference left
    unexpanded (not leading the value) consults nothing."""
    resolved = resolve_config_collecting(
        {
            "a": "${PROBE_T_FOO}",
            "b": "$PROBE_T_BAR",
            "c": "${PROBE_T_PIPED}",
            "d": "${PROBE_T_UNSET:-inline-default}",
            "e": "${PROBE_T_UNSET:=assigned-default}",
            "f": "pre-$PROBE_T_UNSET",
        },
        _resolvers,
    )
    assert resolved.secret_values == {"foo-val", "bar-val", "piped-val"}


@pytest.mark.parametrize(
    "value",
    [
        "${PROBE_T_FOO:0:3}",
        "${PROBE_T_FOO:3}",
        "${PROBE_T_FOO: -2}",
        "${#PROBE_T_FOO}",
        "pre-${PROBE_T_FOO:0:1}-post",
        "${PROBE_T_UNSET:-${PROBE_T_FOO:1}}",
        {"block": ["${PROBE_T_PIPED:2}"]},
    ],
)
def test_a_reference_to_part_of_a_value_is_refused_by_name(
    _resolvers: List[SecretResolver], value: object
) -> None:
    # Only whole resolved values are masked, so a slice would print in clear
    # and two slices rebuild the secret. Refused before anything resolves.
    with pytest.raises(ValueError, match=r"part of \$\{PROBE_T_") as refused:
        resolve_config_collecting({"value": value}, _resolvers)
    for secret in (*_ENV.values(), *_PIPED.values()):
        assert secret not in str(refused.value)


@pytest.mark.parametrize("value", ["${PROBE_T_FOO:+set}", "${PROBE_T_FOO:?needed}"])
def test_a_modifier_that_keeps_the_whole_value_or_recipe_text_resolves(
    _resolvers: List[SecretResolver], value: str
) -> None:
    config: Dict[str, object] = {"value": value}
    probe = resolve_config_collecting(config, _resolvers).config
    assert probe == resolve_env_variables(config, _ingest_environ())


def test_datahubenv_resolver_declines_on_an_unknown_profile(monkeypatch, tmp_path):
    _isolated_home(monkeypatch, tmp_path)
    monkeypatch.setenv("DATAHUB_PROFILE", "missing")
    assert DatahubEnvResolver().resolve("DATAHUB_GMS_URL") is None


def test_recipe_connection_refs_follow_a_selected_profile(monkeypatch, tmp_path):
    """Through the real chain: exported DATAHUB_GMS_* must not pair with (or
    override) a profile chosen by name."""
    from datahub.cli.config_utils import persist_raw_datahub_config, profile_path

    _isolated_home(monkeypatch, tmp_path)
    persist_raw_datahub_config(
        {"gms": {"server": "https://selected.example.com/gms"}},
        profile_path("selected"),
    )
    monkeypatch.setenv("DATAHUB_PROFILE", "selected")
    monkeypatch.setenv("DATAHUB_GMS_URL", "https://other.example.com/gms")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "other-instance-token")
    monkeypatch.setenv("MY_PW", "pw")
    recipe: Dict[str, object] = {"server": "${DATAHUB_GMS_URL}", "password": "${MY_PW}"}
    out = resolve_config(recipe, default_resolvers())
    assert out == {"server": "https://selected.example.com/gms", "password": "pw"}
    # The profile has no token; the exported one belongs to another instance.
    with pytest.raises(ValueError, match="DATAHUB_GMS_TOKEN"):
        resolve_config({"token": "${DATAHUB_GMS_TOKEN}"}, default_resolvers())
