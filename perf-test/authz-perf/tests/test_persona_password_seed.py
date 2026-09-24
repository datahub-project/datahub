from unittest.mock import patch

from lib.persona_password_seed import (
    derive_persona_password,
    is_local_gms_url,
    load_or_create_seed,
    resolve_persona_password,
)


def test_is_local_gms_url() -> None:
    assert is_local_gms_url("http://localhost:8080")
    assert not is_local_gms_url("https://dev04.acryl.io/gms")


def test_local_password_unchanged() -> None:
    assert resolve_persona_password("persona-admin", "http://localhost:8080") == "persona-admin"


@patch("lib.persona_password_seed.load_or_create_seed", return_value="fixed-seed")
@patch("lib.persona_password_seed.socket.gethostname", return_value="bench-host")
def test_derive_persona_password_deterministic(
    _mock_host: object, _mock_seed: object
) -> None:
    remote = "https://dev04.acryl.io/gms"
    first = derive_persona_password("persona-admin", remote)
    second = derive_persona_password("persona-admin", remote)
    assert first == second
    assert first != "persona-admin"
    assert len(first) >= 8


@patch("lib.persona_password_seed.load_or_create_seed", return_value="fixed-seed")
@patch("lib.persona_password_seed.socket.gethostname", return_value="bench-host")
def test_derive_persona_password_varies_by_host(
    _mock_host: object, _mock_seed: object
) -> None:
    a = derive_persona_password(
        "persona-admin", "https://dev04.acryl.io/gms"
    )
    b = derive_persona_password(
        "persona-admin", "https://dev06.acryl.io/gms"
    )
    assert a != b


def test_load_or_create_seed_from_env(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AUTHZ_PERF_PERSONA_PASSWORD_SEED", "from-env")
    assert load_or_create_seed() == "from-env"
