"""The RDS IAM credential path, which had no test at all while it lived on the
Source in two copies.

It could not have one: the interesting behaviour is a `do_connect` listener that
injects a freshly-minted token, and reaching it meant either an RDS instance or
reimplementing the listener in the test. With one implementation on the config,
the listener is reachable from a plain config object.
"""

from typing import Any, Dict, List, Tuple

import pytest

from datahub.ingestion.source.sql.mysql import MySQLConfig
from datahub.ingestion.source.sql.postgres.source import PostgresConfig

_IAM = {"auth_mode": "AWS_IAM", "aws_config": {"aws_region": "us-west-2"}}


def _mysql(**kw: Any) -> MySQLConfig:
    return MySQLConfig.model_validate(
        {"host_port": "db.rds.amazonaws.com:3306", "username": "u", **kw}
    )


def _postgres(**kw: Any) -> PostgresConfig:
    return PostgresConfig.model_validate(
        {"host_port": "db.rds.amazonaws.com:5432", "username": "u", **kw}
    )


class _FakeManager:
    def __init__(self, **kwargs: Any) -> None:
        self.calls = 0
        # The real manager records these and the cache guard reads them back.
        self.endpoint = kwargs.get("endpoint")
        self.port = kwargs.get("port")
        self.username = kwargs.get("username")

    def get_token(self) -> str:
        self.calls += 1
        return f"token-{self.calls}"


def _recording_manager(built: List[Dict[str, Any]]) -> Any:
    """Stands in for RDSIAMTokenManager, recording the kwargs it was built with."""

    def make(**kwargs: Any) -> _FakeManager:
        built.append(kwargs)
        return _FakeManager(**kwargs)

    return make


def _capture(monkeypatch: pytest.MonkeyPatch) -> List[Tuple[Any, str, Any]]:
    """Capture what gets registered, so the listener can be invoked directly."""
    registered: List[Tuple[Any, str, Any]] = []
    monkeypatch.setattr(
        "datahub.ingestion.source.sql.rds_iam.event.listen",
        lambda target, name, fn, **kw: registered.append((target, name, fn)),
    )
    return registered


def _stub_manager(monkeypatch: pytest.MonkeyPatch) -> List[Dict[str, Any]]:
    """Replace the token manager class, never the cached instance.

    Seeding `_rds_iam_manager` directly is not enough: rds_iam_token_manager
    checks that the cache still describes this config, so an identity-less fake
    is rejected and the fall-through constructs the *real* manager -- which
    reaches STS. Stubbing the constructor makes that unreachable.
    """
    built: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        "datahub.ingestion.source.sql.rds_iam.RDSIAMTokenManager",
        _recording_manager(built),
    )
    return built


def _listener(config: Any, monkeypatch: pytest.MonkeyPatch) -> Tuple[Any, Any]:
    _stub_manager(monkeypatch)
    registered = _capture(monkeypatch)
    config.install_rds_iam_auth(object())
    assert [name for _, name, _ in registered] == ["do_connect"]
    return registered[0][2], config.rds_iam_token_manager()


@pytest.mark.parametrize("factory", [_mysql, _postgres], ids=["mysql", "postgres"])
def test_a_non_iam_recipe_gets_no_listener(factory, monkeypatch):
    """PASSWORD auth must be left completely alone -- install_rds_iam_auth is
    called unconditionally by the Source and by the probe."""
    registered = _capture(monkeypatch)
    factory().install_rds_iam_auth(object())
    assert registered == []


@pytest.mark.parametrize("factory", [_mysql, _postgres], ids=["mysql", "postgres"])
def test_the_listener_injects_a_token_as_the_password(factory, monkeypatch):
    listener, manager = _listener(factory(**_IAM), monkeypatch)
    cparams: Dict[str, Any] = {}
    listener(None, None, None, cparams)
    assert cparams["password"] == "token-1"

    # Per connection, not once: a run outliving the token's expiry re-authenticates
    # because get_token() is consulted again rather than a value being captured.
    listener(None, None, None, cparams)
    assert cparams["password"] == "token-2"
    assert manager.calls == 2


def test_mysql_requires_tls_the_way_pymysql_wants_it(monkeypatch):
    listener, _ = _listener(_mysql(**_IAM), monkeypatch)
    cparams: Dict[str, Any] = {}
    listener(None, None, None, cparams)
    assert cparams["ssl"] == {"ssl": True}


def test_mysql_leaves_a_configured_ssl_block_alone(monkeypatch):
    listener, _ = _listener(_mysql(**_IAM), monkeypatch)
    cparams: Dict[str, Any] = {"ssl": {"ca": "/etc/ca.pem"}}
    listener(None, None, None, cparams)
    assert cparams["ssl"] == {"ca": "/etc/ca.pem"}


def test_postgres_requires_tls_the_way_psycopg2_wants_it(monkeypatch):
    listener, _ = _listener(_postgres(**_IAM), monkeypatch)
    cparams: Dict[str, Any] = {}
    listener(None, None, None, cparams)
    assert cparams["sslmode"] == "require"


@pytest.mark.parametrize("mode", ["verify-ca", "verify-full"])
def test_postgres_does_not_weaken_a_stronger_sslmode(mode, monkeypatch):
    listener, _ = _listener(_postgres(**_IAM), monkeypatch)
    cparams: Dict[str, Any] = {"sslmode": mode}
    listener(None, None, None, cparams)
    assert cparams["sslmode"] == mode


def test_postgres_upgrades_a_weaker_sslmode(monkeypatch):
    """`prefer` would silently fall back to plaintext, and an IAM token is a
    bearer credential."""
    listener, _ = _listener(_postgres(**_IAM), monkeypatch)
    cparams: Dict[str, Any] = {"sslmode": "prefer"}
    listener(None, None, None, cparams)
    assert cparams["sslmode"] == "require"


@pytest.mark.parametrize("factory", [_mysql, _postgres], ids=["mysql", "postgres"])
def test_the_token_manager_is_shared_across_engines(factory, monkeypatch):
    """It caches the current token and its expiry, so one per engine would go
    back to STS on every connect -- and ingestion builds an engine per database."""
    built: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        "datahub.ingestion.source.sql.rds_iam.RDSIAMTokenManager",
        _recording_manager(built),
    )
    config = factory(**_IAM)
    first = config.rds_iam_token_manager()
    second = config.rds_iam_token_manager()
    assert first is second
    assert len(built) == 1


@pytest.mark.parametrize(
    "factory,port", [(_mysql, 3306), (_postgres, 5432)], ids=["mysql", "postgres"]
)
def test_an_unparseable_port_falls_back_to_the_dialect_default(
    factory, port, monkeypatch
):
    built: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        "datahub.ingestion.source.sql.rds_iam.RDSIAMTokenManager",
        _recording_manager(built),
    )
    factory(host_port="db.rds.amazonaws.com:nonsense", **_IAM).rds_iam_token_manager()
    assert built[0]["port"] == port
    assert built[0]["endpoint"] == "db.rds.amazonaws.com"


@pytest.mark.parametrize("factory", [_mysql, _postgres], ids=["mysql", "postgres"])
def test_iam_without_a_username_is_a_user_error(factory):
    config = factory(**_IAM)
    # username is Optional on the shared connection config, so IAM has to check.
    object.__setattr__(config, "username", None)
    with pytest.raises(ValueError, match="username is required"):
        config.rds_iam_token_manager()


# --- the gap this closes -----------------------------------------------------


@pytest.mark.parametrize("factory", [_mysql, _postgres], ids=["mysql", "postgres"])
def test_the_probes_engine_gets_the_same_listener(factory, monkeypatch):
    """The reason this moved.

    `probe_prepare_engine` is a config method, so while the listener lived on the
    Source the probe had no way to install it: an AWS_IAM recipe could not be
    probed at all, because the password is a per-connection token and a bare
    create_engine() has no credential to offer.
    """
    config = factory(**_IAM)
    _stub_manager(monkeypatch)
    registered = _capture(monkeypatch)

    config.probe_prepare_engine(object())

    assert [name for _, name, _ in registered] == ["do_connect"]
    cparams: Dict[str, Any] = {}
    registered[0][2](None, None, None, cparams)
    assert cparams["password"] == "token-1"


@pytest.mark.parametrize("factory", [_mysql, _postgres], ids=["mysql", "postgres"])
def test_probing_a_password_recipe_installs_nothing(factory, monkeypatch):
    registered = _capture(monkeypatch)
    factory().probe_prepare_engine(object())
    assert registered == []


def test_sqlalchemy_accepts_the_registration_on_a_real_engine(monkeypatch):
    """The tests above stub event.listen, so one test drives the real thing --
    otherwise they would all still pass if SQLAlchemy rejected the registration.
    create_engine does not connect, so this needs no server."""
    from sqlalchemy import create_engine, event

    config = _postgres(**_IAM)
    _stub_manager(monkeypatch)
    # Built before the spy is installed: patching rds_iam.event.listen resolves
    # to the shared sqlalchemy.event module, so a create_engine() inside the spy's
    # lifetime would have its own on_connect/first_connect registrations captured
    # too, and the count below would be 3.
    engine = create_engine("postgresql+psycopg2://u@db.rds.amazonaws.com:5432/d")

    real_listen = event.listen
    captured: List[Any] = []

    def spy(target: Any, name: str, fn: Any, **kw: Any) -> None:
        captured.append(fn)
        real_listen(target, name, fn, **kw)

    monkeypatch.setattr("datahub.ingestion.source.sql.rds_iam.event.listen", spy)
    config.install_rds_iam_auth(engine)

    assert len(captured) == 1
    # do_connect is a dialect-level event, so it does not land on the engine's
    # own dispatch -- ask SQLAlchemy rather than guessing where it went.
    assert event.contains(engine, "do_connect", captured[0])


@pytest.mark.parametrize("factory", [_mysql, _postgres], ids=["mysql", "postgres"])
def test_a_copy_pointed_at_another_host_does_not_reuse_the_old_token(
    factory, monkeypatch
):
    """model_copy() carries PrivateAttrs, cache included.

    Measured, not assumed: both model_copy() and model_copy(deep=True) bring the
    cached manager across. Without a check the copy would keep presenting a
    token minted for the original endpoint -- to a different host, with a
    credential scoped to the old one.
    """
    built: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        "datahub.ingestion.source.sql.rds_iam.RDSIAMTokenManager",
        _recording_manager(built),
    )
    config = factory(**_IAM)
    config.rds_iam_token_manager()
    assert built[0]["endpoint"] == "db.rds.amazonaws.com"

    moved = config.model_copy(update={"host_port": "other.rds.amazonaws.com:3306"})
    manager = moved.rds_iam_token_manager()

    assert len(built) == 2, "the copy reused a manager built for another endpoint"
    assert built[1]["endpoint"] == "other.rds.amazonaws.com"
    assert manager is not None


@pytest.mark.parametrize("factory", [_mysql, _postgres], ids=["mysql", "postgres"])
def test_an_unchanged_copy_does_not_rebuild_the_manager(factory, monkeypatch):
    """The guard must not defeat the caching it guards.

    The two copy modes differ, which is worth pinning because it is not what you
    would assume: model_copy() hands over the same manager instance, while
    model_copy(deep=True) deep-copies it -- a distinct object that still carries
    the minted token. Neither goes back to the constructor, which is the
    property that matters: no copy of a config re-authenticates against STS.
    """
    built: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        "datahub.ingestion.source.sql.rds_iam.RDSIAMTokenManager",
        _recording_manager(built),
    )
    config = factory(**_IAM)
    first = config.rds_iam_token_manager()

    assert config.model_copy().rds_iam_token_manager() is first

    deep = config.model_copy(deep=True).rds_iam_token_manager()
    assert deep is not first
    assert deep is not None
    assert (deep.endpoint, deep.port, deep.username) == (
        first.endpoint,  # type: ignore[union-attr]
        first.port,  # type: ignore[union-attr]
        first.username,  # type: ignore[union-attr]
    )

    assert len(built) == 1, "a copy went back to the constructor"
