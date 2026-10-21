import errno as errno_codes
import pathlib
from typing import Callable, List, Optional

import pytest
from click.testing import CliRunner, Result
from google.api_core import exceptions as google_exceptions

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe
from datahub.ingestion.agent import probe_methods
from datahub.ingestion.agent.error_policy import (
    classify_foreign,
    foreign_label,
    name_foreign,
)
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodResult,
    probe_method,
    run_probe_method,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
    ProbeReadFailed,
)
from datahub.ingestion.source.bigquery_v2.bigquery_probe import BigQueryMetadataProbe
from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe
from tests.unit.agent import _foreign_coded_errors as coded
from tests.unit.agent._foreign_coded_errors import SENTINEL


def _caught(raiser: Callable[[], object]) -> BaseException:
    try:
        raiser()
    except BaseException as exc:
        return exc
    raise AssertionError("did not raise")


def _wrapped(innermost: BaseException, wrappers: int) -> BaseException:
    exc = innermost
    for _ in range(wrappers):
        outer = RuntimeError("wrapper")
        outer.__cause__ = exc
        exc = outer
    return exc


@pytest.mark.parametrize(
    "raiser, label",
    [
        (coded.http, "HTTPError; HTTP 403"),
        (coded.azure, "HttpResponseError; HTTP 404"),
        (coded.status, "StatusError; HTTP 429"),
        # One code only, and the SQLSTATE before the errno.
        (coded.snowflake, "SnowflakeProgrammingError; SQLSTATE 42S02"),
        (
            lambda: coded.snowflake(sqlstate=None),
            "SnowflakeProgrammingError; errno 2003",
        ),
        (coded.refused, f"ConnectionRefusedError; errno {errno_codes.ECONNREFUSED}"),
        (coded.chained_from_azure, "RuntimeError; HTTP 404"),
    ],
)
def test_the_generic_reader_labels_cross_library_codes(
    raiser: Callable[[], object], label: str
) -> None:
    assert foreign_label(_caught(raiser)) == label


@pytest.mark.parametrize(
    "raiser",
    [
        lambda: coded.azure(status_code=True),
        lambda: coded.azure(status_code=99),
        lambda: coded.azure(status_code=700),
        lambda: coded.azure(status_code="403"),
        lambda: coded.azure(status_code=f"403 {SENTINEL}"),
        lambda: coded.status(code=False),
        lambda: coded.snowflake(errno=True, sqlstate=None),
        lambda: coded.snowflake(errno=-1, sqlstate=None),
        lambda: coded.snowflake(errno=1_000_000, sqlstate=None),
        lambda: coded.snowflake(errno=f"2003 {SENTINEL}", sqlstate=None),
        lambda: coded.snowflake(errno=None, sqlstate=f"42S02 {SENTINEL}"),
        lambda: coded.snowflake(errno=None, sqlstate="HELLO"),
        lambda: coded.snowflake(errno=None, sqlstate="42s02"),
        lambda: coded.snowflake(errno=None, sqlstate=SENTINEL),
        coded.raising_code,
        coded.broken_getattr,
        coded.hostile_getattribute,
        coded.cause_property,
        # The context is not the cause: a 429 handled before an unrelated
        # failure must not label it as rate limiting.
        coded.connection_error_while_handling_429,
        coded.suppressed_429,
        # Vendor shapes the framework has no knowledge of; a provider reads them.
        coded.pg,
        coded.odbc,
        coded.mysql,
        coded.sqlalchemy_wrapping_pg,
        coded.aws,
        coded.fake_google,
        lambda: coded.vendor(status_code=None),
    ],
)
def test_a_value_that_is_not_a_bare_code_is_dropped(
    raiser: Callable[[], object],
) -> None:
    exc = _caught(raiser)
    assert foreign_label(exc) == type(exc).__name__


def test_a_plain_error_gets_no_code_from_its_arguments() -> None:
    assert foreign_label(ValueError("HY000")) == "ValueError"
    assert foreign_label(KeyError(1146)) == "KeyError"


def test_a_str_subclass_cannot_render_its_own_text() -> None:
    exc = _caught(
        lambda: coded.snowflake(errno=None, sqlstate=coded.SneakyStr("42S02"))
    )
    assert foreign_label(exc) == "SnowflakeProgrammingError; SQLSTATE 42S02"


def test_the_cause_chain_is_read_eight_links_deep_and_no_further() -> None:
    inner = _caught(coded.azure)
    assert foreign_label(_wrapped(inner, 7)) == "RuntimeError; HTTP 404"
    assert foreign_label(_wrapped(_caught(coded.azure), 8)) == "RuntimeError"


def test_a_cause_cycle_ends() -> None:
    first, second = RuntimeError("a"), RuntimeError("b")
    first.__cause__ = second
    second.__cause__ = first
    assert foreign_label(first) == "RuntimeError"


def _raise(exc: BaseException) -> Callable[[], object]:
    def raiser() -> object:
        raise exc

    return raiser


def _vendor_code(exc: BaseException) -> Optional[str]:
    code = getattr(exc, "vendor_code", None)
    return code if isinstance(code, str) else None


class _VendorProvider:
    probe_error_code = staticmethod(_vendor_code)


class _RaisingReader:
    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        raise RuntimeError(f"reader quoted {SENTINEL}")


class _ArbitraryReader:
    """Returns whatever the test planted, to show the framework's shape check."""

    planted: object = None

    @classmethod
    def probe_error_code(cls, exc: BaseException) -> object:
        return cls.planted


class _NotCallableReader:
    probe_error_code = "AccessDenied"


def test_the_providers_reader_is_consulted_first() -> None:
    exc = _caught(lambda: coded.vendor(status_code=500))
    assert foreign_label(exc, _VendorProvider) == "VendorError; AccessDenied"


def test_the_providers_reader_sees_each_cause() -> None:
    exc = _caught(coded.chained_from_vendor)
    assert foreign_label(exc, _VendorProvider) == "RuntimeError; AccessDenied"


@pytest.mark.parametrize(
    "planted",
    [
        f"relation {SENTINEL} does not exist",
        "ORA-00942; user=x",
        "ORA-00942",
        "db-01.corp.example.com",
        "permission denied",
        "Invalid.Instance.ID",
        "SQLSTATE " + "1" * 17,
        "code=42",
        "",
        " ORA",
        "9ORA",
        "A" * 33,
        "SQLSTATE " + "1" * 33,
        42,
        b"ORA-00942",
    ],
)
def test_a_provider_code_that_is_not_a_bare_code_falls_back_to_the_generic_one(
    monkeypatch: pytest.MonkeyPatch, planted: object
) -> None:
    monkeypatch.setattr(_ArbitraryReader, "planted", planted)
    exc = _caught(lambda: coded.vendor(status_code=503))
    assert foreign_label(exc, _ArbitraryReader) == "VendorError; HTTP 503"


@pytest.mark.parametrize(
    "planted",
    [
        "SQLSTATE 42P01",
        "errno 1146",
        "HTTP 403",
        "AccessDenied",
        "InvalidInstanceID.NotFound",
        "A" * 32,
    ],
)
def test_a_bare_provider_code_is_shown(
    monkeypatch: pytest.MonkeyPatch, planted: str
) -> None:
    monkeypatch.setattr(_ArbitraryReader, "planted", planted)
    exc = _caught(lambda: coded.vendor(status_code=503))
    assert foreign_label(exc, _ArbitraryReader) == f"VendorError; {planted}"


def test_every_link_is_asked_of_the_provider_before_any_of_the_generic_reader() -> None:
    outer = _caught(lambda: coded.azure(status_code=500))
    outer.__cause__ = _caught(coded.vendor)
    assert foreign_label(outer, _VendorProvider) == "HttpResponseError; AccessDenied"


def test_an_http_status_is_read_before_a_sqlstate() -> None:
    exc = _caught(lambda: coded.azure(status_code=503))
    setattr(exc, "sqlstate", "42S02")  # noqa: B010
    assert foreign_label(exc) == "HttpResponseError; HTTP 503"


def test_a_provider_code_is_shown_as_its_characters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_ArbitraryReader, "planted", coded.SneakyStr("AccessDenied"))
    label = foreign_label(_caught(coded.vendor), _ArbitraryReader)
    assert label == "VendorError; AccessDenied"


@pytest.mark.parametrize("provider_cls", [_RaisingReader, _NotCallableReader, object])
def test_a_reader_that_fails_or_is_absent_leaves_the_generic_code(
    provider_cls: type,
) -> None:
    label = foreign_label(_caught(coded.azure), provider_cls)
    assert label == "HttpResponseError; HTTP 404"


@pytest.mark.parametrize(
    "raiser, code",
    [
        (coded.pg, "SQLSTATE 42P01"),
        (coded.sqlalchemy_wrapping_pg, "SQLSTATE 42P01"),
        (coded.odbc, "SQLSTATE 42S02"),
        (coded.mysql, "errno 1146"),
        (coded.mysqldb, "errno 1146"),
        (coded.sqlalchemy_wrapping_mysql, "errno 1146"),
        # A wrapper raised without `from` hides the driver error from the
        # framework's cause walk, so the generic codes are read on it here.
        (coded.sqlalchemy_wrapping_sqlstate, "SQLSTATE 42S02"),
        (coded.sqlalchemy_wrapping_status, "HTTP 503"),
    ],
)
def test_the_sqlalchemy_family_reads_its_drivers_codes(
    raiser: Callable[[], object], code: str
) -> None:
    assert SqlAlchemyMetadataProbe.probe_error_code(_caught(raiser)) == code


@pytest.mark.parametrize(
    "raiser",
    [
        lambda: coded.pg(pgcode="hello"),
        lambda: coded.pg(pgcode="HELLO"),
        lambda: coded.pg(pgcode=f"42P01 {SENTINEL}"),
        lambda: coded.pg(pgcode=None),
        lambda: coded.odbc(sqlstate=SENTINEL),
        lambda: coded.mysql(errno=True),
        lambda: coded.mysql(errno="1146"),
        lambda: coded.mysql(errno=-1),
        # args[0] is read only from the drivers that put a code there.
        _raise(ValueError("42S02")),
        _raise(KeyError(1146)),
        coded.http,
    ],
)
def test_the_sqlalchemy_family_reads_nothing_else(
    raiser: Callable[[], object],
) -> None:
    assert SqlAlchemyMetadataProbe.probe_error_code(_caught(raiser)) is None


def test_the_sqlalchemy_family_code_reaches_the_label() -> None:
    exc = _caught(coded.sqlalchemy_wrapping_pg)
    label = foreign_label(exc, SqlAlchemyMetadataProbe)
    assert label == "ProgrammingError; SQLSTATE 42P01"


def test_bigquery_reads_the_google_api_status() -> None:
    exc = google_exceptions.Forbidden(f"caller {SENTINEL} lacks bigquery.tables.list")
    assert BigQueryMetadataProbe.probe_error_code(exc) == "HTTP 403"
    assert foreign_label(exc, BigQueryMetadataProbe) == "Forbidden; HTTP 403"


def test_bigquery_reads_a_code_only_from_google_api_errors() -> None:
    assert BigQueryMetadataProbe.probe_error_code(_caught(coded.fake_google)) is None
    assert BigQueryMetadataProbe.probe_error_code(_caught(coded.azure)) is None


def test_the_verbose_switch_appends_the_scrubbed_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    exc = RuntimeError(
        f"fetcher gave up on https://user:{SENTINEL}@host/api password={SENTINEL}"
    )
    assert foreign_label(exc) == "RuntimeError"
    named = name_foreign(exc)
    assert named.startswith("(RuntimeError): fetcher gave up on https://")
    assert SENTINEL not in named


def test_the_verbose_switch_keeps_the_code(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    named = name_foreign(_caught(coded.azure))
    assert named.startswith("(HttpResponseError; HTTP 404): container ")


def test_an_unrenderable_exception_is_named_by_class_under_verbose(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Unprintable(Exception):
        def __str__(self) -> str:
            raise RuntimeError(f"cannot render {SENTINEL}")

    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    assert name_foreign(Unprintable()) == "(Unprintable)"


def test_the_code_does_not_move_the_exit_family() -> None:
    exc = _caught(coded.azure)
    assert isinstance(classify_foreign(exc, "'tables'"), ProbeConnectionError)
    assert str(classify_foreign(exc, "'tables'")) == (
        "'tables' failed (HttpResponseError; HTTP 404)"
    )
    assert isinstance(classify_foreign(ValueError("x"), "'tables'"), ProbeArgumentError)
    assert isinstance(classify_foreign(KeyError("x"), "'tables'"), ProbeInternalError)


def test_classify_foreign_reads_the_providers_code() -> None:
    exc = _caught(coded.vendor)
    assert str(classify_foreign(exc, "'tables'", _VendorProvider)) == (
        "'tables' failed (VendorError; AccessDenied)"
    )


class _Provider:
    probe_error_code = staticmethod(_vendor_code)

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.failures: List[str] = []

    @classmethod
    def for_config(cls, config: object) -> "_Provider":
        mode = getattr(config, "mode", "")
        if mode == "open":
            coded.vendor()
        return cls(mode)

    def __enter__(self) -> "_Provider":
        return self

    def __exit__(self, *exc: object) -> None:
        if self.mode == "close":
            coded.vendor()

    @probe_method(name="things")
    def things(self) -> List[str]:
        """List things."""
        if self.mode == "call":
            coded.vendor()
        if self.mode == "generic":
            coded.azure()
        if self.mode == "recorded":
            self.failures = ["GET /things returned 401"]
            coded.vendor()
        if self.mode == "wrapped":
            try:
                coded.vendor()
            except Exception as exc:
                raise ProbeConnectionError(f"listing failed: {exc}") from exc
        if self.mode == "cause-property":
            coded.cause_property()
        if self.mode == "hostile-getattribute":
            coded.hostile_getattribute()
        if self.mode == "wraps-cause-property":
            try:
                coded.cause_property()
            except Exception as exc:
                raise ProbeConnectionError("listing failed") from exc
        return []


RunFn = Callable[[str], ProbeMethodResult]


@pytest.fixture
def run(monkeypatch: pytest.MonkeyPatch) -> RunFn:
    from datahub.configuration.common import ConfigModel

    class _Config(ConfigModel):
        mode: str = ""

        @classmethod
        def probe_provider_class(cls) -> type:
            return _Provider

    monkeypatch.setattr(probe_methods, "config_class_for", lambda _st: _Config)

    def _run(mode: str) -> ProbeMethodResult:
        return run_probe_method("fake", {"mode": mode}, "things", {})

    return _run


@pytest.mark.parametrize(
    "mode, message",
    [
        ("call", "'things' failed (VendorError; AccessDenied)"),
        ("generic", "'things' failed (HttpResponseError; HTTP 404)"),
        ("open", "opening source 'fake' failed (VendorError; AccessDenied)"),
        ("close", "closing source 'fake' failed (VendorError; AccessDenied)"),
        ("wrapped", "listing failed: (VendorError; AccessDenied)"),
    ],
)
def test_every_foreign_failure_path_reads_the_providers_code(
    run: RunFn, mode: str, message: str
) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run(mode)
    assert str(info.value) == message


def test_a_recorded_failure_reads_the_providers_code(run: RunFn) -> None:
    with pytest.raises(ProbeReadFailed) as info:
        run("recorded")
    assert str(info.value) == (
        "VendorError; AccessDenied; the connector recorded: GET /things returned 401"
    )


def _invoke(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, mode: str
) -> Result:
    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda _r: ("fake", {"mode": mode}, set())
    )
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: fake\n  config: {}\n")
    return CliRunner().invoke(
        recipe, ["probe", "run", "things", "--recipe", str(recipe_file)]
    )


@pytest.mark.parametrize(
    "mode, label",
    [
        ("call", "'things' failed (VendorError; AccessDenied)"),
        ("open", "opening source 'fake' failed (VendorError; AccessDenied)"),
        ("close", "closing source 'fake' failed (VendorError; AccessDenied)"),
        ("wrapped", "listing failed: (VendorError; AccessDenied)"),
        ("cause-property", "'things' failed (_CauseProperty)"),
        ("hostile-getattribute", "'things' failed (_HostileGetattribute)"),
        ("wraps-cause-property", "listing failed"),
    ],
)
def test_the_cli_shows_the_label_and_keeps_exit_3(
    run: RunFn,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    mode: str,
    label: str,
) -> None:
    res = _invoke(monkeypatch, tmp_path, mode)
    assert res.exit_code == 3, res.output
    assert label in res.stderr
    assert SENTINEL not in res.output
