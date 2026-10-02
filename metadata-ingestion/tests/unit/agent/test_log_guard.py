import io
import logging
import warnings
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

import pytest
import yaml
from click.testing import CliRunner

import datahub.ingestion.agent.probe_methods as pm
from datahub.cli.recipe_cli import recipe as recipe_group
from datahub.ingestion.agent.log_guard import REUSED_LOGGERS, quiet_reused_logs
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeInternalError
from tests.unit.agent._parity_fake_source import SOURCE_TYPE as RESOLVABLE_SOURCE

SENTINEL = "PLANTED-log-secret"
# A URL with a password in it, as reused code logs one. Built from parts so a
# secret scanner reading this file does not take the format string for a
# real credential.
_USERINFO_URL = "http://u:" + "%s@host"


def _logger_state() -> Dict[str, Tuple[int, List[object]]]:
    """Level and filters of every guarded logger and every handler that could
    receive their records. The guard scrubs in Logger.callHandlers and must
    leave all of this as it found it."""
    names = [
        name
        for name in list(logging.Logger.manager.loggerDict)
        if any(name == g or name.startswith(g + ".") for g in REUSED_LOGGERS)
    ]
    state: Dict[str, Tuple[int, List[object]]] = {}
    for name in [*REUSED_LOGGERS, *names, "datahub", ""]:
        logger = logging.getLogger(name)
        state[f"logger:{name}"] = (logger.level, list(logger.filters))
        for handler in logger.handlers:
            state[f"handler:{id(handler)}"] = (handler.level, list(handler.filters))
    return state


def test_reused_code_debug_tracebacks_are_dropped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    log = logging.getLogger("datahub.ingestion.source.kafka_connect.common")
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        try:
            raise ValueError(f"jdbc:mysql://db?password={SENTINEL}")
        except ValueError:
            log.debug("lineage failed", exc_info=True)
        log.warning("connecting to " + _USERINFO_URL, SENTINEL)
    assert SENTINEL not in caplog.text
    assert "connecting to" in caplog.text


def test_levels_and_filters_are_restored(caplog: pytest.LogCaptureFixture) -> None:
    log = logging.getLogger("botocore")
    before = log.level
    # Compared, not asserted empty: snowflake-connector installs its own
    # masking filter on botocore once it has been imported by any test.
    filters_before = list(log.filters)
    with quiet_reused_logs(set()):
        pass
    assert log.level == before
    assert log.filters == filters_before


def test_the_verbose_switch_keeps_debug_logs(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        logging.getLogger("datahub.ingestion.source.x").debug("kept")
    assert "kept" in caplog.text


def test_a_traceback_logged_at_warning_loses_its_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        try:
            raise RuntimeError(f"token endpoint said {SENTINEL}")
        except RuntimeError:
            logging.getLogger("botocore.credentials").exception("refresh failed")
    assert "refresh failed" in caplog.text
    assert SENTINEL not in caplog.text
    assert "Traceback" not in caplog.text


def test_a_registered_secret_is_scrubbed_whatever_its_shape(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs({SENTINEL}):
        logging.getLogger("snowflake.connector").warning("bad value %s", SENTINEL)
    assert "bad value" in caplog.text
    assert SENTINEL not in caplog.text


def test_a_child_logger_created_inside_the_guard_is_covered(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A lazy import inside the probe creates its logger after the guard was
    installed, so no logger filter is on it; the handler-level guard is."""
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        late = logging.getLogger("google.auth.created_inside_the_guard")
        late.setLevel(logging.DEBUG)
        late.debug("debug line %s", SENTINEL)
        late.warning(_USERINFO_URL, SENTINEL)
    assert SENTINEL not in caplog.text
    assert "debug line" not in caplog.text


def test_an_existing_child_with_its_own_debug_level_is_floored_and_restored(
    caplog: pytest.LogCaptureFixture,
) -> None:
    child = logging.getLogger("azure.identity.explicitly_debug")
    child.setLevel(logging.DEBUG)
    try:
        caplog.set_level(logging.DEBUG)
        with quiet_reused_logs(set()):
            assert child.getEffectiveLevel() >= logging.WARNING
            child.debug("hidden")
        assert "hidden" not in caplog.text
        assert child.level == logging.DEBUG
    finally:
        child.setLevel(logging.NOTSET)


def test_the_frameworks_own_loggers_are_not_floored(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        try:
            raise ValueError("own")
        except ValueError:
            logging.getLogger("datahub.ingestion.agent.x").debug(
                "framework debug", exc_info=True
            )
        logging.getLogger("datahub.cli.recipe_cli").debug("cli debug")
    assert "framework debug" in caplog.text
    assert "Traceback" in caplog.text
    assert "cli debug" in caplog.text


def test_state_is_restored_exactly_on_exception() -> None:
    before = _logger_state()
    with pytest.raises(RuntimeError), quiet_reused_logs(set()):
        raise RuntimeError("boom")
    assert _logger_state() == before


def test_nested_and_sequential_guards_restore_the_original_state() -> None:
    logging.getLogger("datahub.ingestion.source.nested_child").setLevel(logging.INFO)
    try:
        before = _logger_state()
        with quiet_reused_logs(set()):
            inside_outer = _logger_state()
            with quiet_reused_logs({SENTINEL}):
                pass
            assert _logger_state() == inside_outer
        assert _logger_state() == before
        # Two probes one after the other in the same process.
        with quiet_reused_logs(set()):
            pass
        with quiet_reused_logs(set()):
            pass
        assert _logger_state() == before
    finally:
        logging.getLogger("datahub.ingestion.source.nested_child").setLevel(
            logging.NOTSET
        )


def test_caplog_still_captures_reused_debug_after_the_guard(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        pass
    logging.getLogger("datahub.ingestion.source.after").debug("after the guard")
    assert "after the guard" in caplog.text


class _LeakyProvider:
    @classmethod
    def for_config(cls, config: object) -> "_LeakyProvider":
        return cls()

    def __enter__(self) -> "_LeakyProvider":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    @probe_method()
    def tables(self) -> list:
        "Tables."
        log = logging.getLogger("datahub.ingestion.source.leaky.fetcher")
        try:
            raise ConnectionError(f"GET http://connect/?token={SENTINEL} failed")
        except ConnectionError:
            log.debug("fetch failed", exc_info=True)
        log.warning("retrying " + _USERINFO_URL, SENTINEL)
        return [{"name": "t"}]


class _LeakyConfig:
    @classmethod
    def probe_provider_class(cls) -> type:
        return _LeakyProvider

    @classmethod
    def model_validate(cls, d: object) -> "_LeakyConfig":
        return cls()


def test_run_probe_method_keeps_reused_logs_scrubbed(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pm, "_provider_class", lambda st: _LeakyProvider)
    monkeypatch.setattr(pm, "config_class_for", lambda st: _LeakyConfig)
    caplog.set_level(logging.DEBUG)
    before = _logger_state()
    res = pm.run_probe_method("x", {}, "tables", {})
    assert res.result == [{"name": "t"}]
    assert "retrying" in caplog.text
    assert SENTINEL not in caplog.text
    after = _logger_state()
    # The provider's logger was created during the call, untouched by the guard.
    assert after.pop("logger:datahub.ingestion.source.leaky.fetcher") == (0, [])
    assert after.pop("logger:datahub.ingestion.source.leaky") == (0, [])
    assert after == before


def test_a_captured_warning_is_scrubbed(caplog: pytest.LogCaptureFixture) -> None:
    """The recipe CLI turns on logging.captureWarnings, so a library's
    warnings.warn arrives as a `py.warnings` log record. Emitted here the way
    captureWarnings emits it, rather than by toggling captureWarnings: an
    earlier test's CLI run may have left it on, and then pytest's own warning
    recorder takes the warning instead."""
    import warnings

    caplog.set_level(logging.DEBUG)
    text = warnings.formatwarning(
        "retrying " + _USERINFO_URL % SENTINEL, UserWarning, "lib.py", 1
    )
    with quiet_reused_logs(set()):
        logging.getLogger("py.warnings").warning("%s", text)
    assert "retrying" in caplog.text
    assert SENTINEL not in caplog.text


def test_an_unprintable_message_does_not_escape_into_the_callers_log_call(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _Unprintable:
        def __str__(self) -> str:
            raise RuntimeError("no")

    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        # A name no library has put its own filter on: databricks-sql
        # installs one on urllib3.connectionpool that calls str(record.msg).
        logging.getLogger("datahub.ingestion.source.unprintable").warning(
            _Unprintable()
        )
    assert "<unprintable log message>" in caplog.text


def test_the_last_resort_handler_is_scrubbed_and_restored(
    capsys: pytest.CaptureFixture,
) -> None:
    """A logger with no handler anywhere on its chain is written by
    logging.lastResort, straight to stderr."""
    last_resort = logging.lastResort
    assert last_resort is not None
    filters_before = list(last_resort.filters)
    with quiet_reused_logs(set()):
        lonely = logging.getLogger("httpx.created_inside_without_handlers")
        lonely.propagate = False
        try:
            raise ValueError(SENTINEL)
        except ValueError:
            lonely.exception("request to " + _USERINFO_URL, SENTINEL)
    assert last_resort.filters == filters_before
    err = capsys.readouterr().err
    assert "request to" in err
    assert SENTINEL not in err


@pytest.mark.parametrize(
    "name", ["sqlalchemy.engine", "requests_oauthlib", "oauthlib", "msal", "httpx"]
)
def test_auth_and_driver_loggers_are_floored(
    name: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Root at DEBUG, so an unguarded logger would inherit DEBUG.
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        assert logging.getLogger(name).getEffectiveLevel() >= logging.WARNING


@pytest.mark.parametrize("name", ["pymysql", "kafka.conn", "some_new_sdk.auth"])
def test_an_unlisted_library_logger_is_scrubbed_and_loses_its_traceback(
    name: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Default-deny: a library nobody thought to list is reused code too.
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        try:
            raise RuntimeError(f"token endpoint said {SENTINEL}")
        except RuntimeError:
            logging.getLogger(name).warning(
                "connect failed password=%s", SENTINEL, exc_info=True
            )
    assert "connect failed" in caplog.text
    assert SENTINEL not in caplog.text
    assert "Traceback" not in caplog.text


def test_a_shared_datahub_module_is_scrubbed_but_not_floored(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        try:
            raise RuntimeError(SENTINEL)
        except RuntimeError:
            logging.getLogger("datahub.utilities.some_helper").debug(
                "helper saw password=%s", SENTINEL, exc_info=True
            )
    assert "helper saw" in caplog.text
    assert SENTINEL not in caplog.text
    assert "Traceback" not in caplog.text


@pytest.mark.parametrize(
    "name", ["datahub.cli.recipe_cli", "datahub.masking.x", "datahub.entrypoints"]
)
def test_framework_owned_loggers_keep_their_tracebacks(
    name: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        try:
            raise ValueError("own")
        except ValueError:
            logging.getLogger(name).debug("framework debug", exc_info=True)
    assert "framework debug" in caplog.text
    assert "Traceback" in caplog.text


def test_a_library_handler_that_stops_propagation_is_scrubbed() -> None:
    import io

    stream = io.StringIO()
    lib = logging.getLogger("some_driver_with_its_own_handler")
    handler = logging.StreamHandler(stream)
    lib.addHandler(handler)
    lib.propagate = False
    try:
        with quiet_reused_logs(set()):
            lib.warning("login password=%s", SENTINEL)
        assert "login" in stream.getvalue()
        assert SENTINEL not in stream.getvalue()
        assert handler.filters == []
    finally:
        lib.removeHandler(handler)
        lib.propagate = True


def test_a_silenced_logger_is_dropped_at_every_level(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # SENTINEL has no credential shape, so scrubbing alone would pass it.
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set(), silenced=("datahub.ingestion.source.leaky",)):
        child = logging.getLogger("datahub.ingestion.source.leaky.fetcher")
        child.warning("read %s", SENTINEL)
        child.error("failed on %s", SENTINEL)
        logging.getLogger("datahub.ingestion.source.other").warning("kept")
    assert SENTINEL not in caplog.text
    assert "kept" in caplog.text


def test_a_provider_cannot_silence_the_frameworks_own_loggers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set(), silenced=("datahub.ingestion.agent",)):
        logging.getLogger("datahub.ingestion.agent.probe_methods").warning(
            "framework line"
        )
    assert "framework line" in caplog.text


def test_silencing_ends_with_the_guard(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    before = _logger_state()
    with quiet_reused_logs(set(), silenced=("datahub.ingestion.source.leaky",)):
        pass
    assert _logger_state() == before
    logging.getLogger("datahub.ingestion.source.leaky").warning("after the guard")
    assert "after the guard" in caplog.text


def test_the_verbose_switch_lifts_silencing(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set(), silenced=("datahub.ingestion.source.x",)):
        logging.getLogger("datahub.ingestion.source.x").warning("kept")
    assert "kept" in caplog.text


class _SilencingProvider:
    silenced_loggers = ("datahub.ingestion.source.leaky",)

    @classmethod
    def for_config(cls, config: object) -> "_SilencingProvider":
        return cls()

    def __enter__(self) -> "_SilencingProvider":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    @probe_method()
    def tables(self) -> List[Dict[str, str]]:
        "Tables."
        logging.getLogger("datahub.ingestion.source.leaky.fetcher").warning(
            "read %s", SENTINEL
        )
        return [{"name": "t"}]


class _MisdeclaredProvider:
    # A bare string: iterating it would silence loggers named "d", "a", "t", ...
    # Standalone rather than a _SilencingProvider subclass, so mypy does not
    # reject a str overriding the inherited tuple.
    silenced_loggers = "datahub.ingestion.source.leaky"

    @classmethod
    def for_config(cls, config: object) -> "_MisdeclaredProvider":
        return cls()

    def __enter__(self) -> "_MisdeclaredProvider":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    @probe_method()
    def tables(self) -> List[Dict[str, str]]:
        "Tables."
        return [{"name": "t"}]


def test_run_probe_method_drops_a_providers_silenced_loggers(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pm, "_provider_class", lambda st: _SilencingProvider)
    monkeypatch.setattr(pm, "config_class_for", lambda st: _LeakyConfig)
    caplog.set_level(logging.DEBUG)
    res = pm.run_probe_method("x", {}, "tables", {})
    assert res.result == [{"name": "t"}]
    assert SENTINEL not in caplog.text


def test_probe_run_keeps_silencing_under_the_clis_own_guard(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # `probe run` wraps run_probe_method in a guard of its own, so the
    # provider's silencing comes from the inner one of two nested guards.
    recipe = tmp_path / "recipe.yml"
    recipe.write_text(
        yaml.safe_dump({"source": {"type": RESOLVABLE_SOURCE, "config": {}}})
    )
    monkeypatch.setattr(pm, "_provider_class", lambda st: _SilencingProvider)
    caplog.set_level(logging.DEBUG)
    result = CliRunner().invoke(
        recipe_group, ["probe", "run", "tables", "--recipe", str(recipe)]
    )
    assert result.exit_code == 0, result.output
    assert '"t"' in result.output
    assert SENTINEL not in caplog.text
    assert SENTINEL not in result.output


def test_a_misdeclared_silenced_loggers_is_a_defect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pm, "_provider_class", lambda st: _MisdeclaredProvider)
    monkeypatch.setattr(pm, "config_class_for", lambda st: _LeakyConfig)
    with pytest.raises(ProbeInternalError):
        pm.run_probe_method("x", {}, "tables", {})


def test_a_handler_added_inside_the_guard_is_scrubbed() -> None:
    """A library imported inside the probe may attach its own handler at
    import time, after the guard was entered."""
    stream = io.StringIO()
    lib = logging.getLogger("some_sdk_configured_inside_the_guard")
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))
    try:
        with quiet_reused_logs(set()):
            lib.addHandler(handler)
            lib.propagate = False
            try:
                raise ConnectionError(SENTINEL)
            except ConnectionError:
                lib.warning("token request password=%s", SENTINEL, exc_info=True)
        assert "token request" in stream.getvalue()
        assert SENTINEL not in stream.getvalue()
        assert "Traceback" not in stream.getvalue()
    finally:
        lib.removeHandler(handler)
        lib.propagate = True


def _capturing_warnings() -> bool:
    return getattr(logging, "_warnings_showwarning", None) is not None


@pytest.fixture
def warnings_not_captured() -> Iterator[None]:
    """The state a process is in without the masking bootstrap, which is what
    turns logging.captureWarnings on for the CLI."""
    was_capturing = _capturing_warnings()
    logging.captureWarnings(False)
    try:
        yield
    finally:
        logging.captureWarnings(was_capturing)


@pytest.mark.usefixtures("warnings_not_captured")
def test_warnings_warn_is_scrubbed_without_the_masking_bootstrap(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    showwarning_before = warnings.showwarning
    with warnings.catch_warnings(record=True) as shown:
        warnings.simplefilter("always")
        with quiet_reused_logs(set()):
            warnings.warn(f"retrying password={SENTINEL}", UserWarning, stacklevel=1)
    # Logged, as py.warnings, and scrubbed -- not printed by warnings itself.
    assert shown == []
    assert "retrying" in caplog.text
    assert SENTINEL not in caplog.text
    assert SENTINEL not in capsys.readouterr().err
    assert warnings.showwarning is showwarning_before
    assert not _capturing_warnings()


def test_warning_capture_already_on_is_left_on() -> None:
    was_capturing = _capturing_warnings()
    logging.captureWarnings(True)
    try:
        showwarning_before = warnings.showwarning
        with quiet_reused_logs(set()):
            pass
        assert _capturing_warnings()
        assert warnings.showwarning is showwarning_before
    finally:
        logging.captureWarnings(was_capturing)


def test_logging_is_unpatched_after_nested_guards_and_an_exception() -> None:
    call_handlers = vars(logging.Logger)["callHandlers"]
    with pytest.raises(RuntimeError), quiet_reused_logs(set()):
        with quiet_reused_logs(set()):
            pass
        raise RuntimeError("boom")
    assert vars(logging.Logger)["callHandlers"] is call_handlers
