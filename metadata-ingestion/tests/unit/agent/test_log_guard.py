import io
import logging
import sys
import threading
import warnings
from pathlib import Path
from typing import Dict, Iterator, List

import pytest
import yaml
from click.testing import CliRunner

import datahub.ingestion.agent.probe_methods as pm
from datahub.cli.recipe_cli import recipe as recipe_group
from datahub.ingestion.agent.log_guard import FRAMEWORK_LOGGERS, quiet_reused_logs
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeInternalError
from tests.unit.agent._parity_fake_source import SOURCE_TYPE as RESOLVABLE_SOURCE

SENTINEL = "PLANTED-log-secret"
# A URL with a password in it, as reused code logs one. Built from parts so a
# secret scanner reading this file does not take the format string for a
# real credential.
_USERINFO_URL = "http://u:" + "%s@host"


def _capturing_warnings() -> bool:
    return getattr(logging, "_warnings_showwarning", None) is not None


def _logging_state() -> Dict[str, object]:
    """Everything the guard may change: the record factory, warning capture,
    and the level, filters, handlers and propagation of every logger."""
    state: Dict[str, object] = {
        "factory": logging.getLogRecordFactory(),
        "capture": _capturing_warnings(),
        "showwarning": warnings.showwarning,
    }
    if logging.lastResort is not None:
        state["lastResort"] = list(logging.lastResort.filters)
    loggers: Dict[str, logging.Logger] = {"": logging.getLogger()}
    for name, obj in list(logging.Logger.manager.loggerDict.items()):
        if isinstance(obj, logging.Logger):
            loggers[name] = obj
    for name, logger in loggers.items():
        handlers = [(h, h.level, list(h.filters)) for h in logger.handlers]
        state[f"logger:{name}"] = (
            logger.level,
            list(logger.filters),
            handlers,
            logger.propagate,
        )
    return state


def _assert_restored(before: Dict[str, object]) -> None:
    for key, value in _logging_state().items():
        if key in before:
            assert value == before[key], key
        else:
            # A logger first created inside the guard, as logging made it.
            assert value == (logging.NOTSET, [], [], True), key


def _scrubbed_by_the_guard(name: str, secret: str) -> bool:
    """Whether a record logged now under `name` reaches a handler scrubbed."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    log = logging.getLogger(name)
    log.addHandler(handler)
    log.propagate = False
    try:
        log.warning("value %s", secret)
    finally:
        log.removeHandler(handler)
        log.propagate = True
    return secret not in stream.getvalue()


def test_a_shaped_and_a_registered_secret_are_scrubbed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs({SENTINEL}):
        logging.getLogger("snowflake.connector").warning("bad value %s", SENTINEL)
        logging.getLogger("some_sdk.http").warning(
            "connecting to " + _USERINFO_URL, "PLANTED-shaped"
        )
    assert "bad value" in caplog.text
    assert "connecting to" in caplog.text
    assert SENTINEL not in caplog.text
    assert "PLANTED-shaped" not in caplog.text


def test_reused_debug_is_kept_scrubbed_without_its_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Volume is the host's logging config's business; the guard only
    scrubs, so a source's DEBUG line still reaches a DEBUG handler."""
    log = logging.getLogger("datahub.ingestion.source.kafka_connect.common")
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs({SENTINEL}):
        try:
            raise ValueError(f"jdbc:mysql://db?password={SENTINEL}")
        except ValueError:
            log.debug("lineage failed for %s", SENTINEL, exc_info=True)
    assert "lineage failed for" in caplog.text
    assert SENTINEL not in caplog.text
    assert "Traceback" not in caplog.text


def test_logger_exception_and_stack_info_are_dropped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        try:
            raise RuntimeError(f"token endpoint said {SENTINEL}")
        except RuntimeError:
            logging.getLogger("botocore.credentials").exception("refresh failed")
        logging.getLogger("botocore.credentials").warning("where", stack_info=True)
    assert "refresh failed" in caplog.text
    assert SENTINEL not in caplog.text
    assert "Traceback" not in caplog.text
    assert "Stack (most recent call last)" not in caplog.text


@pytest.mark.parametrize("name", FRAMEWORK_LOGGERS)
def test_a_framework_record_passes_untouched(
    name: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        try:
            raise ValueError("own")
        except ValueError:
            logging.getLogger(name).debug("framework %s", "debug", exc_info=True)
    assert "framework debug" in caplog.text
    assert "Traceback" in caplog.text


@pytest.mark.parametrize(
    "name",
    [
        "pymysql",
        "kafka.conn",
        "some_new_sdk.auth",
        "datahub.utilities.x",
        # CLI helpers and telemetry a source reuses log reused text too.
        "datahub.cli.config_utils",
        "datahub.telemetry.telemetry",
        "datahub.entrypoints",
    ],
)
def test_any_other_logger_is_scrubbed_and_loses_its_traceback(
    name: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Default-deny: a library nobody thought to list is reused code too, and
    # so is a datahub module shared with ingestion.
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


def test_a_logger_and_handler_created_inside_the_guard_are_covered() -> None:
    """A library imported inside the probe may create its logger and attach
    its own handler at import time, after the guard was entered."""
    name = "some_sdk_configured_inside_the_guard"
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    try:
        with quiet_reused_logs({SENTINEL}):
            lib = logging.getLogger(name)
            lib.addHandler(handler)
            lib.propagate = False
            lib.setLevel(logging.DEBUG)
            try:
                raise ConnectionError(SENTINEL)
            except ConnectionError:
                lib.debug("token request %s", SENTINEL, exc_info=True)
        assert "token request" in stream.getvalue()
        assert SENTINEL not in stream.getvalue()
        assert "Traceback" not in stream.getvalue()
    finally:
        lib = logging.getLogger(name)
        lib.removeHandler(handler)
        lib.propagate = True
        lib.setLevel(logging.NOTSET)


def test_a_library_logger_that_stops_propagation_is_scrubbed() -> None:
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


def test_the_last_resort_handler_is_scrubbed(capsys: pytest.CaptureFixture) -> None:
    """A logger with no handler anywhere on its chain is written by
    logging.lastResort, straight to stderr."""
    lonely = logging.getLogger("httpx.created_inside_without_handlers")
    try:
        with quiet_reused_logs(set()):
            lonely.propagate = False
            try:
                raise ValueError(SENTINEL)
            except ValueError:
                lonely.exception("request to " + _USERINFO_URL, SENTINEL)
    finally:
        lonely.propagate = True
    err = capsys.readouterr().err
    assert "request to" in err
    assert SENTINEL not in err


def test_a_logger_filter_sees_the_scrubbed_record() -> None:
    """A logger's own filter runs before any handler -- error reporters
    collect breadcrumbs there -- so the record is scrubbed when it is made."""
    seen: List[str] = []

    class _Breadcrumbs(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            seen.append(record.getMessage())
            return True

    lib = logging.getLogger("some_sdk.with_breadcrumbs")
    crumbs = _Breadcrumbs()
    lib.addFilter(crumbs)
    try:
        with quiet_reused_logs({SENTINEL}):
            lib.warning("value %s", SENTINEL)
    finally:
        lib.removeFilter(crumbs)
    assert len(seen) == 1
    assert "value" in seen[0]
    assert SENTINEL not in seen[0]


def test_a_logger_subclass_that_dispatches_records_itself_is_scrubbed() -> None:
    class _SelfDispatching(logging.Logger):
        def callHandlers(self, record: logging.LogRecord) -> None:
            for handler in self.handlers:
                handler.handle(record)

    stream = io.StringIO()
    lib = _SelfDispatching("some_sdk.self_dispatching")
    lib.addHandler(logging.StreamHandler(stream))
    with quiet_reused_logs({SENTINEL}):
        lib.warning("value %s", SENTINEL)
    assert "value" in stream.getvalue()
    assert SENTINEL not in stream.getvalue()


def test_a_record_built_by_make_log_record_does_not_break_logging() -> None:
    """makeLogRecord calls the record factory with no name and fills the
    record in afterwards; that is the documented gap, not a crash."""
    with quiet_reused_logs({SENTINEL}):
        record = logging.makeLogRecord({"name": "some_lib.socket", "msg": "m"})
    assert record.name == "some_lib.socket"


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
def test_warnings_warn_is_scrubbed_without_prior_capture(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    showwarning_before = warnings.showwarning
    with warnings.catch_warnings(record=True) as shown:
        warnings.simplefilter("always")
        with quiet_reused_logs(set()):
            assert _capturing_warnings()
            warnings.warn(f"retrying password={SENTINEL}", UserWarning, stacklevel=1)
    # Logged, as py.warnings, and scrubbed -- not printed by warnings itself.
    assert shown == []
    assert "retrying" in caplog.text
    assert SENTINEL not in caplog.text
    assert SENTINEL not in capsys.readouterr().err
    assert warnings.showwarning is showwarning_before
    assert not _capturing_warnings()


def test_warnings_warn_is_scrubbed_under_prior_capture_which_is_left_on(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    was_capturing = _capturing_warnings()
    logging.captureWarnings(False)
    logging.captureWarnings(True)
    try:
        showwarning_before = warnings.showwarning
        # Not record=True: that would put warnings' own printer back in front
        # of the capture.
        with warnings.catch_warnings():
            warnings.simplefilter("always")
            with quiet_reused_logs(set()):
                warnings.warn(
                    f"retrying password={SENTINEL}", UserWarning, stacklevel=1
                )
        assert "retrying" in caplog.text
        assert SENTINEL not in caplog.text
        assert _capturing_warnings()
        assert warnings.showwarning is showwarning_before
    finally:
        logging.captureWarnings(False)
        logging.captureWarnings(was_capturing)


@pytest.mark.usefixtures("warnings_not_captured")
def test_a_bypassed_capture_is_left_alone_a_documented_gap() -> None:
    """Capture is on but a library's own printer is in front: the guard turns
    nothing on and puts no printer of its own in, so the warning reaches that
    printer unscrubbed -- the gap log_guard's module docstring documents.
    Pinned, so closing the gap means updating the docstring too."""
    printed: List[str] = []
    logging.captureWarnings(True)
    warnings.showwarning = lambda message, *args, **kwargs: printed.append(str(message))
    library_printer = warnings.showwarning
    message = f"retrying password={SENTINEL}"
    try:
        # Not record=True: that would put warnings' own printer in front.
        with warnings.catch_warnings():
            warnings.simplefilter("always")
            with quiet_reused_logs(set()):
                assert warnings.showwarning is library_printer
                warnings.warn(message, UserWarning, stacklevel=1)
        assert printed == [message]
        assert _capturing_warnings()
    finally:
        logging.captureWarnings(False)


def test_a_silenced_logger_and_its_inheriting_children_are_dropped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The values have no credential shape, so scrubbing alone would pass them.
    caplog.set_level(logging.DEBUG)
    own_level = logging.getLogger("datahub.ingestion.source.leaky.own_level")
    own_level.setLevel(logging.DEBUG)
    try:
        before = _logging_state()
        with quiet_reused_logs(set(), silenced=("datahub.ingestion.source.leaky",)):
            logging.getLogger("datahub.ingestion.source.leaky").error(
                "read %s", "PLANTED-parent"
            )
            child = logging.getLogger("datahub.ingestion.source.leaky.fetcher")
            child.critical("read %s", "PLANTED-child")
            # A level of its own escapes the silencing, but not the scrub.
            own_level.debug("read " + _USERINFO_URL, "PLANTED-own-level")
            logging.getLogger("datahub.ingestion.source.other").warning("kept")
        _assert_restored(before)
    finally:
        own_level.setLevel(logging.NOTSET)
    assert "PLANTED-parent" not in caplog.text
    assert "PLANTED-child" not in caplog.text
    assert "PLANTED-own-level" not in caplog.text
    assert "read http://" in caplog.text
    assert "kept" in caplog.text
    logging.getLogger("datahub.ingestion.source.leaky").warning("after the guard")
    assert "after the guard" in caplog.text


def test_a_logger_silenced_by_two_guards_stays_silent_until_both_close(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    name = "datahub.ingestion.source.silenced_twice"
    first = quiet_reused_logs(set(), silenced=(name,))
    second = quiet_reused_logs(set(), silenced=(name,))
    first.__enter__()
    second.__enter__()
    first.__exit__(None, None, None)
    try:
        logging.getLogger(name).warning("read %s", "PLANTED-silenced-twice")
    finally:
        second.__exit__(None, None, None)
    assert "PLANTED-silenced-twice" not in caplog.text
    assert logging.getLogger(name).level == logging.NOTSET


def test_the_verbose_switch_keeps_logs_as_logged(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set(), silenced=("datahub.ingestion.source.x",)):
        try:
            raise ValueError("why")
        except ValueError:
            logging.getLogger("datahub.ingestion.source.x").debug("kept", exc_info=True)
    assert "kept" in caplog.text
    assert "Traceback" in caplog.text


@pytest.mark.usefixtures("warnings_not_captured")
def test_everything_is_restored_when_the_guarded_code_raises() -> None:
    name = "datahub.ingestion.source.raising"
    logging.getLogger(name).setLevel(logging.INFO)
    try:
        before = _logging_state()
        with pytest.raises(RuntimeError), quiet_reused_logs({SENTINEL}, (name,)):
            with quiet_reused_logs(set()):
                pass
            raise RuntimeError("boom")
        _assert_restored(before)
    finally:
        logging.getLogger(name).setLevel(logging.NOTSET)


def test_every_open_guards_secrets_are_scrubbed() -> None:
    with quiet_reused_logs({"PLANTED-outer"}), quiet_reused_logs({"PLANTED-inner"}):
        assert _scrubbed_by_the_guard("some_lib.two_open", "PLANTED-outer")
        assert _scrubbed_by_the_guard("some_lib.two_open", "PLANTED-inner")


def test_guards_closing_out_of_order_keep_the_open_one_scrubbing() -> None:
    original = logging.getLogRecordFactory()
    first = quiet_reused_logs({"PLANTED-first"})
    second = quiet_reused_logs({"PLANTED-second"})
    first.__enter__()
    second.__enter__()
    first.__exit__(None, None, None)
    try:
        assert _scrubbed_by_the_guard("some_lib.out_of_order", "PLANTED-second")
        assert not _scrubbed_by_the_guard("some_lib.out_of_order", "PLANTED-first")
    finally:
        second.__exit__(None, None, None)
    assert logging.getLogRecordFactory() is original
    assert not _scrubbed_by_the_guard("some_lib.out_of_order", "PLANTED-second")


def test_a_record_factory_installed_inside_the_guard_is_left_in_place() -> None:
    """A library that wraps the record factory itself while the guard is
    open keeps its wrapper; the guard's, left underneath, passes records
    through until a guard opens again."""
    original = logging.getLogRecordFactory()
    tagged: List[str] = []

    def tagging(*args: object, **kwargs: object) -> logging.LogRecord:
        record = below(*args, **kwargs)
        tagged.append(record.name)
        return record

    with quiet_reused_logs({"PLANTED-tagged"}):
        below = logging.getLogRecordFactory()
        logging.setLogRecordFactory(tagging)
    try:
        assert logging.getLogRecordFactory() is tagging
        assert not _scrubbed_by_the_guard("some_lib.tagged", "PLANTED-tagged")
        assert tagged
        # A guard opened over it scrubs, and does not call itself through it.
        with quiet_reused_logs({"PLANTED-again"}):
            assert _scrubbed_by_the_guard("some_lib.tagged", "PLANTED-again")
        assert logging.getLogRecordFactory() is tagging
        # The library unwraps, putting back what it found: the guard's
        # factory, which the next guard to close removes.
        logging.setLogRecordFactory(below)
        with quiet_reused_logs(set()):
            pass
        assert logging.getLogRecordFactory() is original
    finally:
        logging.setLogRecordFactory(original)


def test_guards_on_two_threads_both_scrub_and_restore() -> None:
    original = logging.getLogRecordFactory()
    entered = [threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]
    results: Dict[int, bool] = {}

    def probe(i: int) -> None:
        secret = f"PLANTED-thread-{i}"
        with quiet_reused_logs({secret}):
            entered[i].set()
            release[i].wait(5)
            results[i] = _scrubbed_by_the_guard(f"some_lib.thread{i}", secret)

    threads = [threading.Thread(target=probe, args=(i,)) for i in (0, 1)]
    for t in threads:
        t.start()
    for e in entered:
        assert e.wait(5)
    # The first one in leaves first.
    release[0].set()
    threads[0].join(5)
    release[1].set()
    threads[1].join(5)
    assert results == {0: True, 1: True}
    assert logging.getLogRecordFactory() is original


def test_caplog_still_captures_reused_debug_after_the_guard(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    with quiet_reused_logs(set()):
        pass
    logging.getLogger("datahub.ingestion.source.after").debug(
        "after the guard %s", SENTINEL
    )
    assert f"after the guard {SENTINEL}" in caplog.text


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
    before = _logging_state()
    res = pm.run_probe_method("x", {}, "tables", {})
    assert res.result == [{"name": "t"}]
    assert "retrying" in caplog.text
    assert "fetch failed" in caplog.text
    assert SENTINEL not in caplog.text
    assert "Traceback" not in caplog.text
    _assert_restored(before)


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


@pytest.mark.parametrize("guard_logs", [True, False])
def test_a_library_caller_is_guarded_unless_it_opts_out(
    guard_logs: bool,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # On by default: a caller that does nothing gets the CLI's protection.
    monkeypatch.setattr(pm, "_provider_class", lambda st: _SilencingProvider)
    monkeypatch.setattr(pm, "config_class_for", lambda st: _LeakyConfig)
    caplog.set_level(logging.DEBUG)
    pm.run_probe_method("x", {}, "tables", {}, guard_logs=guard_logs)
    assert (SENTINEL in caplog.text) is not guard_logs


class _ThreadWatchingProvider(_LeakyProvider):
    """Logs a traceback from another thread while the probe call is open, as
    a library caller's own worker would."""

    other_thread_records: List[logging.LogRecord] = []
    # Read by the test, not asserted here: an AssertionError raised inside a
    # provider call is policed into a source failure that hides it.
    worker_finished = False

    @probe_method()
    def tables(self) -> list:
        "Tables."

        def worker() -> None:
            log = logging.getLogger("my_app.worker")
            try:
                raise RuntimeError("worker failed")
            except RuntimeError:
                record = log.makeRecord(
                    log.name,
                    logging.ERROR,
                    __file__,
                    0,
                    "worker %s",
                    ("failed",),
                    exc_info=sys.exc_info(),
                )
            self.other_thread_records.append(record)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(5)
        type(self).worker_finished = not thread.is_alive()
        return [{"name": "t"}]


@pytest.mark.parametrize("guard_logs", [False, True])
def test_a_library_callers_other_threads_keep_their_tracebacks_unless_guarded(
    guard_logs: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pm, "_provider_class", lambda st: _ThreadWatchingProvider)
    monkeypatch.setattr(pm, "config_class_for", lambda st: _LeakyConfig)
    _ThreadWatchingProvider.other_thread_records = []
    _ThreadWatchingProvider.worker_finished = False
    before = _logging_state()
    res = pm.run_probe_method("x", {}, "tables", {}, guard_logs=guard_logs)
    assert res.result == [{"name": "t"}]
    assert _ThreadWatchingProvider.worker_finished, "the worker did not finish"
    (record,) = _ThreadWatchingProvider.other_thread_records
    # Opted out, the embedding process's own records are as it logged them.
    assert (record.exc_info is not None) is not guard_logs
    assert (record.args is not None) is not guard_logs
    _assert_restored(before)


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


@pytest.mark.parametrize(
    "name",
    [
        "datahub.ingestion.agent",
        "datahub.cli.recipe_cli",
        # Ancestors: framework loggers inherit their level.
        "datahub.ingestion",
        "datahub",
        "root",
    ],
)
def test_silencing_a_framework_logger_is_a_defect(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = type("_Provider", (_SilencingProvider,), {"silenced_loggers": (name,)})
    monkeypatch.setattr(pm, "_provider_class", lambda st: provider)
    monkeypatch.setattr(pm, "config_class_for", lambda st: _LeakyConfig)
    before = _logging_state()
    with pytest.raises(ProbeInternalError, match="silenced_loggers cannot name"):
        pm.run_probe_method("x", {}, "tables", {})
    _assert_restored(before)


class _FrameworkSilencingProvider(_SilencingProvider):
    silenced_loggers = ("datahub.ingestion.agent",)


def test_probe_run_exits_1_when_a_provider_silences_the_framework(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    recipe = tmp_path / "recipe.yml"
    recipe.write_text(
        yaml.safe_dump({"source": {"type": RESOLVABLE_SOURCE, "config": {}}})
    )
    monkeypatch.setattr(pm, "_provider_class", lambda st: _FrameworkSilencingProvider)
    result = CliRunner().invoke(
        recipe_group, ["probe", "run", "tables", "--recipe", str(recipe)]
    )
    assert result.exit_code == 1, result.output
    assert "silenced_loggers cannot name 'datahub.ingestion.agent'" in result.output
