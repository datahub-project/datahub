import logging
from typing import Dict, Optional

import pytest

from datahub.executor.common.env_config import (
    ReportedSetting,
    env_to_bool,
    env_to_float,
    env_to_int,
    reported_bool,
    reported_float,
    reported_int,
)

VAR = "DATAHUB_TEST_ENV_CONFIG_VAR"


@pytest.fixture(autouse=True)
def _unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(VAR, raising=False)


def test_env_to_bool_falls_back_when_unset() -> None:
    assert env_to_bool(VAR) is True
    assert env_to_bool(VAR, "False") is False


@pytest.mark.parametrize(
    "raw, expected", [("TRUE", True), ("true", True), ("1", False), ("", False)]
)
def test_env_to_bool_only_true_is_true(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: bool
) -> None:
    monkeypatch.setenv(VAR, raw)
    assert env_to_bool(VAR, "False") is expected


def test_env_to_int_parses_digits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(VAR, "25")
    assert env_to_int(VAR, 4) == 25


@pytest.mark.parametrize("raw", [None, "-1", "abc", " 5", ""])
def test_env_to_int_falls_back_on_unset_or_non_digits(
    monkeypatch: pytest.MonkeyPatch, raw: Optional[str]
) -> None:
    if raw is not None:
        monkeypatch.setenv(VAR, raw)
    assert env_to_int(VAR, 4) == 4


def test_env_to_float_parses_valid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(VAR, "0.3")
    assert env_to_float(VAR, 0.0) == 0.3


def test_env_to_float_warns_and_falls_back_on_unparseable_value(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv(VAR, "not-a-number")
    with caplog.at_level(logging.WARNING, logger="datahub.executor.common.env_config"):
        assert env_to_float(VAR, 0.25) == 0.25
    assert VAR in caplog.text


def test_env_to_float_falls_back_when_unset() -> None:
    assert env_to_float(VAR, 0.5) == 0.5


def test_reported_helpers_record_into_the_given_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first: Dict[str, ReportedSetting] = {}
    second: Dict[str, ReportedSetting] = {}
    monkeypatch.setenv("DATAHUB_TEST_REPORTED_INT", "-1")
    monkeypatch.setenv("DATAHUB_TEST_REPORTED_BOOL", "1")
    monkeypatch.setenv("DATAHUB_TEST_REPORTED_FLOAT", "2.5")

    assert reported_int(first, "DATAHUB_TEST_REPORTED_INT", 4) == 4
    assert reported_bool(first, "DATAHUB_TEST_REPORTED_BOOL", True) is False
    assert reported_float(second, "DATAHUB_TEST_REPORTED_FLOAT", 1800.0) == 2.5

    assert first == {
        "DATAHUB_TEST_REPORTED_INT": ReportedSetting(
            env_name="DATAHUB_TEST_REPORTED_INT", value=4, default=4
        ),
        "DATAHUB_TEST_REPORTED_BOOL": ReportedSetting(
            env_name="DATAHUB_TEST_REPORTED_BOOL", value=False, default=True
        ),
    }
    assert second == {
        "DATAHUB_TEST_REPORTED_FLOAT": ReportedSetting(
            env_name="DATAHUB_TEST_REPORTED_FLOAT", value=2.5, default=1800.0
        ),
    }


def test_reported_float_raises_on_unparseable_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry: Dict[str, ReportedSetting] = {}
    monkeypatch.setenv(VAR, "not-a-number")
    with pytest.raises(ValueError):
        reported_float(registry, VAR, 1.0)
    assert registry == {}
