from datahub.emitter import rest_emitter
from datahub.emitter.rest_emitter import DatahubRestEmitter

MOCK_GMS_ENDPOINT = "http://fakegmshost:8080"


def test_datahub_rest_emitter_construction() -> None:
    emitter = DatahubRestEmitter(MOCK_GMS_ENDPOINT)
    assert emitter._session_config.timeout == rest_emitter._DEFAULT_TIMEOUT_SEC
    assert (
        emitter._session_config.retry_status_codes
        == rest_emitter._DEFAULT_RETRY_STATUS_CODES
    )
    assert (
        emitter._session_config.retry_max_times == rest_emitter._DEFAULT_RETRY_MAX_TIMES
    )


def test_datahub_rest_emitter_timeout_construction() -> None:
    emitter = DatahubRestEmitter(
        MOCK_GMS_ENDPOINT, connect_timeout_sec=2, read_timeout_sec=4
    )
    assert emitter._session_config.timeout == (2, 4)


def test_datahub_rest_emitter_general_timeout_construction() -> None:
    emitter = DatahubRestEmitter(MOCK_GMS_ENDPOINT, timeout_sec=2, read_timeout_sec=4)
    assert emitter._session_config.timeout == (2, 4)


def test_datahub_rest_emitter_retry_construction() -> None:
    emitter = DatahubRestEmitter(
        MOCK_GMS_ENDPOINT,
        retry_status_codes=[418],
        retry_max_times=42,
    )
    assert emitter._session_config.retry_status_codes == [418]
    assert emitter._session_config.retry_max_times == 42


def test_datahub_rest_emitter_extra_params() -> None:
    emitter = DatahubRestEmitter(
        MOCK_GMS_ENDPOINT, extra_headers={"key1": "value1", "key2": "value2"}
    )
    assert emitter._session.headers.get("key1") == "value1"
    assert emitter._session.headers.get("key2") == "value2"
