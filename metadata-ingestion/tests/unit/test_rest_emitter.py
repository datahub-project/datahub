from datahub.emitter.rest_emitter import DatahubRestEmitter

MOCK_GMS_ENDPOINT = "http://fakegmshost:8080"


def test_datahub_rest_emitter_construction():
    emitter = DatahubRestEmitter(MOCK_GMS_ENDPOINT)
    assert emitter._connect_timeout_sec == emitter.DEFAULT_CONNECT_TIMEOUT_SEC
    assert emitter._read_timeout_sec == emitter.DEFAULT_READ_TIMEOUT_SEC


def test_datahub_rest_emitter_timeout_construction():
    emitter = DatahubRestEmitter(
        MOCK_GMS_ENDPOINT, connect_timeout_sec=2, read_timeout_sec=4
    )
    assert emitter._connect_timeout_sec == 2
    assert emitter._read_timeout_sec == 4
