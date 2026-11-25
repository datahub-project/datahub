from typing import Iterator, List

import pytest
import sentry_sdk
from sentry_sdk.consts import DEFAULT_MAX_VALUE_LENGTH
from sentry_sdk.envelope import Envelope
from sentry_sdk.transport import Transport

from datahub.masking.secret_registry import SecretRegistry
from datahub.telemetry import telemetry
from datahub.telemetry.telemetry import _init_sentry, _mask_sentry_event

SECRET = "s3cr3t-test-value"
UNREGISTERED = "unregistered-test-value"


class _CapturingTransport(Transport):
    def __init__(self) -> None:
        super().__init__()
        self.envelopes: List[Envelope] = []

    def capture_envelope(self, envelope: Envelope) -> None:
        self.envelopes.append(envelope)


@pytest.fixture
def registry() -> Iterator[SecretRegistry]:
    registry = SecretRegistry.get_instance()
    registry.clear()
    yield registry
    registry.clear()


class _Config:
    def __repr__(self) -> str:
        return f"Config(url=postgresql://user:{SECRET}@host/db)"


def _connect(url: str, message: str) -> None:
    endpoint = UNREGISTERED  # noqa: F841
    raise ConnectionError(message)


def _send_failed_connection(
    monkeypatch: pytest.MonkeyPatch, message: str = "unable to connect"
) -> bytes:
    """Capture an error through the telemetry Sentry setup; return what was sent."""
    transport = _CapturingTransport()
    real_init = sentry_sdk.init
    monkeypatch.setattr(telemetry, "SENTRY_DSN", "https://key@example.invalid/1")
    monkeypatch.setattr(
        sentry_sdk,
        "init",
        lambda **options: real_init(
            **options, transport=transport, default_integrations=False
        ),
    )
    _init_sentry()
    try:
        with sentry_sdk.isolation_scope():
            sentry_sdk.add_breadcrumb(message=f"connecting with {SECRET}")
            sentry_sdk.set_extra("config", _Config())
            try:
                _connect(f"postgresql://user:{SECRET}@host/db", message)
            except ConnectionError as e:
                sentry_sdk.capture_exception(e)
    finally:
        sentry_sdk.get_client().close()
        sentry_sdk.get_global_scope().set_client(None)
    return b"".join(envelope.serialize() for envelope in transport.envelopes)


def test_sentry_event_masks_registered_secrets_and_drops_locals(
    registry: SecretRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry.register_secret("DB_PASSWORD", SECRET)

    sent = _send_failed_connection(monkeypatch)

    assert b"unable to connect" in sent
    assert b"connecting with ***REDACTED:DB_PASSWORD***" in sent
    assert b"Config(url=postgresql://user:***REDACTED:DB_PASSWORD***" in sent
    assert SECRET.encode() not in sent
    assert UNREGISTERED.encode() not in sent


def test_sentry_event_masks_a_secret_at_the_length_limit(
    registry: SecretRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry.register_secret("DB_PASSWORD", SECRET)

    sent = _send_failed_connection(
        monkeypatch, "x" * (DEFAULT_MAX_VALUE_LENGTH - 6) + SECRET
    )

    assert b"xxxxx" in sent
    assert SECRET[:3].encode() not in sent


def test_sentry_event_is_dropped_while_masking_fails_closed(
    registry: SecretRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(SecretRegistry, "MAX_SECRETS", 1)
    registry.register_secrets_batch({"DB_PASSWORD": SECRET, "DB_USER": "admin-user"})

    assert _mask_sentry_event({"event_id": "1", "message": SECRET}, {}) is None
