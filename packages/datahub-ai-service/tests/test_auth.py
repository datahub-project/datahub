import pytest
import requests
from datahub.configuration.common import GraphError
from helpers import FakeClientFactory, FakeGms, rejected_token_error

from datahub_ai_service.mcp.auth import (
    DataHubTokenVerifier,
    GmsUnavailableError,
    InvalidCredentialError,
    classify_gms_error,
)


class _Clock:
    now = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> _Clock:
    return _Clock()


@pytest.fixture
def verifier(gms: FakeGms, clock: _Clock) -> DataHubTokenVerifier:
    return DataHubTokenVerifier(
        FakeClientFactory(gms), cache_ttl_seconds=300, clock=clock
    )


async def test_verified_token_is_cached_until_the_ttl(
    gms: FakeGms, clock: _Clock, verifier: DataHubTokenVerifier
) -> None:
    caller = await verifier.verify("alice-token")
    clock.now = 299
    await verifier.verify("alice-token")

    assert caller.actor_urn == "urn:li:corpuser:alice"
    assert "alice-token" not in repr(caller)
    assert gms.me_calls == 1

    clock.now = 301
    await verifier.verify("alice-token")
    assert gms.me_calls == 2
    assert gms.open_clients == 0


async def test_rejections_and_outages_are_not_cached(
    gms: FakeGms, verifier: DataHubTokenVerifier
) -> None:
    with pytest.raises(InvalidCredentialError):
        await verifier.verify("carol-token")
    gms.users["carol-token"] = "urn:li:corpuser:carol"
    assert (await verifier.verify("carol-token")).actor_urn == "urn:li:corpuser:carol"

    gms.down = True
    with pytest.raises(GmsUnavailableError):
        await verifier.verify("bob-token")


def _status_error(status: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(response=response)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (rejected_token_error(), InvalidCredentialError),
        (_status_error(403), InvalidCredentialError),
        (GraphError("Unauthorized"), InvalidCredentialError),
        (_status_error(502), GmsUnavailableError),
        (_status_error(429), GmsUnavailableError),
        (requests.ConnectionError("refused"), GmsUnavailableError),
        (requests.Timeout("slow"), GmsUnavailableError),
        (ValueError("unexpected"), GmsUnavailableError),
    ],
)
def test_classify_gms_error(error: Exception, expected: type) -> None:
    assert isinstance(classify_gms_error(error), expected)
