"""Where the probe's secret-key catalog (agent.redact) and recording's
(recording.redaction) disagree. One masks what a caller reads, the other what
an archive keeps for replay, so they differ on purpose: a change to either that
moves a key on this list fails here, and the table says why each one differs.
"""

from typing import Dict, FrozenSet, Tuple

from datahub.ingestion.agent.redact import (
    SENSITIVE_KEY_HINTS,
    collect_nested_secret_values,
)
from datahub.ingestion.recording.redaction import is_secret_key

# key -> (the probe masks its value, recording redacts it).
DISAGREE: Dict[str, Tuple[bool, bool]] = {
    # Half of an OAuth pair to the probe. To recording an identifier, which a
    # recorded token request keeps.
    "client_id": (True, False),
    "managed_identity_client_id": (True, False),
    "oauth2_client_id": (True, False),
    # Replay needs the endpoint, and the token_type enum a source may check.
    "token_url": (True, False),
    "token_type": (True, False),
    # Credentials recording's patterns do not spell: an account key, Schema
    # Registry's `user:password`, a librdkafka PEM key, the `passwd` spelling.
    "account_key": (True, False),
    "adls.account-key": (True, False),
    "basic.auth.user.info": (True, False),
    "ssl.key": (True, False),
    "passwd": (True, False),
    # A key file's path, reached by the probe's `ssl.key` hint: over-masked.
    "ssl_keyfile": (True, False),
    # Not the credential but its path, to the probe's suffix rule; recording
    # matches `credential` anywhere in a key.
    "credentials_path": (False, True),
}
BOTH: FrozenSet[str] = frozenset(
    {
        "password",
        "sasl.password",
        "access_token",
        "client_secret",
        "api-key",
        "private-key",
        "private_key_id",
        "private_key_path",
        "credential",
        "s3.access-key-id",
        "s3.secret-access-key",
        "gcs.oauth2.token",
    }
)
# deploy_key, connection_string and consumer_key are typed SecretStr where a
# config declares them, which covers the probe, not recording.
NEITHER: FrozenSet[str] = frozenset(
    {
        "deploy_key",
        "connection_string",
        "consumer_key",
        "username",
        "project_id",
        "auth_type",
        "authenticator",
    }
)


def _probe_masks(key: str) -> bool:
    return collect_nested_secret_values({key: "v"}, SENSITIVE_KEY_HINTS) == {"v"}


def test_the_probe_and_recording_disagree_on_exactly_these_keys() -> None:
    verdicts = {
        key: (_probe_masks(key), is_secret_key(key))
        for key in [*DISAGREE, *BOTH, *NEITHER]
    }
    assert {k: v for k, v in verdicts.items() if v[0] != v[1]} == DISAGREE
    assert {k for k, v in verdicts.items() if v == (True, True)} == BOTH
    assert {k for k, v in verdicts.items() if v == (False, False)} == NEITHER
