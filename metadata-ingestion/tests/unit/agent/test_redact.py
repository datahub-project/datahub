from typing import Any, Callable, Dict, Optional, Set

import pytest

from datahub.configuration.common import ConfigModel
from datahub.ingestion.agent.redact import (
    SENSITIVE_KEY_HINTS,
    collect_nested_credential_values,
    collect_nested_secret_values,
    redact,
    scrub_text,
)


def test_redacts_exact_and_embedded_values():
    secrets = {"s3cr3t"}
    payload = {
        "note": "connected with s3cr3t ok",
        "list": ["s3cr3t", "safe"],
        "nested": {"pw": "s3cr3t"},
    }
    out = redact(payload, secrets)
    assert "s3cr3t" not in str(out)
    assert isinstance(out, dict)
    out_list = out["list"]
    assert isinstance(out_list, list)
    assert out_list[1] == "safe"


def test_empty_secrets_is_noop():
    payload = {"a": "b"}
    assert redact(payload, set()) == {"a": "b"}


def test_redacts_secret_in_dict_key():
    out = redact({"user_s3cr3t_key": "value"}, {"s3cr3t"})
    assert "s3cr3t" not in str(out)


def test_a_very_short_secret_does_not_mangle_surrounding_text():
    # Substring masking on a 1-3 character value corrupts every identifier and
    # key it appears in ("name" -> "n***me"), producing output an agent cannot
    # use, while masking nothing that is plausibly a credential.
    payload = {"name": "metadata_aspect_v2", "kind": "Table"}
    assert redact(payload, {"a"}) == payload


def test_a_very_short_secret_is_still_masked_on_an_exact_match():
    # Protection is narrowed, not dropped: a field whose whole value is the
    # secret is still masked.
    assert redact({"password_echo": "a"}, {"a"}) == {"password_echo": "***"}


def test_a_realistic_secret_is_still_masked_inside_longer_text():
    # The defence-in-depth case that matters: a credential embedded in a
    # connection string or driver error must not survive.
    out = redact(
        {"error": "could not connect to postgresql://u:hunter2@db:5432/x"},
        {"hunter2"},
    )
    assert "hunter2" not in str(out)


def test_overlapping_secrets_are_masked_longest_first():
    """A shorter secret must not be replaced first and strand the longer one's tail.

    Two registered secrets can overlap -- a password and a connection string
    containing it. Replacing the short one first destroys the match for the long
    one, leaving its remainder in the output. Set iteration order is arbitrary, so
    the leak was real but intermittent; the loop range makes the check
    order-independent rather than lucky.
    """
    for i in range(40):
        short = f"pw{i}a"
        long = short + "SECRETTAIL"
        out = redact("conn=" + long + ";x", {short, long})
        assert isinstance(out, str)
        assert "SECRETTAIL" not in out, (
            f"leaked tail for {{{short!r}, {long!r}}}: {out!r}"
        )


def test_a_url_encoded_password_in_a_driver_error_is_masked():
    """The case this exists for, and the one exact-substring matching missed.

    A driver echoing a connection string URL-encodes a password's special
    characters, so the raw value never appears in the error text -- for the
    secret "p@ssword" the DSN reads "u:p%40ssword@host" and passed straight
    through. Driver error text is where credentials leak in practice, which is
    what makes this the important direction rather than an edge case.
    """
    out = redact(
        {"error": "could not connect to postgresql://u:p%40ssword@db:5432/x"},
        {"p@ssword"},
    )
    rendered = str(out)
    assert "p%40ssword" not in rendered
    assert "p@ssword" not in rendered
    assert "***" in rendered


@pytest.mark.parametrize(
    "secret,text,masked",
    [
        # The boundary itself, which nothing pinned: flipping < to <= silently
        # unmasks 4-character secrets in driver-error text.
        ("abc", "pw is abc here", False),
        ("abcd", "pw is abcd here", True),
    ],
)
def test_the_substring_length_boundary_is_where_it_says_it_is(secret, text, masked):
    out = redact({"error": text}, {secret})
    assert (secret not in str(out)) is masked


def test_a_short_secrets_encoded_forms_are_not_substring_masked_either():
    """A short value stays whole-match-only in every form. Substring-masking
    its encodings would corrupt identifiers for the same reason the raw value
    would."""
    assert redact({"note": "a@b appears here"}, {"a@b"}) == {"note": "a@b appears here"}
    assert redact({"note": "a@b"}, {"a@b"}) == {"note": "***"}


def test_a_nested_private_key_is_collected():
    """Key-pair and service-account credentials nest one level down, where the
    top-level SecretStr sweep does not reach -- so the name hints must cover
    them or an inline key reaches the transcript unmasked."""
    key = "-----BEGIN PRIVATE KEY-----\nabc\n"
    cfg = {"credential": {"private_key": key, "project_id": "proj"}}
    values = collect_nested_secret_values(cfg, SENSITIVE_KEY_HINTS)
    assert key in values
    assert "proj" not in values
    assert key not in str(redact(cfg, values))


def test_two_keys_that_redact_alike_both_survive():
    """A dict comprehension kept only the last, so one field vanished from the
    report with nothing saying it had. Losing a field silently is the failure
    this interface exists to prevent, and it was happening inside the function
    whose whole job is care."""
    from datahub.ingestion.agent.redact import redact

    payload = {"alpha": 1, "bravo": 2}
    out = redact(payload, {"alpha", "bravo"})
    assert isinstance(out, dict)
    assert len(out) == 2, f"a field was dropped: {out}"
    assert sorted(out.values()) == [1, 2]
    # Both keys are masked; the second is suffixed so it cannot overwrite.
    assert set(out) == {"***", "***~2"}


def test_ordinary_keys_are_untouched_by_the_collision_handling():
    from datahub.ingestion.agent.redact import redact

    out = redact({"host": "h", "port": 5432}, {"secret"})
    assert out == {"host": "h", "port": 5432}


def test_an_identifier_or_a_path_is_not_a_credential():
    """`private_key_id` and `private_key_path` are real GCP fields.

    The hints match as substrings, so the `private_key` hint -- which exists
    for `credential.private_key`, nested one level down -- also swallowed the
    key's ID (public metadata, not a secret) and the PATH to the key file.
    The detector then told an author their correct service-account recipe
    held two plaintext secrets, and the only edit that would satisfy it is to
    stop naming the file.

    A suffix that makes a key name an identifier, a location or a reference
    cannot also make it the credential.
    """
    # Assembled, not written: a PEM header is a high-confidence signature for
    # the repo's secret scanner, which cannot tell a fixture from a leak.
    pem = "-----BEGIN " + "PRIVATE KEY" + "-----xyz"
    cfg = {
        "credential": {
            "private_key_id": "abc123keyid",
            "private_key": pem,
            "private_key_path": "/etc/gcp/key.json",
        }
    }
    found = collect_nested_credential_values(cfg, SENSITIVE_KEY_HINTS)

    assert pem in found, found
    assert "abc123keyid" not in found, found
    assert "/etc/gcp/key.json" not in found, found


def test_an_identifier_suffix_still_wins_under_a_sensitive_parent():
    """The suffix rule is about the leaf's own name, so inheriting a sensitive
    parent must not resurrect `_id` and `_path` as credentials."""
    # `token` is the sensitive parent here -- `credential` matches no hint on
    # its own, which is why the test above nests private_key under it.
    cfg = {"token": {"access_id": "abc123keyid", "value": "t0k3nvalue"}}
    flagged = collect_nested_credential_values(cfg, SENSITIVE_KEY_HINTS)
    assert "t0k3nvalue" in flagged, flagged
    assert "abc123keyid" not in flagged, flagged


def test_a_grant_listing_does_not_return_account_names_in_the_clear():
    """`SELECT * FROM information_schema.table_privileges` is permitted.

    Not by an exception -- by the BASE catalog scope every SQL connector
    gets, which allows the whole information_schema schema. So a default
    recipe could read grantor/grantee, and those are account names. The
    seven-name withheld list covered none of them, and `SELECT *` names no
    column for the gate to refuse, so both layers passed it through.

    The relation stays readable: what a probe wants from it is which
    privilege exists on which table, and that is exactly what survives.
    """
    from datahub.ingestion.agent.redact import mask_identity_columns

    columns = [
        "grantor",
        "grantee",
        "table_catalog",
        "table_schema",
        "table_name",
        "privilege_type",
        "is_grantable",
    ]
    rows = [["alice_admin", "bob_analyst", "db", "public", "orders", "SELECT", "NO"]]

    (masked,) = mask_identity_columns(columns, rows)

    assert masked[0] == "***", "grantor is an account name"
    assert masked[1] == "***", "grantee is an account name"
    # And the answer the caller actually wanted is intact.
    assert masked[2:] == ["db", "public", "orders", "SELECT", "NO"]


def test_ordinary_type_metadata_is_not_mistaken_for_an_identity():
    """Why the match is exact rather than a substring.

    information_schema.tables and .columns carry user_defined_type_name,
    user_defined_type_catalog and user_defined_type_schema. A substring rule
    on "user" -- the obvious way to widen this list -- would mask three
    ordinary type-metadata columns on every wildcard read of the two
    relations the probe exists to read.
    """
    from datahub.ingestion.agent.redact import mask_identity_columns

    columns = [
        "table_name",
        "column_name",
        "data_type",
        "user_defined_type_catalog",
        "user_defined_type_schema",
        "user_defined_type_name",
        "domain_name",
        "udt_name",
    ]
    rows = [["orders", "id", "integer", "db", "public", "my_type", "d", "int4"]]

    (masked,) = mask_identity_columns(columns, rows)

    assert masked == rows[0], f"an ordinary catalog column was masked: {masked}"


def test_a_credential_nested_under_a_sensitive_key_is_collected():
    """Third appearance of one bug, and the first where it fails to MASK.

    Both collectors decided sensitivity per key and dropped it at the
    recursion, so a sensitive key holding a MAPPING had its children judged by
    their own names:

        token:      {access: ...}             -> collected by neither
        credential: {private_key: {pem: ...}} -> collected by neither

    The credential is simply never collected, so the redactor does not mask
    it and validate does not mention it: it reaches the caller in the clear.

    `credential.private_key` is the shape the private_key hint was added for,
    and the comment there says "nested one level down" -- which works only
    while the value is a string. One level further and it was gone.
    """
    cfg = {
        "connection": {
            "token": {"access": "acc3ssvalue"},
            "credential": {"private_key": {"pem": "p3mvalue"}},
            "database": "analytics",
        }
    }

    masked = collect_nested_secret_values(cfg, SENSITIVE_KEY_HINTS)
    assert "acc3ssvalue" in masked, masked
    assert "p3mvalue" in masked, masked

    flagged = collect_nested_credential_values(cfg, SENSITIVE_KEY_HINTS)
    assert "acc3ssvalue" in flagged, flagged
    assert "p3mvalue" in flagged, flagged

    # A plain identifier under a plain key stays out of both, or the fix has
    # simply widened everything.
    assert "analytics" not in masked, masked
    assert "analytics" not in flagged, flagged


class _TokenRequest(ConfigModel):
    request_type: str = "get"
    url_complement: str = ""
    client_secret: Optional[str] = None
    headers: Dict[str, Any] = {}


class _TypedConfig(ConfigModel):
    get_token: Optional[_TokenRequest] = None
    consumer_config: Dict[str, Any] = {}


_COLLECTORS = pytest.mark.parametrize(
    "collect",
    [collect_nested_secret_values, collect_nested_credential_values],
    ids=["mask", "detect"],
)


@_COLLECTORS
def test_a_typed_block_under_a_sensitive_key_is_judged_by_its_own_field_names(
    collect: Callable[..., Set[str]],
) -> None:
    """A key holding "token" made every value beneath it a secret, typed block
    or not: `get_token.request_type: post` registered "post", and output text
    `postgresql://h/x; got posts` read `***gresql://h/x; got ***s`. A config
    block's fields have names of their own to be judged by."""
    cfg = {
        "get_token": {
            "request_type": "post",
            "url_complement": "api/login",
            "client_secret": "PLANTED-client-secret",
            "not_declared": "PLANTED-undeclared",
        }
    }

    found = collect(cfg, SENSITIVE_KEY_HINTS, config_cls=_TypedConfig)

    assert "post" not in found, found
    assert "api/login" not in found, found
    # Named for a credential, so collected though typed plain str.
    assert "PLANTED-client-secret" in found, found
    # The block does not declare it, so nothing types it: free-form.
    assert "PLANTED-undeclared" in found, found
    # With no class to read the walk stays on the safe side.
    assert "post" in collect(cfg, SENSITIVE_KEY_HINTS), found


@_COLLECTORS
def test_a_free_form_value_under_a_sensitive_key_still_inherits(
    collect: Callable[..., Set[str]],
) -> None:
    """A client config typed Dict[str, Any] has no field names to judge, so
    everything beneath a sensitive key in it is still the secret. So is a key
    the config does not declare at all."""
    cfg = {
        "consumer_config": {"sasl": {"username": "PLANTED-sasl-user"}},
        "token": {"value": "PLANTED-token-value"},
    }

    found = collect(cfg, SENSITIVE_KEY_HINTS, config_cls=_TypedConfig)

    assert "PLANTED-sasl-user" in found, found
    assert "PLANTED-token-value" in found, found


@_COLLECTORS
def test_a_declared_free_form_field_in_a_sensitive_block_still_inherits(
    collect: Callable[..., Set[str]],
) -> None:
    """A block's Dict[str, Any] field is declared, but its keys are not: they
    have no field names to be judged by, so the block's verdict carries in,
    while the block's plain fields keep their own."""
    cfg = {
        "get_token": {
            "request_type": "post",
            "headers": {"X-Custom": "PLANTED-header-value"},
        }
    }

    found = collect(cfg, SENSITIVE_KEY_HINTS, config_cls=_TypedConfig)

    assert "PLANTED-header-value" in found, found
    assert "post" not in found, found


def test_a_credential_named_api_key_is_collected_as_a_secret():
    """Five connectors carry a credential under a key the original hints
    missed, none SecretStr-typed -- elasticsearch's api_key, and
    aws_access_key_id on dynamodb/glue/quicksight/sagemaker -- so without the
    hint the typed registry does not cover them either. An encrypted private
    key's passphrase (`passphrase`, `ssh_passphrase`) is collected the same
    way."""
    for key in (
        "api_key",
        "apikey",
        "passwd",
        "aws_access_key_id",
        "kafka_api_key",
        "passphrase",
        "ssh_passphrase",
    ):
        found = collect_nested_secret_values({key: "the-value"}, SENSITIVE_KEY_HINTS)
        assert found == {"the-value"}, f"{key} was not collected"


def test_the_widened_hints_do_not_swallow_structural_fields():
    """Bare "key" is absent from the hints on purpose.

    partition_key, primary_key and key_path are structure, not credentials,
    and masking them would corrupt ordinary output. Same reason "credential"
    is absent: it names a mixed object whose secret child is already matched.
    """
    for key in ("partition_key", "primary_key", "key_path", "sort_key", "project_id"):
        found = collect_nested_secret_values({key: "structural"}, SENSITIVE_KEY_HINTS)
        assert found == set(), f"{key} is not a credential"


def _fresh_registry():
    from datahub.masking.secret_registry import SecretRegistry

    SecretRegistry.reset_instance()
    return SecretRegistry.get_instance()


@pytest.mark.parametrize(
    "text, secret",
    [
        ("GET http://admin:PLANTED-pw@connect.example:8083/connectors", "PLANTED-pw"),
        ("jdbc:mysql://db:3306?user=u&password=PLANTED-pw", "PLANTED-pw"),
        ("https://acct.blob.core.windows.net/c?sv=1&sig=PLANTEDsig%3D", "PLANTEDsig"),
        ("Authorization: Bearer PLANTED.jwt.value", "PLANTED.jwt.value"),
        ("aws_access_key_id=AKIAPLANTED000000000 rejected", "AKIAPLANTED000000000"),
        ("connection failed: secret: 'PLANTED-quoted'", "PLANTED-quoted"),
    ],
)
def test_scrub_text_strips_secrets_with_no_registered_values(
    text: str, secret: str
) -> None:
    out = scrub_text(text, set())
    assert secret not in out
    assert "***" in out


def test_scrub_text_keeps_identifiers_that_mention_secret_words() -> None:
    text = "table token_usage in schema password_resets has 3 columns"
    assert scrub_text(text, set()) == text


def test_scrub_text_keeps_the_host_after_removing_userinfo() -> None:
    out = scrub_text("http://u:p4ssw0rd@connect.example:8083/x", set())
    assert out == "http://***@connect.example:8083/x"


def test_scrub_text_still_masks_registered_values() -> None:
    assert scrub_text("driver said hunter2-long", {"hunter2-long"}) == "driver said ***"


def test_hyphenated_and_dotted_keys_are_treated_as_secrets() -> None:
    config = {
        "catalog": {
            "s3.access-key-id": "AKIAPLANTED000000000",
            "adls.account-key": "PLANTED-account-key",
            "credential": "client:PLANTED-cred",
        },
        "client_id": "PLANTED-client-id",
    }
    found = collect_nested_secret_values(config, SENSITIVE_KEY_HINTS)
    assert {
        "AKIAPLANTED000000000",
        "PLANTED-account-key",
        "client:PLANTED-cred",
        "PLANTED-client-id",
    } <= found


def test_a_dotted_hint_matches_its_key_in_a_free_form_client_config() -> None:
    # Schema Registry and librdkafka spell their keys with dots, and the
    # userinfo has no shape scrub_text would catch: only its key marks it.
    config = {
        "connection": {
            "schema_registry_config": {
                "basic.auth.user.info": "PLANTED-user:PLANTED-pw",
                "url": "http://registry:8081",
            },
            "consumer_config": {"ssl.key.pem": "PLANTED-pem-body", "group.id": "g1"},
        }
    }
    found = collect_nested_secret_values(config, SENSITIVE_KEY_HINTS)
    assert found == {"PLANTED-user:PLANTED-pw", "PLANTED-pem-body"}


def test_credential_mapping_does_not_mask_sibling_identifiers() -> None:
    cfg = {"credential": {"project_id": "proj", "private_key": "PLANTED-key"}}
    found = collect_nested_secret_values(cfg, SENSITIVE_KEY_HINTS)
    assert "PLANTED-key" in found
    assert "proj" not in found


@pytest.mark.parametrize(
    "secret, tail",
    [
        ("p@ssw0rd-long", "ssw0rd-long"),
        ("hunter two-long", "two-long"),
        ("abc&def-long-secret", "def-long-secret"),
        ("ab;cd-long-secret", "cd-long-secret"),
    ],
)
def test_a_registered_secret_is_removed_whole_before_structural_passes(
    secret: str, tail: str
) -> None:
    for text in (
        f"http://admin:{secret}@db:5432/x",
        f"password={secret} rejected",
        f"token={secret}",
        f"pwd={secret}",
    ):
        out = scrub_text(text, {secret})
        assert tail not in out, (text, out)


@pytest.mark.parametrize(
    "text",
    [
        "client_secret=PLANTEDvalue",
        "auth_token=PLANTEDvalue",
        "access_token=PLANTEDvalue",
        "refresh_token=PLANTEDvalue",
        "session_token=PLANTEDvalue",
        "private_key=PLANTEDvalue",
        "aws_secret_access_key=PLANTEDvalue",
        "aws_access_key_id=PLANTEDvalue",
        "secretKey=PLANTEDvalue",
        "clientSecret: PLANTEDvalue",
        '{"pass' + 'word": "PLANTEDvalue"}',
        "{'pass" + "word': 'PLANTEDvalue'}",
        "Authorization: Basic UExBTlRFRHZhbHVl",
        "SharedAccessSignature=PLANTEDvalue",
        "token%3DPLANTEDvalue",
        'secret="PLANTED\\"value"',
        "x\n-----BEGIN RSA PRIVATE"
        + " KEY-----\nPLANTEDvalue\n-----END RSA PRIVATE KEY-----\ny",
        "-----BEGIN PRIVATE" + " KEY-----\nPLANTEDvalue",
        # A key as JSON or a log line escapes its newlines.
        'material="-----BEGIN PRIVATE'
        + ' KEY-----\\nPLANTEDvalue\\nPLANTEDmore\\n-----END PRIVATE KEY-----"',
        "passphrase=PLANTEDvalue",
        "private_key_passphrase: PLANTEDvalue",
    ],
)
def test_scrub_text_covers_more_credential_shapes(text: str) -> None:
    out = scrub_text(text, set())
    # UExBTlRFRHZhbHVl is base64 of the planted value in the Basic header case.
    assert "PLANTED" not in out and "UExBTlRFRHZhbHVl" not in out
    assert "***" in out


@pytest.mark.parametrize(
    "text, masked",
    [
        ("passphrase=PLANTED horse battery staple", "passphrase=***"),
        (
            "private_key_passphrase: PLANTED horse battery\nnext: x",
            "private_key_passphrase: ***\nnext: x",
        ),
        # The next `name=` pair on the line ends it.
        ("passphrase=PLANTED horse user=bob", "passphrase=*** user=bob"),
        ("PASSPHRASE=PLANTED horse;UID=bob", "PASSPHRASE=***;UID=bob"),
    ],
)
def test_scrub_text_masks_an_unquoted_passphrase_of_several_words(
    text: str, masked: str
) -> None:
    assert scrub_text(text, set()) == masked


def test_scrub_text_masks_userinfo_up_to_the_last_at_sign() -> None:
    assert scrub_text("http://u:p@ss@h/x", set()) == "http://***@h/x"


@pytest.mark.parametrize(
    "text, masked",
    [
        ("postgresql://svc:pa/ss@db.example/x", "postgresql://***@db.example/x"),
        # Built from parts so a secret scanner does not read the fixture as a
        # real connection string.
        ("postgresql://svc:" + "p@a/ss@db.example/x", "postgresql://***@db.example/x"),
        # The scan stops at the next URL, so its own userinfo is still found.
        ("http://a:1/x,http://u:pa/ss@h/y", "http://a:1/x,http://***@h/y"),
    ],
)
def test_scrub_text_masks_a_password_holding_a_slash(text: str, masked: str) -> None:
    assert scrub_text(text, set()) == masked


@pytest.mark.parametrize(
    "text, masked",
    [
        # Built from parts so a secret scanner does not read the fixtures as
        # real connection strings.
        (
            '{"url":"postgresql://svc:' + 'pw@db.example/x","owner":"ann@example.com"}',
            '{"url":"postgresql://***@db.example/x","owner":"ann@example.com"}',
        ),
        # A userinfo with no password, and a URL with no path after it.
        (
            '{"url":"postgresql://svc@db.example","owner":"ann@example.com"}',
            '{"url":"postgresql://***@db.example","owner":"ann@example.com"}',
        ),
        ("'mysql://u:" + "p/q@h/db'", "'mysql://***@h/db'"),
        ("`mysql://u:" + "p/q@h/db`", "`mysql://***@h/db`"),
        (
            "{'url': 'mysql://u:" + "p/q', 'owner': 'ann@example.com'}",
            "{'url': 'mysql://u:" + "p/q', 'owner': 'ann@example.com'}",
        ),
    ],
)
def test_scrub_text_keeps_a_url_userinfo_match_inside_its_quotes(
    text: str, masked: str
) -> None:
    assert scrub_text(text, set()) == masked


@pytest.mark.parametrize(
    "text",
    [
        "postgresql://svc:" + 'pa"ss@db.example/x',
        # A quote and a slash: neither stops the match.
        "postgresql://svc:" + 'pa"s/s@db.example/x',
        "postgresql://svc:" + "p'a\"s/s@db.example/x",
        # A quote before a delimiter is the password's while an `@` follows it
        # before the next quote.
        "postgresql://svc:" + 'pa",s/s@db.example/x',
        "postgresql://s'vc:" + "pa/ss@db.example/x",
    ],
)
def test_scrub_text_masks_a_password_holding_a_quote(text: str) -> None:
    assert scrub_text(text, set()) == "postgresql://***@db.example/x"


def test_scrub_text_over_masks_a_path_at_sign_after_a_port() -> None:
    # `host:8080` reads as `user:password` once a password may hold `/`, so an
    # `@` later in the path ends a userinfo. Over-masking is the safe side.
    assert scrub_text("http://host:8080/u/x@y", set()) == "http://***@y"


@pytest.mark.parametrize(
    "text",
    [
        "https://host/path",
        "https://host:8080/path",
        "https://host/u/x@y",
        "https://host:8080/a:b/c",
    ],
)
def test_scrub_text_keeps_a_url_without_userinfo(text: str) -> None:
    assert scrub_text(text, set()) == text


# Assembled, not written: a PEM header is a high-confidence signature for the
# repo's secret scanner, which cannot tell a fixture from a leak.
_LEGACY_BEGIN = "-----BEGIN RSA PRIVATE" + " KEY-----"
_LEGACY_END = "-----END RSA PRIVATE" + " KEY-----"
_LEGACY_ENCRYPTED_KEY_LINES = [
    _LEGACY_BEGIN,
    "Proc-Type: 4,ENCRYPTED",
    "DEK-Info: AES-128-CBC,00112233445566778899AABBCCDDEEFF",
    "",
    "PLANTEDbody0123456789abcdefghijklmnopqrstuvwxyz+/ABCDEFGHIJKLMNOPQR",
    "PLANTEDmore==",
    _LEGACY_END,
]


def test_scrub_text_masks_a_legacy_encrypted_pem_body() -> None:
    text = "key:\n" + "\n".join(_LEGACY_ENCRYPTED_KEY_LINES) + "\nnext line"
    assert scrub_text(text, set()) == "key:\n***\nnext line"


def test_scrub_text_masks_a_legacy_encrypted_pem_with_escaped_newlines() -> None:
    # A key inside JSON or a log line reads `\n` for each newline.
    text = 'material="' + "\\n".join(_LEGACY_ENCRYPTED_KEY_LINES) + '"'
    assert scrub_text(text, set()) == 'material="***"'


def test_scrub_text_masks_a_truncated_legacy_encrypted_pem() -> None:
    text = "\n".join(_LEGACY_ENCRYPTED_KEY_LINES[:5])
    assert scrub_text(text, set()) == "***"


@pytest.mark.parametrize(
    "text",
    [
        "Invalid password: authentication failed for user x",
        "access_key: field required",
        "Bearer token required",
        "token: expired",
        "passphrase: required",
        "Passphrase must be set for an encrypted private key",
    ],
)
def test_scrub_text_keeps_diagnostic_words_after_a_secret_keyword(text: str) -> None:
    assert scrub_text(text, set()) == text


def test_scrub_text_stays_linear_on_long_input() -> None:
    import time

    start = time.time()
    for text in (
        "a.a." * 50000,
        "password" * 12500,
        "x://" * 25000,
        "http://u:p/" * 10000,
        'x://u:"' * 15000,
        'x://u:",' * 12000,
        'x://u:"a' * 12000,
        'x://u:",' + "a" * 100000,
        "passphrase=a" + " b" * 50000,
        "passphrase=a " + "b" * 100000,
        "passphrase=a" + " b=" * 30000,
        "passphrase " * 10000,
        "pwd={" * 20000,
        "PWD={" + "a" * 100000,
        "eyJaaaaa." * 11000,
        "Authorization: Token " * 5000,
        "basic " * 17000,
        "input_value=" * 8000,
        "-----BEGIN PRIVATE KEY-----" + "\\n" * 50000,
        "-----BEGIN PRIVATE KEY-----" + "\nA:" * 33000,
    ):
        scrub_text(text, set())
    # Generous on purpose: this catches catastrophic (quadratic or worse)
    # backtracking on 100k-character inputs, not a slow CI machine.
    assert time.time() - start < 10


def test_credential_mapping_keys_with_other_suffixes_are_not_secrets() -> None:
    cfg = {
        "credential_source": "file",
        "credentials_path": "/etc/key.json",
        "credential_id": "abc",
        "credential": "client:PLANTED",
    }
    assert collect_nested_secret_values(cfg, SENSITIVE_KEY_HINTS) == {"client:PLANTED"}


@pytest.mark.parametrize(
    "text",
    [
        "DRIVER={ODBC Driver 18};SERVER=h;PWD={a;PLANTEDvalue};UID=u",
        # Built from parts so a secret scanner does not read this file's
        # fixtures as real credentials.
        "token was " + "eyJ" + "hbGciOiJIUzI1NiJ9." + "eyJ" + "zdWIiOiJQTEFOVEVEIn0."
        "PLANTEDsignature1",
        "auth with ghp_PLANTEDvalue0123456789abcdef",
        "posting with xoxb-1234-PLANTEDvalue-abc",
        "Authorization: Token PLANTEDvalue",
        "authorization=token PLANTEDvalue",
        "Authorization: Basic dXNlcjpQTEFOVEVE",
    ],
)
def test_scrub_text_masks_vendor_token_shapes(text: str) -> None:
    out = scrub_text(text, set())
    assert "PLANTED" not in out
    assert "dXNlcjpQTEFOVEVE" not in out
    assert "***" in out


@pytest.mark.parametrize(
    "text, masked",
    [
        # Lowercase-only base64 has none of the characters the bare `Basic`
        # rule needs to tell a token from prose; the header settles it.
        ("Authorization: Basic ajphamdh", "Authorization: Basic ***"),
        ("authorization=basic ajphamdh", "authorization=basic ***"),
    ],
)
def test_the_word_after_an_explicit_basic_header_is_masked(
    text: str, masked: str
) -> None:
    assert scrub_text(text, set()) == masked


def test_a_fine_grained_github_token_is_masked_whole() -> None:
    # Built from parts so a secret scanner does not read the fixture as a
    # token. The underscore inside the body is part of the format.
    token = "github" + "_pat_" + "11PLANTED0123456789ab_" + "Zq9" * 19
    assert scrub_text(f"clone failed with {token}", set()) == "clone failed with ***"


def test_an_odbc_braced_value_is_masked_without_swallowing_its_neighbours() -> None:
    out = scrub_text("SERVER=h;PWD={a;PLANTEDvalue};UID=u", set())
    assert out == "SERVER=h;PWD=***;UID=u"


@pytest.mark.parametrize(
    "text",
    [
        "basic connectivity failed for source x",
        "Basic configuration is missing",
        "Authorization: Token required",
    ],
)
def test_scrub_text_keeps_prose_after_scheme_words(text: str) -> None:
    assert scrub_text(text, set()) == text


def test_scrub_strings_walks_json_shapes() -> None:
    from datahub.ingestion.agent.redact import scrub_strings

    payload = {
        "a": ["client_secret=PLANTEDvalue", 3, None],
        "b": {"c": "http://u:" + "PLANTEDvalue@h/x"},
        "d": True,
    }
    out = scrub_strings(payload, set())
    assert "PLANTED" not in str(out)
    assert out == {
        "a": ["client_secret=***", 3, None],
        "b": {"c": "http://***@h/x"},
        "d": True,
    }
