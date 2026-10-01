from typing import Dict

import pytest

from datahub.ingestion.agent.redact import (
    _SENSITIVE_KEY_HINTS,
    collect_nested_credential_values,
    collect_nested_secret_values,
    collect_secret_values,
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


def test_collect_secret_values_only_from_secret_fields():
    resolved: Dict[str, object] = {"password": "s3cr3t", "host_port": "db:3306"}
    values = collect_secret_values(resolved, {"password"})
    assert values == {"s3cr3t"}


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
    values = collect_nested_secret_values(cfg, _SENSITIVE_KEY_HINTS)
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
    found = collect_nested_credential_values(cfg, _SENSITIVE_KEY_HINTS)

    assert pem in found, found
    assert "abc123keyid" not in found, found
    assert "/etc/gcp/key.json" not in found, found


def test_an_identifier_suffix_still_wins_under_a_sensitive_parent():
    """The suffix rule is about the leaf's own name, so inheriting a sensitive
    parent must not resurrect `_id` and `_path` as credentials."""
    # `token` is the sensitive parent here -- `credential` matches no hint on
    # its own, which is why the test above nests private_key under it.
    cfg = {"token": {"access_id": "abc123keyid", "value": "t0k3nvalue"}}
    flagged = collect_nested_credential_values(cfg, _SENSITIVE_KEY_HINTS)
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

    masked = collect_nested_secret_values(cfg, _SENSITIVE_KEY_HINTS)
    assert "acc3ssvalue" in masked, masked
    assert "p3mvalue" in masked, masked

    flagged = collect_nested_credential_values(cfg, _SENSITIVE_KEY_HINTS)
    assert "acc3ssvalue" in flagged, flagged
    assert "p3mvalue" in flagged, flagged

    # A plain identifier under a plain key stays out of both, or the fix has
    # simply widened everything.
    assert "analytics" not in masked, masked
    assert "analytics" not in flagged, flagged


def test_a_credential_named_api_key_is_collected_as_a_secret():
    """Five connectors carry a credential under a key the original hints
    missed, none SecretStr-typed -- elasticsearch's api_key, and
    aws_access_key_id on dynamodb/glue/quicksight/sagemaker -- so without the
    hint the typed registry does not cover them either."""
    from datahub.ingestion.agent.redact import SENSITIVE_KEY_HINTS

    for key in ("api_key", "apikey", "passwd", "aws_access_key_id", "kafka_api_key"):
        found = collect_nested_secret_values({key: "the-value"}, SENSITIVE_KEY_HINTS)
        assert found == {"the-value"}, f"{key} was not collected"


def test_the_widened_hints_do_not_swallow_structural_fields():
    """Bare "key" is absent from the hints on purpose.

    partition_key, primary_key and key_path are structure, not credentials,
    and masking them would corrupt ordinary output. Same reason "credential"
    is absent: it names a mixed object whose secret child is already matched.
    """
    from datahub.ingestion.agent.redact import SENSITIVE_KEY_HINTS

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
    found = collect_nested_secret_values(config, _SENSITIVE_KEY_HINTS)
    assert {
        "AKIAPLANTED000000000",
        "PLANTED-account-key",
        "client:PLANTED-cred",
        "PLANTED-client-id",
    } <= found


def test_credential_mapping_does_not_mask_sibling_identifiers() -> None:
    cfg = {"credential": {"project_id": "proj", "private_key": "PLANTED-key"}}
    found = collect_nested_secret_values(cfg, _SENSITIVE_KEY_HINTS)
    assert "PLANTED-key" in found
    assert "proj" not in found
