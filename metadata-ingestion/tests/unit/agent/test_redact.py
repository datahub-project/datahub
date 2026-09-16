from typing import Dict

import pytest

from datahub.ingestion.agent.redact import (
    _SENSITIVE_KEY_HINTS,
    collect_nested_secret_values,
    collect_secret_values,
    redact,
)
from datahub.masking.secret_registry import plain_config_values


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


def test_a_secret_under_a_sensitive_parent_is_not_treated_as_disclosed():
    """The disclosure exemption must not reach INTO a sensitive subtree.

    plain_config_values exempts values a recipe states in the clear under a
    non-sensitive key, because masking one of those only corrupts output. It
    decided that per key while walking, but forgot the decision on the way
    down: a sensitive key whose value was a mapping recursed without carrying
    `sensitive`, so `token: {access: ...}` was judged by the inner key
    `access` -- which no hint matches -- and the credential came back as
    "already disclosed", meaning it would not be masked anywhere.

    Nothing under a sensitive key is public, whatever the child keys are
    called, so the subtree is skipped entirely.
    """
    assert (
        plain_config_values(
            {"token": {"access": "acc3ssvalue", "refresh": "r3freshvalue"}},
            _SENSITIVE_KEY_HINTS,
        )
        == set()
    )

    assert (
        plain_config_values(
            {"secret": {"outer": {"inner": "deepvalue"}}}, _SENSITIVE_KEY_HINTS
        )
        == set()
    )

    # The converse, so the fix cannot become "exempt nothing": a plain
    # identifier under a plain key is still disclosed, which is the whole
    # point of the exemption.
    assert plain_config_values({"database": "analytics"}, _SENSITIVE_KEY_HINTS) == {
        "analytics"
    }
    assert plain_config_values(
        {"connection": {"database": "analytics"}}, _SENSITIVE_KEY_HINTS
    ) == {"analytics"}
