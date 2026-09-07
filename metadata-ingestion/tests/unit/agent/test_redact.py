from typing import Dict

from datahub.ingestion.agent.redact import collect_secret_values, redact


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
