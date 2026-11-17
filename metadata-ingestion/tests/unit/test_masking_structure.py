import json

from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry


def _filter_with(secrets):
    registry = SecretRegistry()
    for name, value in secrets.items():
        registry.register_secret(name, value)
    return SecretMaskingFilter(registry)


def test_masks_strings_at_every_nesting_level():
    masking_filter = _filter_with({"PW": "hunter2secret"})
    masked = masking_filter.mask_structure(
        {
            "source": {"failures": ["pw=hunter2secret", {"ctx": "hunter2secret"}]},
            "hunter2secret": 1,
            "count": 7,
        }
    )
    assert masked == {
        "source": {
            "failures": [
                "pw=***REDACTED:PW***",
                {"ctx": "***REDACTED:PW***"},
            ]
        },
        "***REDACTED:PW***": 1,
        "count": 7,
    }


def test_secret_with_json_escaped_characters_is_masked_before_serialization():
    secret = 'pä"ss\\word123'
    masking_filter = _filter_with({"PW": secret})
    serialized = json.dumps(masking_filter.mask_structure({"failure": f"got {secret}"}))
    assert secret not in serialized
    assert json.dumps(secret)[1:-1] not in serialized
    assert "***REDACTED:PW***" in serialized


def test_non_string_scalars_pass_through():
    masking_filter = _filter_with({})
    original = {"a": 1, "b": None, "c": 2.5, "d": True}
    assert masking_filter.mask_structure(original) == original


def test_colliding_masked_keys_keep_all_entries():
    registry = SecretRegistry()
    registry.register_secret("TOKEN", "old-token-value")
    registry.register_secret("TOKEN", "new-token-value")
    registry.register_secret("TOKEN", "mid-token-value")
    masking_filter = SecretMaskingFilter(registry)

    masked = masking_filter.mask_structure(
        {
            "pw=old-token-value": "a",
            "pw=new-token-value": "b",
            "pw=mid-token-value": "c",
        }
    )

    assert sorted(masked.values()) == ["a", "b", "c"]
    assert sorted(masked.keys()) == [
        "pw=***REDACTED:TOKEN***",
        "pw=***REDACTED:TOKEN*** (duplicate 2)",
        "pw=***REDACTED:TOKEN*** (duplicate 3)",
    ]
