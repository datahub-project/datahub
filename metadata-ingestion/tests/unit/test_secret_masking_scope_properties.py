"""Property test: a task scope masks exactly as one registry would.

Task scoping replaced a single process-global registry with a scope that
masks against its own secrets plus the global's. The single registry is the
reference: for any split of the same secrets between the global and a scope,
masking inside the scope must produce what one registry holding all of them
produces -- on the first pass and on a second one, since executor output is
masked more than once. The one regression found after this was built (the
combined pattern dropping the marker and sentinel alternatives) is exactly a
disagreement between the two.
"""

from typing import Dict, List, Tuple

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from datahub.masking.constants import REDACTED_PREFIX, SENTINEL_MESSAGES
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry, task_secret_scope

# Characters that make secrets collide with each other, with marker text and
# with sentinels: the cases a naive pattern gets wrong.
_ALPHABET = "abcPASSWORD_:*-[]"
_VALUE = st.text(alphabet=_ALPHABET, min_size=4, max_size=10)
_NAME = st.text(alphabet="ABCDEFGHIJ_", min_size=1, max_size=6)


@st.composite
def _slice_of(draw: st.DrawFn, sources: List[str]) -> str:
    source = draw(st.sampled_from(sources))
    start = draw(st.integers(0, max(0, len(source) - 4)))
    length = draw(st.integers(4, 12))
    return source[start : start + length]


@st.composite
def _secrets_and_text(draw: st.DrawFn) -> Tuple[Dict[str, str], List[bool], str]:
    names = draw(st.lists(_NAME, min_size=1, max_size=5, unique=True))
    # Some values are slices of marker and sentinel text: a secret that is a
    # substring of an existing marker (`password` inside
    # `***REDACTED:snowflake_password***`) is what breaks a pattern that does
    # not consume markers whole, and a random alphabet almost never draws one.
    markers = [f"{REDACTED_PREFIX}{n}***" for n in names] + list(SENTINEL_MESSAGES)
    value = st.one_of(_VALUE, _slice_of(markers))
    values = draw(
        st.lists(value, min_size=len(names), max_size=len(names), unique=True).filter(
            lambda vs: all(len(v) >= 4 for v in vs)
        )
    )
    secrets = dict(zip(names, values, strict=True))
    in_scope = draw(st.lists(st.booleans(), min_size=len(values), max_size=len(values)))
    pieces = st.one_of(
        st.sampled_from(values),
        st.sampled_from([f"{REDACTED_PREFIX}{n}***" for n in names]),
        st.sampled_from(list(SENTINEL_MESSAGES)),
        st.text(alphabet=_ALPHABET + " ", max_size=8),
    )
    text = "".join(draw(st.lists(pieces, max_size=8)))
    return secrets, in_scope, text


@pytest.fixture(autouse=True)
def _clean_registry():
    SecretRegistry.global_instance().clear()
    yield
    SecretRegistry.global_instance().clear()


@settings(max_examples=400, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_secrets_and_text())
def test_a_scope_masks_exactly_as_one_registry_would(
    case: Tuple[Dict[str, str], List[bool], str],
) -> None:
    secrets, in_scope, text = case

    reference = SecretRegistry()
    reference.register_secrets_batch(secrets)
    expected_mask = SecretMaskingFilter(secret_registry=reference)
    once = expected_mask.mask_text(text)
    twice = expected_mask.mask_text(once)

    SecretRegistry.global_instance().clear()
    scoped = {n: v for (n, v), s in zip(secrets.items(), in_scope, strict=True) if s}
    process_level = {
        n: v for (n, v), s in zip(secrets.items(), in_scope, strict=True) if not s
    }
    SecretRegistry.global_instance().register_secrets_batch(process_level)
    with task_secret_scope():
        SecretRegistry.get_instance().register_secrets_batch(scoped)
        masking = SecretMaskingFilter()
        scoped_once = masking.mask_text(text)
        scoped_twice = masking.mask_text(scoped_once)

    assert scoped_once == once
    assert scoped_twice == twice
