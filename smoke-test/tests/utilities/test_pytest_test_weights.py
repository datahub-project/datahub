"""CPU-only tests for mapping pytest nodeids to JUnit weight keys."""

from __future__ import annotations

import pytest

from shard_pack import lookup_test_weight, nodeid_to_weight_keys
from tests.utilities.domains import Domain

pytestmark = pytest.mark.domain(Domain.PLATFORM_INTERNAL)

DEFAULT = 0.104
PARAM_NAME = "test_create_in_allowed_domain_succeeds[openapi-sync]"
CLASS_NODEID = (
    "tests/authorization/test_domain_scoped_create_entity_auth.py"
    f"::TestDomainScopedCreateEntityAuth::{PARAM_NAME}"
)
CLASS_JUNIT_KEY = (
    "tests.authorization.test_domain_scoped_create_entity_auth"
    f".TestDomainScopedCreateEntityAuth::{PARAM_NAME}"
)


def test_class_nodeid_includes_junit_classname_key() -> None:
    keys = nodeid_to_weight_keys(CLASS_NODEID)
    assert CLASS_JUNIT_KEY in keys
    assert (
        "tests.authorization.test_domain_scoped_create_entity_auth"
        f"::TestDomainScopedCreateEntityAuth::{PARAM_NAME}"
    ) in keys


def test_class_nodeid_hits_junit_weight_not_default() -> None:
    weight, used_default = lookup_test_weight(
        CLASS_NODEID, {CLASS_JUNIT_KEY: 65.018}, DEFAULT
    )
    assert used_default is False
    assert weight == pytest.approx(65.018)


def test_module_level_nodeid_hits_function_key() -> None:
    nodeid = "test_e2e.py::test_gms_get_dataset"
    keys = nodeid_to_weight_keys(nodeid)
    assert keys[0] == "test_e2e::test_gms_get_dataset"
    weight, used_default = lookup_test_weight(
        nodeid, {"test_e2e::test_gms_get_dataset": 262.8}, DEFAULT
    )
    assert used_default is False
    assert weight == pytest.approx(262.8)


def test_parametrized_class_key_keeps_brackets() -> None:
    keys = nodeid_to_weight_keys(CLASS_NODEID)
    assert all("[openapi-sync]" in key for key in keys)
    assert CLASS_JUNIT_KEY in keys


def test_missing_id_uses_default() -> None:
    weight, used_default = lookup_test_weight(
        "tests/foo.py::TestBar::test_new", {}, DEFAULT
    )
    assert used_default is True
    assert weight == pytest.approx(DEFAULT)
