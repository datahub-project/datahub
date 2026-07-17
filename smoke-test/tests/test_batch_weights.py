import json
from pathlib import Path

from conftest import (
    get_pytest_test_weight,
    load_pytest_test_weights,
    normalize_test_id,
)


class _FakeItem:
    def __init__(self, nodeid: str) -> None:
        self.nodeid = nodeid


def test_normalize_module_level_test():
    assert (
        normalize_test_id("tests/metrics/test_metrics.py::test_export")
        == "tests.metrics.test_metrics::test_export"
    )


def test_normalize_class_test_matches_dotted_key():
    # The bug: class tests must map to `module.Class::test`, not `module::Class::test`.
    assert (
        normalize_test_id(
            "tests/status/test_lifecycle_state.py::TestLifecycleStageAPIs::test_hidden"
        )
        == "tests.status.test_lifecycle_state.TestLifecycleStageAPIs::test_hidden"
    )


def test_normalize_preserves_parametrization():
    assert (
        normalize_test_id("tests/policies/test_perf.py::test_domains[100-50]")
        == "tests.policies.test_perf::test_domains[100-50]"
    )


def test_normalize_nested_class_joins_with_dot():
    assert (
        normalize_test_id("tests/mod.py::Outer::Inner::test_x")
        == "tests.mod.Outer.Inner::test_x"
    )


def test_class_test_weight_is_looked_up_not_defaulted():
    weights = {"tests.status.test_lifecycle_state.TestFoo::test_bar": 440.0}
    item = _FakeItem("tests/status/test_lifecycle_state.py::TestFoo::test_bar")
    assert get_pytest_test_weight(item, weights, default_weight=1.0) == 440.0


def test_unknown_test_uses_default_weight():
    weights = {"tests.a::test_a": 10.0}
    item = _FakeItem("tests/unknown.py::test_missing")
    assert get_pytest_test_weight(item, weights, default_weight=7.5) == 7.5


def test_every_weights_file_key_is_reachable():
    # Guards against a future key-format drift silently reverting the 44%-ignored bug:
    # every real key must be producible by normalizing some nodeid form.
    weights = load_pytest_test_weights()
    assert weights, "weights file should load"
    unreachable = []
    for key in weights:
        module_part, _, leaf = key.rpartition("::")
        # Reconstruct a nodeid: last dotted segment before '::' that is a class (Capitalized)
        # is separated by '::'; the rest is the module path.
        segments = module_part.split(".")
        # class segments are Capitalized; module segments are lowercase file/dirs
        cls_idx = next(
            (i for i, s in enumerate(segments) if s[:1].isupper()), len(segments)
        )
        module = "/".join(segments[:cls_idx]) + ".py"
        classes = segments[cls_idx:]
        nodeid = "::".join([module, *classes, leaf]) if classes else f"{module}::{leaf}"
        if normalize_test_id(nodeid) != key:
            unreachable.append(key)
    assert not unreachable, f"{len(unreachable)} keys unreachable, e.g. {unreachable[:3]}"
