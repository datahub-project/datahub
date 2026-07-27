from pathlib import Path

from lib.paths import default_fixture_dir
from lib.personas import load_benchmark_pack, load_personas_oracle
from lib.query_spec import ExpandTypeSpec

HARNESS_ROOT = Path(__file__).resolve().parent.parent


def test_all_personas_have_core_ops() -> None:
    fixture_dir = default_fixture_dir(HARNESS_ROOT)
    benchmarks = load_benchmark_pack(fixture_dir).benchmarks
    oracles = load_personas_oracle(fixture_dir)
    assert len(oracles) == 17
    assert len(benchmarks) == 17
    for persona in oracles:
        bm = benchmarks[persona]
        ops = [s.operation for s in bm.scenarios]
        assert "getMe" in ops
        assert "getSearchResultsForMultiple" in ops


def test_deny_get_domain_expects_403() -> None:
    fixture_dir = default_fixture_dir(HARNESS_ROOT)
    benchmarks = load_benchmark_pack(fixture_dir).benchmarks
    deny = benchmarks["persona-zero-authz"]
    domain = next(s for s in deny.scenarios if s.operation == "getDomain")
    assert domain.expected_status_code == 403


def test_query_specs_cover_all_scenarios() -> None:
    fixture_dir = default_fixture_dir(HARNESS_ROOT)
    pack = load_benchmark_pack(fixture_dir)
    assert set(pack.query_specs.keys()) == {
        "getMe",
        "getSearchResultsForMultiple",
        "getDomain",
    }
    for benchmark in pack.benchmarks.values():
        for scenario in benchmark.scenarios:
            assert scenario.operation in pack.query_specs


def test_get_me_uses_privilege_expand() -> None:
    pack = load_benchmark_pack(default_fixture_dir(HARNESS_ROOT))
    spec = pack.query_specs["getMe"]
    assert spec.expand_types == [
        ExpandTypeSpec(parent_path="me.platformPrivileges", type="PlatformPrivileges")
    ]
    assert not any(p.startswith("me.platformPrivileges.") for p in spec.paths)
    assert spec.field_overrides["me.corpUser.groups"].field == "relationships"
