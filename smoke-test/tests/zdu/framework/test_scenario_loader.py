"""Tests for the codified scenario list and the ScenarioExecutor.

Replaces the legacy CSV-row-parsing tests. Scenarios live in
``framework/scenarios.py`` as Python objects; the loader is a thin
list-returning shim.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from tests.zdu.framework.context import SeededEntity, TestContext
from tests.zdu.framework.scenario_loader import (
    KNOWN_FAILURES,
    ScenarioExecutor,
    ScenarioLoader,
)
from tests.zdu.framework.scenarios import SUITE_A_SCENARIOS, load_scenarios
from tests.zdu.framework.suite import Suite


class TestLoadScenarios:
    def test_load_returns_full_suite_a(self) -> None:
        ss = load_scenarios()
        suite_a = [s for s in ss if s.suite == Suite.A]
        assert len(suite_a) == 23
        assert {s.tc_number for s in suite_a} == set(range(1, 24))

    def test_loader_class_returns_same_list(self) -> None:
        # Backward-compatible API: ScenarioLoader().load() == load_scenarios().
        a = [s.tc_number for s in ScenarioLoader().load()]
        b = [s.tc_number for s in load_scenarios()]
        assert a == b

    def test_loader_ignores_source_kwarg(self) -> None:
        # Existing callers may pass source=...; new loader ignores it.
        ss = ScenarioLoader().load(source="ignored")
        suite_a = [s for s in ss if s.suite == Suite.A]
        assert len(suite_a) == 23

    def test_all_suite_a_scenarios_are_aspect_migration(self) -> None:
        # Suite A scenarios use the aspect_migration executor.
        for s in load_scenarios():
            if s.suite == Suite.A:
                assert s.scenario_type == "aspect_migration"


class TestScenarioFields:
    def test_tc1_globaltags_dataset(self) -> None:
        tc1 = next(s for s in load_scenarios() if s.tc_number == 1)
        assert tc1.aspect_name == "globalTags"
        assert tc1.entity_type == "dataset"
        assert tc1.action == "sweep"
        assert tc1.expected_schema_version == 2
        assert tc1.starting_schema_version is None
        assert tc1.expected_to_fail is False

    def test_tc4_embed_dashboard_multi_hop(self) -> None:
        tc4 = next(s for s in load_scenarios() if s.tc_number == 4)
        assert tc4.aspect_name == "embed"
        assert tc4.entity_type == "dashboard"
        assert tc4.expected_schema_version == 4

    def test_tc7_mid_chain_v2_is_active(self) -> None:
        # bridgeGap is wired (EmbedV2ToV3Mutator + EmbedV3ToV4Mutator exist).
        # TC-7 was XFAIL on "v2→v3 mutator absent in gap simulation"; that
        # reason is now stale and the scenario runs as an active validator.
        tc7 = next(s for s in load_scenarios() if s.tc_number == 7)
        assert tc7.starting_schema_version == 2
        assert tc7.expected_schema_version == 4
        assert tc7.expected_to_fail is False
        assert tc7.skip_reason is None

    def test_tc20_read_path_in_memory(self) -> None:
        tc20 = next(s for s in load_scenarios() if s.tc_number == 20)
        assert tc20.starting_schema_version == 1
        assert tc20.expected_schema_version == 4
        assert tc20.action == "read"


class TestKnownFailuresSet:
    def test_expected_to_fail_matches_known_failures_dict(self) -> None:
        # KNOWN_FAILURES governs Suite A only; Suite D manages expected_to_fail
        # per-scenario in its own factory.
        for s in load_scenarios():
            if s.suite != Suite.A:
                continue
            expected = s.tc_number in KNOWN_FAILURES
            assert s.expected_to_fail is expected, (
                f"TC-{s.tc_number}: expected_to_fail={s.expected_to_fail} "
                f"but KNOWN_FAILURES has it={expected}"
            )

    def test_known_failure_skip_reason_matches_dict(self) -> None:
        for s in load_scenarios():
            if s.suite != Suite.A:
                continue
            if s.expected_to_fail:
                assert s.skip_reason == KNOWN_FAILURES[s.tc_number]


class TestExecutorValidateTakesCtxAndFiltersByTc:
    def test_validates_only_tc_specific_urns(self) -> None:
        datahub = MagicMock()
        aspect_resp = MagicMock()
        tc1 = next(s for s in SUITE_A_SCENARIOS if s.tc_number == 1)
        aspect_resp.schema_version = tc1.expected_schema_version
        datahub.get_aspect.return_value = aspect_resp

        ctx = TestContext()
        ctx.seeded_entities = [
            SeededEntity(
                urn="urn:li:dataset:tc1",
                aspect_name="globalTags",
                tc_number=1,
                seeded_data={},
                expected_schema_version=2,
                validator=lambda d: True,
            ),
            SeededEntity(
                urn="urn:li:dataset:tc2",
                aspect_name="globalTags",
                tc_number=2,
                seeded_data={},
                expected_schema_version=2,
                validator=lambda d: True,
            ),
        ]
        result = ScenarioExecutor(datahub).validate(tc1, ctx)
        assert result.status == "PASS"
        # Only TC-1's URN was queried, not TC-2's
        datahub.get_aspect.assert_called_once_with("urn:li:dataset:tc1", "globalTags")


class TestSuiteD:
    def test_load_scenarios_includes_9_suite_d_scenarios(self) -> None:
        from tests.zdu.framework.scenarios import load_scenarios

        scenarios = load_scenarios()
        suite_d = [s for s in scenarios if s.suite == Suite.D]
        assert len(suite_d) == 9
        # tc_numbers cover 301..309
        assert {s.tc_number for s in suite_d} == set(range(301, 310))

    def test_suite_d_scenarios_use_catch_up_scenario_type(self) -> None:
        from tests.zdu.framework.scenarios import load_scenarios

        scenarios = load_scenarios()
        suite_d = [s for s in scenarios if s.suite == Suite.D]
        assert all(s.scenario_type == "catch_up" for s in suite_d)
        assert all(s.action == "catch_up" for s in suite_d)

    def test_dev_stack_xfail_scenarios_have_skip_reason(self) -> None:
        from tests.zdu.framework.scenarios import load_scenarios

        scenarios = load_scenarios()
        suite_d = [s for s in scenarios if s.suite == Suite.D]
        # TC-305 / TC-306 PASS on dev (active validators); TC-308 / TC-309
        # SKIP on dev via the P0b/c reclassification (was XFAIL with a
        # misleading "runtime knob" reason; the real blocker is G20c
        # reindex-capture). None of these four are "expected to fail".
        non_xfail = {s.tc_number for s in suite_d if not s.expected_to_fail}
        assert non_xfail == {305, 306, 308, 309}
        # Every XFAIL scenario must explain why with a skip_reason.
        xfail = [s for s in suite_d if s.expected_to_fail]
        for s in xfail:
            assert s.skip_reason, f"TC-{s.tc_number} XFAIL without skip_reason"
        # The reclassified SKIP scenarios must also carry a skip_reason
        # documenting their blocker (G20c reindex capture).
        for tc in (308, 309):
            scenario = next(s for s in suite_d if s.tc_number == tc)
            assert scenario.skip_reason, f"TC-{tc} SKIP without skip_reason"
            assert "G20c" in scenario.skip_reason or "reindex" in scenario.skip_reason


class TestAllSuites:
    def test_load_scenarios_returns_combined_list(self) -> None:
        from tests.zdu.framework.scenarios import load_scenarios

        scenarios = load_scenarios()
        suites_present = {s.suite for s in scenarios}
        # Suite A + B + D + E + F (no C, G, H, I yet)
        assert Suite.A in suites_present
        assert Suite.B in suites_present
        assert Suite.D in suites_present
        assert Suite.E in suites_present
        assert Suite.F in suites_present

    def test_suite_b_count(self) -> None:
        from tests.zdu.framework.scenarios import SUITE_B_SCENARIOS

        assert len(SUITE_B_SCENARIOS) == 12
        assert {s.tc_number for s in SUITE_B_SCENARIOS} == set(range(101, 113))

    def test_suite_e_count(self) -> None:
        from tests.zdu.framework.scenarios import SUITE_E_SCENARIOS

        assert len(SUITE_E_SCENARIOS) == 8
        assert {s.tc_number for s in SUITE_E_SCENARIOS} == set(range(401, 409))

    def test_suite_f_count(self) -> None:
        from tests.zdu.framework.scenarios import SUITE_F_SCENARIOS

        assert len(SUITE_F_SCENARIOS) == 7
        assert {s.tc_number for s in SUITE_F_SCENARIOS} == set(range(501, 508))

    def test_total_scenarios_is_sum_of_suites(self) -> None:
        from tests.zdu.framework.scenarios import load_scenarios

        # 23 A + 12 B + 9 D + 8 E + 7 F = 59
        assert len(load_scenarios()) == 59
