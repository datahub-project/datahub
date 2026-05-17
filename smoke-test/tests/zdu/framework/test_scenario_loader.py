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
        # Suite A spans TC-301..031: 23 per-URN aspect-migration scenarios
        # + 8 sweep-invariant scenarios (originally TC-401..408 in the
        # design doc), all sharing the non-blocking sweep phase.
        ss = load_scenarios()
        suite_a = [s for s in ss if s.suite == Suite.A]
        assert len(suite_a) == 31
        assert {s.tc_number for s in suite_a} == set(range(301, 332))

    def test_loader_class_returns_same_list(self) -> None:
        # Backward-compatible API: ScenarioLoader().load() == load_scenarios().
        a = [s.tc_number for s in ScenarioLoader().load()]
        b = [s.tc_number for s in load_scenarios()]
        assert a == b

    def test_loader_ignores_source_kwarg(self) -> None:
        # Existing callers may pass source=...; new loader ignores it.
        ss = ScenarioLoader().load(source="ignored")
        suite_a = [s for s in ss if s.suite == Suite.A]
        assert len(suite_a) == 31

    def test_suite_a_scenario_types(self) -> None:
        # Suite A holds two scenario_types: per-URN aspect-migration scenarios
        # use "aspect_migration"; sweep-invariant scenarios use "sweep".
        aspect_migration = []
        sweep = []
        for s in load_scenarios():
            if s.suite != Suite.A:
                continue
            if s.scenario_type == "aspect_migration":
                aspect_migration.append(s.tc_number)
            elif s.scenario_type == "sweep":
                sweep.append(s.tc_number)
            else:
                raise AssertionError(
                    f"Suite A TC-{s.tc_number} has unexpected "
                    f"scenario_type={s.scenario_type}"
                )
        assert set(aspect_migration) == set(range(301, 324))
        assert set(sweep) == set(range(324, 332))


class TestScenarioFields:
    def test_tc1_globaltags_dataset(self) -> None:
        tc1 = next(s for s in load_scenarios() if s.tc_number == 301)
        assert tc1.aspect_name == "globalTags"
        assert tc1.entity_type == "dataset"
        assert tc1.action == "sweep"
        assert tc1.expected_schema_version == 2
        assert tc1.starting_schema_version is None
        assert tc1.expected_to_fail is False

    def test_tc4_embed_dashboard_multi_hop(self) -> None:
        tc4 = next(s for s in load_scenarios() if s.tc_number == 304)
        assert tc4.aspect_name == "embed"
        assert tc4.entity_type == "dashboard"
        assert tc4.expected_schema_version == 4

    def test_tc7_mid_chain_v2_is_active(self) -> None:
        # bridgeGap is wired (EmbedV2ToV3Mutator + EmbedV3ToV4Mutator exist).
        # TC-307 was XFAIL on "v2→v3 mutator absent in gap simulation"; that
        # reason is now stale and the scenario runs as an active validator.
        tc7 = next(s for s in load_scenarios() if s.tc_number == 307)
        assert tc7.starting_schema_version == 2
        assert tc7.expected_schema_version == 4
        assert tc7.expected_to_fail is False
        assert tc7.skip_reason is None

    def test_tc20_read_path_in_memory(self) -> None:
        tc20 = next(s for s in load_scenarios() if s.tc_number == 320)
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
        tc1 = next(s for s in SUITE_A_SCENARIOS if s.tc_number == 301)
        aspect_resp.schema_version = tc1.expected_schema_version
        datahub.get_aspect.return_value = aspect_resp

        ctx = TestContext()
        ctx.seeded_entities = [
            SeededEntity(
                urn="urn:li:dataset:tc1",
                aspect_name="globalTags",
                tc_number=301,
                seeded_data={},
                expected_schema_version=2,
                validator=lambda d: True,
            ),
            SeededEntity(
                urn="urn:li:dataset:tc2",
                aspect_name="globalTags",
                tc_number=302,
                seeded_data={},
                expected_schema_version=2,
                validator=lambda d: True,
            ),
        ]
        result = ScenarioExecutor(datahub).validate(tc1, ctx)
        assert result.status == "PASS"
        # Only TC-301's URN was queried, not TC-302's
        datahub.get_aspect.assert_called_once_with("urn:li:dataset:tc1", "globalTags")


class TestSuiteD:
    def test_load_scenarios_includes_6_suite_d_scenarios(self) -> None:
        from tests.zdu.framework.scenarios import load_scenarios

        scenarios = load_scenarios()
        suite_d = [s for s in scenarios if s.suite == Suite.D]
        # Suite D — ES Phase 2 reindexing (dual-write phase). 6 codified.
        assert len(suite_d) == 6
        assert {s.tc_number for s in suite_d} == set(range(201, 207))

    def test_suite_d_scenarios_use_catch_up_scenario_type(self) -> None:
        from tests.zdu.framework.scenarios import load_scenarios

        scenarios = load_scenarios()
        suite_d = [s for s in scenarios if s.suite == Suite.D]
        assert all(s.scenario_type == "catch_up" for s in suite_d)
        assert all(s.action == "catch_up" for s in suite_d)

    def test_dev_stack_skip_scenarios_have_skip_reason(self) -> None:
        from tests.zdu.framework.scenarios import load_scenarios

        scenarios = load_scenarios()
        suite_d = [s for s in scenarios if s.suite == Suite.D]
        # No XFAIL scenarios remain in Suite D post-revival. TC-205 / TC-206
        # carry the G20c-pending SKIP reason; the rest are active validators.
        non_xfail = {s.tc_number for s in suite_d if not s.expected_to_fail}
        assert non_xfail == {201, 202, 203, 204, 205, 206}
        # The SKIP scenarios must carry a skip_reason documenting the blocker.
        for tc in (205, 206):
            scenario = next(s for s in suite_d if s.tc_number == tc)
            assert scenario.skip_reason, f"TC-{tc} SKIP without skip_reason"
            assert "G20c" in scenario.skip_reason or "reindex" in scenario.skip_reason


class TestAllSuites:
    def test_load_scenarios_returns_combined_list(self) -> None:
        from tests.zdu.framework.scenarios import load_scenarios

        scenarios = load_scenarios()
        suites_present = {s.suite for s in scenarios}
        # Suite A + B + D + F (Suite E was folded into A; C, G, H not codified yet)
        assert Suite.A in suites_present
        assert Suite.B in suites_present
        assert Suite.D in suites_present
        assert Suite.F in suites_present

    def test_suite_a_count(self) -> None:
        from tests.zdu.framework.scenarios import (
            SUITE_A_SCENARIOS,
            SUITE_A_SWEEP_INVARIANT_SCENARIOS,
        )

        # Suite A has two subsets: the per-URN aspect-migration scenarios
        # (TC-301..023) and the sweep-invariant scenarios (TC-324..031, which
        # were originally TC-401..408 in the design doc). Both groups share
        # the non-blocking sweep phase.
        assert len(SUITE_A_SCENARIOS) == 23
        assert len(SUITE_A_SWEEP_INVARIANT_SCENARIOS) == 8
        all_a_tcs = {s.tc_number for s in SUITE_A_SCENARIOS} | {
            s.tc_number for s in SUITE_A_SWEEP_INVARIANT_SCENARIOS
        }
        assert all_a_tcs == set(range(301, 332))

    def test_suite_b_count(self) -> None:
        from tests.zdu.framework.scenarios import SUITE_B_SCENARIOS

        # Suite B — ES Phase 1 reindexing (blocking phase). 9 codified.
        assert len(SUITE_B_SCENARIOS) == 9
        assert {s.tc_number for s in SUITE_B_SCENARIOS} == set(range(101, 110))

    def test_suite_f_count(self) -> None:
        from tests.zdu.framework.scenarios import SUITE_F_SCENARIOS

        assert len(SUITE_F_SCENARIOS) == 3
        assert {s.tc_number for s in SUITE_F_SCENARIOS} == set(range(401, 404))

    def test_total_scenarios_is_sum_of_suites(self) -> None:
        from tests.zdu.framework.scenarios import load_scenarios

        # 23 A_aspect + 8 A_sweep + 9 B + 6 D + 3 F = 49
        assert len(load_scenarios()) == 49
