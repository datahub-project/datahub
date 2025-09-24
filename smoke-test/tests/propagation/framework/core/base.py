"""Enhanced base test framework for propagation tests with improved readability and extensibility."""

import datetime
import logging
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional

from datahub.emitter.rest_emitter import EmitMode
from datahub.ingestion.graph.client import DataHubGraph
from tests.integrations_service_utils import wait_until_action_has_processed_event
from tests.propagation.framework.core.action_manager import ActionManager
from tests.propagation.framework.core.expectations import ExpectationBase
from tests.propagation.framework.core.models import PropagationTestScenario
from tests.utils import wait_for_writes_to_sync

logger = logging.getLogger(__name__)


def validate_expectations(
    expectations: List[ExpectationBase],
    graph_client: DataHubGraph,
    action_urn: Optional[str] = None,
    rollback: bool = False,
    verbose: bool = True,
) -> None:
    """Validate a list of expectations by calling check_expectation() on each.

    This is the new generic validation approach that works with all expectation types.
    Each expectation handles its own validation logic through its check_expectation() method.
    The graph_client is injected at validation time rather than requiring it in constructors.

    Args:
        expectations: List of expectation objects that inherit from ExpectationBase
        graph_client: DataHub graph client for aspect retrieval
        action_urn: Optional action URN for attribution checking
        rollback: Whether to check rollback behavior - when True, only propagated expectations are validated
        verbose: Whether to print human-friendly expectation explanations

    Raises:
        AssertionError: If any expectation fails validation
    """
    # Filter expectations for rollback mode - only check propagated content
    filtered_expectations = expectations
    if rollback:
        filtered_expectations = [exp for exp in expectations if exp.is_propagated]
        if len(filtered_expectations) < len(expectations):
            skipped_count = len(expectations) - len(filtered_expectations)
            logger.info(
                f"Rollback mode: Skipping {skipped_count} non-propagated expectations"
            )

    for i, expectation in enumerate(filtered_expectations):
        if verbose:
            explanation = expectation.explain(rollback=rollback)
            if explanation is not None:
                if rollback:
                    logger.info(
                        f"   🎯 {i + 1}/{len(expectations)}: 🔄 Rollback: {explanation}"
                    )
                else:
                    logger.info(f"   🎯 {i + 1}/{len(expectations)}: {explanation}")
            else:
                logger.info(
                    f"Validating expectation: {expectation.get_expectation_type()}"
                )
        else:
            logger.info(f"Validating expectation: {expectation.get_expectation_type()}")

        try:
            expectation.check_expectation(
                graph_client, action_urn=action_urn, rollback=rollback
            )
        except AssertionError as e:
            logger.error(f"Expectation validation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during expectation validation: {e}")
            raise AssertionError(f"Validation failed with error: {e}") from e


class TestPhase(Enum):
    """Test execution phases."""

    BOOTSTRAP = "bootstrap"
    ROLLBACK = "rollback"
    LIVE = "live"
    LIVE_ROLLBACK = "live_rollback"


class PropagationTestResult:
    """Container for test execution results and timing."""

    def __init__(self):
        self.phase_timings: Dict[TestPhase, float] = {}
        self.phase_results: Dict[TestPhase, bool] = {}
        self.total_time: float = 0.0
        self.success: bool = True
        self.error_details: Optional[str] = None

    def record_phase(
        self,
        phase: TestPhase,
        duration: float,
        success: bool,
        error: Optional[str] = None,
    ):
        """Record results for a test phase."""
        self.phase_timings[phase] = duration
        self.phase_results[phase] = success
        if not success:
            self.success = False
            self.error_details = error

    def get_summary(self) -> str:
        """Get a formatted summary of test results."""
        lines = [
            f"Test Result: {'✅ SUCCESS' if self.success else '❌ FAILURE'}",
            f"Total Time: {self.total_time:.2f}s",
            "Phase Results:",
        ]

        for phase, success in self.phase_results.items():
            timing = self.phase_timings.get(phase, 0.0)
            status = "✅" if success else "❌"
            lines.append(f"  {status} {phase.value}: {timing:.2f}s")

        if self.error_details:
            lines.append(f"Error: {self.error_details}")

        return "\n".join(lines)


class PropagationTestConfig:
    """Configuration for propagation test execution."""

    def __init__(
        self,
        skip_bootstrap: bool = False,
        skip_live: bool = False,
        skip_rollback: bool = False,
        bootstrap_timeout: int = 120,
        live_timeout: int = 120,
        verbose_logging: bool = True,
        fail_fast: bool = True,
    ):
        self.skip_bootstrap = skip_bootstrap
        self.skip_live = skip_live
        self.skip_rollback = skip_rollback
        self.bootstrap_timeout = bootstrap_timeout
        self.live_timeout = live_timeout
        self.verbose_logging = verbose_logging
        self.fail_fast = fail_fast


class BasePropagationTest(ABC):
    """Abstract base class for creating specific propagation test types.

    This allows easy extension for new propagation types (e.g., ownership, properties).
    """

    @abstractmethod
    def get_action_type(self) -> str:
        """Return the action type for this propagation test."""
        pass

    @abstractmethod
    def get_recipe_filename(self) -> str:
        """Return the recipe filename for this propagation test."""
        pass

    @abstractmethod
    def get_action_name(self) -> str:
        """Return a descriptive name for the action."""
        pass

    def get_glossary_required(self) -> bool:
        """Return True if this test requires glossary loading."""
        return False

    def customize_config(self, config: PropagationTestConfig) -> PropagationTestConfig:
        """Allow test types to customize the default configuration."""
        return config


class TermPropagationTest(BasePropagationTest):
    """Concrete implementation for glossary term propagation tests."""

    def get_action_type(self) -> str:
        return "datahub_integrations.propagation.propagation.generic_propagation_action.GenericPropagationAction"

    def get_recipe_filename(self) -> str:
        return "term_propagation_generic_action_recipe.yaml"

    def get_action_name(self) -> str:
        return "test_term_propagation"

    def get_glossary_required(self) -> bool:
        return True


class TagPropagationTest(BasePropagationTest):
    """Concrete implementation for tag propagation tests."""

    def get_action_type(self) -> str:
        return "datahub_integrations.propagation.propagation.generic_propagation_action.GenericPropagationAction"

    def get_recipe_filename(self) -> str:
        return "tag_propagation_generic_action_recipe.yaml"

    def get_action_name(self) -> str:
        return "test_tag_propagation"


class DocumentationPropagationTest(BasePropagationTest):
    """Concrete implementation for documentation propagation tests."""

    def __init__(
        self, action_type: Optional[str] = None, recipe_filename: Optional[str] = None
    ):
        """Initialize with optional overrides for action type and recipe."""
        self._action_type_override = action_type
        self._recipe_filename_override = recipe_filename

    def get_action_type(self) -> str:
        return (
            self._action_type_override
            or "datahub_integrations.propagation.propagation.generic_propagation_action.GenericPropagationAction"
        )

    def get_recipe_filename(self) -> str:
        return (
            self._recipe_filename_override
            or "docs_propagation_generic_action_recipe.yaml"
        )

    def get_action_name(self) -> str:
        return "test_docs_propagation"


class PropagationTestFramework:
    """Enhanced propagation test framework with improved readability and extensibility.

    Features:
    - Type-safe propagation test definitions
    - Detailed result tracking and timing
    - Configurable execution phases
    - Extensible for new propagation types
    - Better error handling and logging
    """

    def __init__(
        self,
        auth_session: Any,
        graph_client: DataHubGraph,
        test_resources_dir: str,
        config: Optional[PropagationTestConfig] = None,
    ):
        self.auth_session = auth_session
        self.graph_client = graph_client
        self.test_resources_dir = test_resources_dir
        self.action_manager = ActionManager(
            auth_session, graph_client, test_resources_dir
        )
        self.config = config or PropagationTestConfig()

    def run_complete_test_cycle(
        self,
        scenario: PropagationTestScenario,
        test_action_urn: str,
        scenario_name: str = "Unknown Scenario",
    ) -> None:
        """Run complete 4-phase test cycle (backward compatibility)."""
        result = self.run_propagation_test(
            scenario, test_action_urn, TermPropagationTest(), scenario_name
        )
        if not result.success:
            raise AssertionError(f"Test failed: {result.error_details}")

    def run_propagation_test(
        self,
        scenario: PropagationTestScenario,
        test_action_urn: str,
        test_type: BasePropagationTest,
        scenario_name: str = "Propagation Test",
    ) -> PropagationTestResult:
        """Run a complete propagation test with the specified test type.

        Args:
            scenario: Test scenario definition
            test_action_urn: URN for the test action
            test_type: Propagation test type implementation
            scenario_name: Descriptive name for the test

        Returns:
            PropagationTestResult with detailed execution results
        """
        result = PropagationTestResult()
        start_time = time.time()

        # Allow test type to customize config
        config = test_type.customize_config(self.config)

        self._print_test_header(scenario_name, test_type, scenario)

        # Initial cleanup (if enabled)
        cleanup_start = time.time()
        self.cleanup_scenario_before_start(scenario)
        cleanup_time = time.time() - cleanup_start
        if scenario.cleanup_entities:
            logger.info(f"Initial cleanup completed in {cleanup_time:.2f}s")

        try:
            # Always set up base graph data (independent of bootstrap phase)
            if scenario.base_graph:
                logger.info(f"Setting up {len(scenario.base_graph)} base graph MCPs...")
                self._setup_base_graph(scenario)

            # Apply pre-bootstrap mutations (if any)
            if scenario.pre_bootstrap_mutations:
                logger.info(
                    f"Applying {len(scenario.pre_bootstrap_mutations)} pre-bootstrap mutations..."
                )
                self._apply_pre_bootstrap_mutations(scenario)

            # Phase 1: Bootstrap (if enabled)
            if not config.skip_bootstrap and scenario.run_bootstrap:
                success, duration, error = self._run_phase_safely(
                    TestPhase.BOOTSTRAP,
                    lambda: self._execute_bootstrap_phase(
                        scenario, test_action_urn, config.bootstrap_timeout
                    ),
                )
                result.record_phase(TestPhase.BOOTSTRAP, duration, success, error)

                if not success and config.fail_fast:
                    return self._finalize_result(result, start_time, scenario)

            # Phase 2: Rollback after bootstrap (if bootstrap ran and rollback not skipped)
            if (
                not config.skip_bootstrap
                and scenario.run_bootstrap
                and not config.skip_rollback
            ):
                success, duration, error = self._run_phase_safely(
                    TestPhase.ROLLBACK,
                    lambda: self._execute_rollback_phase(
                        scenario, test_action_urn, post_mutation=False
                    ),
                )
                result.record_phase(TestPhase.ROLLBACK, duration, success, error)

                if not success and config.fail_fast:
                    return self._finalize_result(result, start_time, scenario)

            # Phase 3: Live (if enabled and has mutations)
            if not config.skip_live and scenario.mutations:
                success, duration, error = self._run_phase_safely(
                    TestPhase.LIVE,
                    lambda: self._execute_live_phase(
                        scenario, test_action_urn, config.live_timeout
                    ),
                )
                result.record_phase(TestPhase.LIVE, duration, success, error)

                if not success and config.fail_fast:
                    return self._finalize_result(result, start_time, scenario)

                # Phase 4: Live rollback (if live ran and rollback not skipped)
                if not config.skip_rollback:
                    success, duration, error = self._run_phase_safely(
                        TestPhase.LIVE_ROLLBACK,
                        lambda: self._execute_rollback_phase(
                            scenario, test_action_urn, post_mutation=True
                        ),
                    )
                    result.record_phase(
                        TestPhase.LIVE_ROLLBACK, duration, success, error
                    )

        except Exception as e:
            result.success = False
            result.error_details = str(e)
            logger.error(f"Test execution failed: {e}")

        return self._finalize_result(result, start_time, scenario)

    def _run_phase_safely(
        self, phase: TestPhase, phase_func
    ) -> tuple[bool, float, Optional[str]]:
        """Run a test phase with error handling and timing."""
        phase_start = time.time()
        try:
            phase_func()
            duration = time.time() - phase_start
            logger.info(f"✅ {phase.value.upper()} phase completed in {duration:.2f}s")
            return True, duration, None
        except Exception as e:
            duration = time.time() - phase_start
            error_msg = str(e)
            logger.error(
                f"❌ {phase.value.upper()} phase failed in {duration:.2f}s: {error_msg}"
            )
            return False, duration, error_msg

    def _setup_base_graph(self, scenario: PropagationTestScenario) -> None:
        """Set up base graph data (datasets, relationships, etc.) - independent of bootstrap phase."""
        logger.info("Setting up base graph data...")
        for i, mcp in enumerate(scenario.base_graph):
            logger.debug(
                f"Emitting MCP {i + 1}/{len(scenario.base_graph)}: {mcp.entityUrn} - {type(mcp.aspect).__name__}"
            )
            if scenario.debug_mcps:
                self._print_mcp_debug_info(
                    mcp, f"Base Graph MCP {i + 1}/{len(scenario.base_graph)}"
                )
            self.graph_client.emit(
                mcp, emit_mode=EmitMode.SYNC_WAIT
            )  # Use sync wait emit mode to ensure writes are processed

        logger.info("Waiting for writes to sync...")
        wait_for_writes_to_sync()

    def _apply_pre_bootstrap_mutations(self, scenario: PropagationTestScenario) -> None:
        """Apply pre-bootstrap mutations - these represent existing metadata that should propagate during bootstrap."""
        logger.info("Applying pre-bootstrap mutations...")
        for i, mcp in enumerate(scenario.pre_bootstrap_mutations):
            logger.info(
                f"Applying pre-bootstrap mutation {i + 1}/{len(scenario.pre_bootstrap_mutations)}: {mcp.entityUrn} - {type(mcp.aspect).__name__}"
            )
            if scenario.debug_mcps:
                self._print_mcp_debug_info(
                    mcp,
                    f"Pre-bootstrap Mutation MCP {i + 1}/{len(scenario.pre_bootstrap_mutations)}",
                )
            self.graph_client.emit(mcp, emit_mode=EmitMode.SYNC_WAIT)

        logger.info("Waiting for pre-bootstrap mutations to sync...")
        wait_for_writes_to_sync()

    def _execute_bootstrap_phase(
        self, scenario: PropagationTestScenario, test_action_urn: str, timeout: int
    ) -> None:
        """Execute the bootstrap phase."""
        logger.info(f"\n{'=' * 60}")
        logger.info("🏗️  STARTING BOOTSTRAP PHASE")
        logger.info(f"Action URN: {test_action_urn}")
        logger.info(f"Base graph MCPs: {len(scenario.base_graph)}")
        logger.info(f"Base expectations: {len(scenario.base_expectations)}")
        logger.info(f"{'=' * 60}")

        logger.info("Running bootstrap action...")
        try:
            self.action_manager.run_bootstrap(test_action_urn)
        except Exception as e:
            if "Timed out waiting" in str(e) and "bootstrap" in str(e):
                logger.warning(
                    "Bootstrap may have completed but stats endpoint failed - continuing with validation"
                )
            else:
                raise

        # Check base expectations using modern validation system
        logger.info(
            f"Validating {len(scenario.base_expectations)} base expectations..."
        )
        validate_expectations(
            scenario.base_expectations,
            self.graph_client,
            action_urn=test_action_urn,
            rollback=False,
            verbose=scenario.verbose_mode,
        )

        logger.info("✅ BOOTSTRAP PHASE COMPLETED SUCCESSFULLY")

    def _execute_rollback_phase(
        self,
        scenario: PropagationTestScenario,
        test_action_urn: str,
        post_mutation: bool = False,
    ) -> None:
        """Execute the rollback phase."""
        phase_name = "LIVE ROLLBACK" if post_mutation else "ROLLBACK"
        logger.info(f"\n{'=' * 60}")
        logger.info(f"↩️  STARTING {phase_name} PHASE")
        logger.info(f"Action URN: {test_action_urn}")
        logger.info(f"Post-mutation rollback: {post_mutation}")
        logger.info(f"{'=' * 60}")

        logger.info("Waiting for writes to sync before rollback...")
        wait_for_writes_to_sync()

        logger.info("Executing rollback action...")
        self.action_manager.run_rollback(test_action_urn)

        logger.info("Waiting for rollback propagation to complete...")
        time.sleep(3)  # Wait 3 seconds for rollback effects to propagate

        # Check rollback expectations using modern validation system
        all_expectations = scenario.base_expectations
        if post_mutation:
            all_expectations += scenario.post_mutation_expectations

        logger.info(f"Validating {len(all_expectations)} rollback expectations...")
        validate_expectations(
            all_expectations,
            self.graph_client,
            action_urn=test_action_urn,
            rollback=True,
            verbose=scenario.verbose_mode,
        )

        logger.info(f"✅ {phase_name} PHASE COMPLETED SUCCESSFULLY")

    def _execute_live_phase(
        self, scenario: PropagationTestScenario, test_action_urn: str, timeout: int
    ) -> None:
        """Execute the live phase."""
        logger.info(f"\n{'=' * 60}")
        logger.info("🔴 STARTING LIVE PHASE")
        logger.info(f"Action URN: {test_action_urn}")
        logger.info(f"Mutations: {len(scenario.mutations)}")
        logger.info(
            f"Post-mutation expectations: {len(scenario.post_mutation_expectations)}"
        )
        logger.info(f"{'=' * 60}")

        logger.info("Starting live action...")
        self.action_manager.start_live_action(test_action_urn)

        try:
            # Apply mutations
            audit_timestamp = datetime.datetime.now(datetime.timezone.utc)
            logger.info(f"Applying {len(scenario.mutations)} mutations...")
            for i, mutation in enumerate(scenario.mutations):
                # Print mutation info at INFO level if verbose mode is enabled
                # Use original mutation object for explain() if available, fallback to basic info
                mutation_explanation = "mutation"
                if i < len(scenario.mutation_objects):
                    mutation_explanation = scenario.mutation_objects[i].explain()
                elif hasattr(mutation, "aspect") and hasattr(
                    mutation.aspect, "__class__"
                ):
                    mutation_explanation = (
                        f"{mutation.entityUrn} - {mutation.aspect.__class__.__name__}"
                    )

                if scenario.verbose_mode:
                    logger.info(
                        f"Applying mutation {i + 1}/{len(scenario.mutations)}: {mutation_explanation}"
                    )
                else:
                    logger.debug(
                        f"Applying mutation {i + 1}/{len(scenario.mutations)}: {mutation_explanation}"
                    )

                # Use the MCP directly for emission
                mcp_to_emit = mutation
                if scenario.debug_mcps:
                    self._print_mcp_debug_info(
                        mcp_to_emit, f"Mutation MCP {i + 1}/{len(scenario.mutations)}"
                    )
                self.graph_client.emit(
                    mcp_to_emit, emit_mode=EmitMode.SYNC_WAIT
                )  # Use Sync wait emit mode to make sure all the writes processed

            logger.info(
                f"Waiting for action to process events (timestamp: {audit_timestamp})..."
            )
            wait_until_action_has_processed_event(
                test_action_urn,
                self.auth_session.integrations_service_url(),
                audit_timestamp,
            )

            # Check post-mutation expectations using modern validation system
            logger.info(
                f"Validating {len(scenario.post_mutation_expectations)} post-mutation expectations..."
            )
            validate_expectations(
                scenario.post_mutation_expectations,
                self.graph_client,
                action_urn=test_action_urn,
                rollback=False,
                verbose=scenario.verbose_mode,
            )

            logger.info("✅ LIVE PHASE COMPLETED SUCCESSFULLY")

        finally:
            logger.info("Stopping live action...")
            self.action_manager.stop_live_action(test_action_urn)

    def _cleanup_test_entities(self, urns: List[str]) -> None:
        """Clean up test entities and any stale actions."""
        self.action_manager.cleanup(urns=urns, remove_actions=False)

    def cleanup_scenario_before_start(self, scenario: PropagationTestScenario) -> None:
        """Cleanup any existing entities before starting a scenario."""
        if scenario.cleanup_entities:
            logger.info("🧹 Cleaning up existing entities before scenario...")
            self._cleanup_test_entities(scenario.get_urns())
            logger.info("🧹 Pre-scenario cleanup completed")

    def cleanup_scenario_after_completion(
        self, scenario: PropagationTestScenario
    ) -> None:
        """Cleanup entities created by this scenario after completion."""
        if scenario.cleanup_entities:
            logger.info("🧹 Cleaning up scenario entities...")
            self._cleanup_test_entities(scenario.get_urns())
            logger.info("🧹 Post-scenario cleanup completed")

    def _print_test_header(
        self,
        scenario_name: str,
        test_type: BasePropagationTest,
        scenario: PropagationTestScenario,
    ) -> None:
        """Print formatted test header."""
        logger.info(f"\n{'=' * 80}")
        logger.info(f"🧪 PROPAGATION TEST: {scenario_name.upper()}")
        logger.info(f"📋 Test Type: {test_type.__class__.__name__}")
        logger.info(f"⚙️  Action Type: {test_type.get_action_name()}")
        logger.info(f"📊 Base MCPs: {len(scenario.base_graph)}")
        logger.info(f"🎯 Base Expectations: {len(scenario.base_expectations)}")
        logger.info(f"🔄 Mutations: {len(scenario.mutations)}")
        logger.info(
            f"🎯 Post-mutation Expectations: {len(scenario.post_mutation_expectations)}"
        )
        logger.info(f"{'=' * 80}")

        phases = []
        if not self.config.skip_bootstrap and scenario.run_bootstrap:
            phases.append("🏗️  Bootstrap")
            if not self.config.skip_rollback:
                phases.append("↩️  Rollback")
        if not self.config.skip_live and scenario.mutations:
            phases.append("🔴 Live")
            if not self.config.skip_rollback:
                phases.append("↩️  Live Rollback")

        logger.info("📋 Test Phases:")
        for i, phase in enumerate(phases, 1):
            logger.info(f"   {i}. {phase}")
        logger.info(f"{'=' * 80}\n")

    def _finalize_result(
        self,
        result: PropagationTestResult,
        start_time: float,
        scenario: PropagationTestScenario,
    ) -> PropagationTestResult:
        """Finalize test result with cleanup."""
        # Final cleanup (if enabled)
        final_cleanup_start = time.time()
        self.cleanup_scenario_after_completion(scenario)
        final_cleanup_time = time.time() - final_cleanup_start

        result.total_time = time.time() - start_time

        logger.info(f"Final cleanup completed in {final_cleanup_time:.2f}s")
        logger.info(f"\n{result.get_summary()}")

        return result

    def print_scenario_summary(
        self, scenario: PropagationTestScenario, scenario_name: str
    ) -> None:
        """Print a summary of what the test scenario will do (backward compatibility)."""
        logger.info(f"\n{'=' * 120}")
        logger.info(f"🧪 TEST SCENARIO: {scenario_name.upper()}")
        logger.info(f"{'=' * 120}")

        logger.info("🧪 TEST PHASES:")
        logger.info(
            "   1. 🏗️  Bootstrap: Set up data, run initial propagation, verify base expectations"
        )
        logger.info("   2. ↩️  Rollback: Remove all propagated data, verify cleanup")
        logger.info(
            "   3. 🔴 Live: Start live action, apply mutations, verify real-time propagation"
        )
        logger.info(
            "   4. ↩️  Live Rollback: Remove all propagated data again, verify final cleanup"
        )

        logger.info(f"{'=' * 120}\n")

    def _print_mcp_debug_info(self, mcp: Any, mcp_description: str) -> None:
        """Print detailed debug information for an MCP."""
        import json

        from datahub.emitter.serialization_helper import pre_json_transform

        logger.info(f"\n{'=' * 80}")
        logger.info(f"🔍 DEBUG MCP: {mcp_description}")
        logger.info(f"{'=' * 80}")
        logger.info(f"Entity URN: {mcp.entityUrn}")
        logger.info(f"Aspect Type: {type(mcp.aspect).__name__}")

        try:
            aspect_dict = pre_json_transform(mcp.aspect)
            aspect_json = json.dumps(aspect_dict, indent=2, sort_keys=True)
            logger.info(f"Aspect Content:\n{aspect_json}")
        except Exception as e:
            logger.warning(f"Failed to serialize aspect: {e}")
            logger.info(f"Raw aspect: {mcp.aspect}")

        logger.info(f"{'=' * 80}\n")
