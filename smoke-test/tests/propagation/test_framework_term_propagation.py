"""
Modern implementation of generic term propagation tests using the latest framework API.

This test demonstrates the new expectation-based validation system that:
- Uses the refactored framework with plugin-based expectations
- Leverages validate_expectations() function that iterates over expectations
- Uses proper ExpectationBase classes with check_expectation() methods
- Uses the framework builders for clean scenario construction
- Built-in cleanup management through the framework

Key improvements over the legacy version:
- Framework-based scenario building with fluent API
- Plugin-based expectations from framework/plugins/
- Built-in cleanup and lifecycle management
- Type-safe validation with proper error messages
- Better organization and maintainability
"""

import logging

from datahub.ingestion.graph.client import DataHubGraph
from tests.propagation.framework.builders.scenario_builder import (
    PropagationScenarioBuilder,
)

# Import the modern framework components
from tests.propagation.framework.core.base import (
    PropagationTestFramework,
    TermPropagationTest,
    validate_expectations,
)
from tests.propagation.framework.plugins.term.expectations import (
    NoTermPropagationExpectation,
    TermPropagationExpectation,
)
from tests.propagation.framework.utils.test_utilities import create_standard_fixtures

logger = logging.getLogger(__name__)


# Get standard fixtures from framework
(
    test_resources_dir,
    test_action_urn,
    load_glossary,
    test_framework,
    resilient_test_framework,
    create_test_action_fixture,
) = create_standard_fixtures()

# Create the test action fixture for term propagation
create_modern_test_action = create_test_action_fixture(TermPropagationTest)


def create_modern_1_to_1_scenario_with_new_expectations(test_action_urn: str):
    """
    Create a modern 1:1 lineage scenario using the framework builder and new expectations.

    This demonstrates:
    - Framework-based scenario construction
    - Plugin-based expectations (TermPropagationExpectation, NoTermPropagationExpectation)
    - Built-in cleanup management
    - Fluent API for scenario building
    """
    builder = PropagationScenarioBuilder(test_action_urn, "modern2")

    # Create upstream dataset with terms using framework builder
    upstream = (
        builder.add_dataset("upstream_table", "snowflake")
        .with_description("Modern upstream dataset with terms")
        .with_columns(["field_0", "field_1", "field_2", "field_3", "field_4"])
        .with_column_glossary_term("field_0", "urn:li:glossaryTerm:TestTerm1_0")
        .with_column_glossary_term("field_1", "urn:li:glossaryTerm:TestTerm1_1")
        .with_column_glossary_term("field_2", "urn:li:glossaryTerm:TestTerm2_2")
        .build()
    )

    # Create downstream dataset
    downstream = (
        builder.add_dataset("downstream_table", "snowflake")
        .with_description("Modern downstream dataset")
        .with_columns(["field_0", "field_1", "field_2", "field_3", "field_4"])
        .build()
    )

    # Register datasets with builder using consistent naming
    builder.register_dataset("upstream_table", upstream)
    builder.register_dataset("downstream_table", downstream)

    # Create lineage using framework builder - now uses registered dataset keys automatically
    lineage = (
        builder.add_lineage("upstream_table", "downstream_table")
        .add_field_lineage("upstream_table", "field_0", "downstream_table", "field_0")
        .add_field_lineage("upstream_table", "field_1", "downstream_table", "field_1")
        .add_field_lineage("upstream_table", "field_2", "downstream_table", "field_2")
        .build(upstream.urn)
    )

    builder.register_lineage("upstream_table", "downstream_table", lineage)

    # Add expectations using new plugin-based expectation classes
    # Create TermPropagationExpectation using the new plugin system
    # Note: Framework adds prefix to dataset names, so we need to match that

    # Based on term_propagation_generic_action_recipe.yaml:
    # - TestTerm1_0 is NOT in target_terms, so field_0 should have NO propagation
    # - TestTerm1_1 IS in target_terms, so field_1 should have propagation
    # - TestTerm2_2 IS in target_terms, so field_2 should have propagation

    no_term_expectation_0 = NoTermPropagationExpectation(
        platform="snowflake",
        dataset_name="modern2.downstream_table",  # Include framework prefix
        field_name="field_0",  # TestTerm1_0 not in propagation rules
    )

    term_expectation_1 = TermPropagationExpectation(
        platform="snowflake",
        dataset_name="modern2.downstream_table",  # Include framework prefix
        field_name="field_1",
        expected_term_urn="urn:li:glossaryTerm:TestTerm1_1",
        origin_dataset="modern2.upstream_table",
        origin_field="field_1",
        propagation_found=True,
        propagation_source=test_action_urn,
    )

    # Field 2 SHOULD propagate (TestTerm2_2 IS in target_terms)
    term_expectation_2 = TermPropagationExpectation(
        platform="snowflake",
        dataset_name="modern2.downstream_table",  # Include framework prefix
        field_name="field_2",
        expected_term_urn="urn:li:glossaryTerm:TestTerm2_2",
        origin_dataset="modern2.upstream_table",
        origin_field="field_2",
        propagation_found=True,
        propagation_source=test_action_urn,
    )

    # Fields 3-4 have no lineage, so no propagation expected
    no_term_expectation_3 = NoTermPropagationExpectation(
        platform="snowflake",
        dataset_name="modern2.downstream_table",  # Include framework prefix
        field_name="field_3",
    )

    no_term_expectation_4 = NoTermPropagationExpectation(
        platform="snowflake",
        dataset_name="modern2.downstream_table",  # Include framework prefix
        field_name="field_4",
    )

    # Add mutation for live testing
    builder.add_term_mutation(
        "upstream_table", "field_0", "urn:li:glossaryTerm:TestTerm1_2"
    )

    # Add live expectation
    live_expectation = TermPropagationExpectation(
        platform="snowflake",
        dataset_name="modern2.downstream_table",  # Include framework prefix
        field_name="field_0",
        expected_term_urn="urn:li:glossaryTerm:TestTerm1_2",
        origin_dataset="modern2.upstream_table",
        origin_field="field_0",
        propagation_found=True,
        propagation_source=test_action_urn,
    )

    # Build scenario with both framework and plugin expectations
    scenario = builder.enable_debug_mcps(True).build()

    # Override with our custom expectations since we want to use the new plugin system
    scenario.base_expectations = [
        no_term_expectation_0,
        term_expectation_1,
        term_expectation_2,
        no_term_expectation_3,
        no_term_expectation_4,
    ]
    scenario.post_mutation_expectations = [live_expectation]

    return scenario


def test_modern_term_propagation_bootstrap(
    test_framework: PropagationTestFramework,
    graph_client: DataHubGraph,
    test_action_urn: str,
    create_modern_test_action,
    load_glossary,
) -> None:
    """Test bootstrap phase using modern framework and expectation system."""
    logger.info("🧪 TESTING BOOTSTRAP PHASE - Modern Term Propagation")

    # Create scenario using framework builder and plugin expectations
    scenario = create_modern_1_to_1_scenario_with_new_expectations(test_action_urn)

    # Use framework to run bootstrap phase with built-in cleanup
    test_framework._execute_bootstrap_phase(scenario, test_action_urn, 120)

    # Debug: Verify lineage was applied in DataHub
    from datahub.metadata.schema_classes import UpstreamLineageClass

    downstream_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,modern2.downstream_table,PROD)"
    )
    lineage_aspect = graph_client.get_aspect(downstream_urn, UpstreamLineageClass)
    if lineage_aspect:
        logger.info(
            f"✅ Lineage aspect found with {len(lineage_aspect.fineGrainedLineages or [])} fine-grained lineages"
        )
        if lineage_aspect.fineGrainedLineages:
            for i, fgl in enumerate(lineage_aspect.fineGrainedLineages):
                upstream_urn = fgl.upstreams[0] if fgl.upstreams else "unknown"
                downstream_urn = fgl.downstreams[0] if fgl.downstreams else "unknown"
                logger.info(
                    f"  Applied FGL {i + 1}: {upstream_urn} -> {downstream_urn}"
                )
    else:
        logger.warning("⚠️  No lineage aspect found in DataHub!")

    # Additional validation using new plugin-based expectations
    logger.info("✅ Validating expectations using modern plugin system...")
    validate_expectations(
        expectations=scenario.base_expectations,
        graph_client=graph_client,
        action_urn=test_action_urn,
        rollback=False,
    )

    logger.info("🎉 Bootstrap phase completed successfully!")


def test_modern_term_propagation_rollback(
    test_framework: PropagationTestFramework,
    graph_client: DataHubGraph,
    test_action_urn: str,
    create_modern_test_action,
    load_glossary,
) -> None:
    """Test rollback phase using modern framework and expectation system."""
    logger.info("🧪 TESTING ROLLBACK PHASE - Modern Term Propagation")

    scenario = create_modern_1_to_1_scenario_with_new_expectations(test_action_urn)

    # Use framework to run bootstrap first, then rollback - built-in cleanup
    test_framework._execute_bootstrap_phase(scenario, test_action_urn, 120)

    # Now test rollback using framework
    test_framework._execute_rollback_phase(
        scenario, test_action_urn, post_mutation=False
    )

    # Additional validation using plugin-based expectations
    logger.info("✅ Validating rollback using modern plugin system...")
    validate_expectations(
        expectations=scenario.base_expectations,
        graph_client=graph_client,
        action_urn=test_action_urn,
        rollback=True,
    )

    logger.info("🎉 Rollback phase completed successfully!")


def test_modern_term_propagation_live(
    test_framework: PropagationTestFramework,
    graph_client: DataHubGraph,
    test_action_urn: str,
    create_modern_test_action,
    load_glossary,
) -> None:
    """Test live phase using modern framework and expectation system."""
    logger.info("🧪 TESTING LIVE PHASE - Modern Term Propagation")

    scenario = create_modern_1_to_1_scenario_with_new_expectations(test_action_urn)

    # Use framework to run live phase with built-in cleanup and lifecycle management
    test_framework._execute_live_phase(scenario, test_action_urn, 120)

    # Additional validation using plugin-based expectations
    logger.info(
        "✅ Validating post-mutation expectations using modern plugin system..."
    )
    validate_expectations(
        expectations=scenario.post_mutation_expectations,
        graph_client=graph_client,
        action_urn=test_action_urn,
        rollback=False,
    )

    logger.info("🎉 Live phase completed successfully!")


def test_modern_term_propagation_full_cycle(
    test_framework: PropagationTestFramework,
    graph_client: DataHubGraph,
    test_action_urn: str,
    create_modern_test_action,
    load_glossary,
) -> None:
    """
    Test the complete propagation cycle using the modern framework and API.

    This test demonstrates the power of the combined modern systems:
    1. Framework-based scenario construction and lifecycle management
    2. Plugin-based expectation classes with check_expectation() methods
    3. validate_expectations() function that iterates over expectations
    4. Built-in cleanup and error handling

    All phases use the framework with plugin-based validation.
    """
    logger.info(
        "🧪 TESTING FULL CYCLE - Propagation Test Framework + Plugin Expectations"
    )
    logger.info("This demonstrates the complete modern system integration!")

    # Create scenario using framework builder and plugin expectations
    scenario = create_modern_1_to_1_scenario_with_new_expectations(test_action_urn)

    # Use the framework to run the complete test cycle with built-in cleanup
    test_type = TermPropagationTest()
    result = test_framework.run_propagation_test(
        scenario,
        test_action_urn,
        test_type,
        "Propagation Test Framework Integration Test",
    )

    # Assert success using framework result
    assert result.success, f"Test failed: {result.error_details}"

    logger.info("\n" + "🎉" * 50)
    logger.info("🎉 FULL CYCLE TEST COMPLETED SUCCESSFULLY! 🎉")
    logger.info("🎉" * 50)

    logger.info(f"\n📈 Test completed in {result.total_time:.2f}s")
    logger.info(result.get_summary())


if __name__ == "__main__":
    print("🚀 Modern Term Propagation Test Suite")
    print("=====================================")
    print("This demonstrates the complete modern system integration!")
    print("")
    print("Run with: pytest test_framework_term_propagation.py -v")
