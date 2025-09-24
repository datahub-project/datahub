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
from typing import Any

import pytest

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.graph.client import DataHubGraph
from tests.propagation.framework.builders.scenario_builder import (
    PropagationScenarioBuilder,
)

# Import the modern framework components
from tests.propagation.framework.core.base import (
    PropagationTestFramework,
    TermPropagationTest,
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

    # With bootstrap configuration added, term propagation SHOULD work during bootstrap!
    # Based on term_propagation_generic_action_recipe.yaml:
    # - TestTerm1_0 is NOT in target_terms, so field_0 should have NO propagation
    # - TestTerm1_1 IS in target_terms, so field_1 should have propagation
    # - TestTerm2_2 IS in target_terms, so field_2 should have propagation

    no_term_expectation_0 = NoTermPropagationExpectation(
        field_urn=make_schema_field_urn(
            downstream.urn, "field_0"
        ),  # TestTerm1_0 not in propagation rules
    )

    term_expectation_1 = TermPropagationExpectation(
        field_urn=make_schema_field_urn(downstream.urn, "field_1"),
        expected_term_urn="urn:li:glossaryTerm:TestTerm1_1",
        origin_dataset="modern2.upstream_table",
        origin_field="field_1",
        propagation_found=True,
        propagation_source=test_action_urn,
        propagation_origin=make_schema_field_urn(upstream.urn, "field_1"),
    )

    # Field 2 SHOULD propagate (TestTerm2_2 IS in target_terms)
    term_expectation_2 = TermPropagationExpectation(
        field_urn=make_schema_field_urn(downstream.urn, "field_2"),
        expected_term_urn="urn:li:glossaryTerm:TestTerm2_2",
        origin_dataset="modern2.upstream_table",
        origin_field="field_2",
        propagation_found=True,
        propagation_source=test_action_urn,
        propagation_origin=make_schema_field_urn(upstream.urn, "field_2"),
    )

    # Fields 3-4 have no lineage, so no propagation expected
    no_term_expectation_3 = NoTermPropagationExpectation(
        field_urn=make_schema_field_urn(downstream.urn, "field_3"),
    )

    no_term_expectation_4 = NoTermPropagationExpectation(
        field_urn=make_schema_field_urn(downstream.urn, "field_4"),
    )

    # Add mutation for live testing
    builder.add_term_mutation(
        "upstream_table", "field_0", "urn:li:glossaryTerm:TestTerm1_2"
    )

    # Add live expectation
    live_expectation = TermPropagationExpectation(
        field_urn=make_schema_field_urn(downstream.urn, "field_0"),
        expected_term_urn="urn:li:glossaryTerm:TestTerm1_2",
        origin_dataset="modern2.upstream_table",
        origin_field="field_0",
        propagation_found=True,
        propagation_source=test_action_urn,
        propagation_origin=make_schema_field_urn(upstream.urn, "field_0"),
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
    # For full cycle tests, include live expectations that will work during mutations
    # Note: Only expect propagation for the NEW mutation, not re-establishment of bootstrap terms
    scenario.post_mutation_expectations = [
        live_expectation,  # From mutation - TestTerm1_2 should propagate to field_0
        # Bootstrap terms (TestTerm1_1, TestTerm2_2) are NOT re-established after rollback
        # The live mutation only propagates the new term, not the original dataset state
    ]

    return scenario


def create_1_to_1_scenario(test_action_urn: str) -> Any:
    """Create a 1:1 lineage scenario similar to the original test."""
    builder = PropagationScenarioBuilder(test_action_urn, "terms_1_to_1")

    # Create source dataset with terms
    source_dataset = (
        builder.add_dataset("upstream_table", "snowflake")
        .with_description("Upstream dataset with terms")
        .with_columns(["field_0", "field_1", "field_2", "field_3", "field_4"])
        .with_column_glossary_term("field_0", "urn:li:glossaryTerm:TestTerm1_0")
        .with_column_glossary_term("field_1", "urn:li:glossaryTerm:TestTerm1_1")
        .with_column_glossary_term("field_2", "urn:li:glossaryTerm:TestTerm2_2")
        .build()
    )

    # Create target dataset without terms
    target_dataset = (
        builder.add_dataset("downstream_table", "snowflake")
        .with_description("Downstream dataset")
        .with_columns(["field_0", "field_1", "field_2", "field_3", "field_4"])
        .build()
    )

    # Register datasets
    builder.register_dataset("source", source_dataset)
    builder.register_dataset("target", target_dataset)

    # Create lineage - 1:1 mappings for field_0, field_1, field_2
    lineage = (
        builder.add_lineage("source", "target")
        .add_field_lineage("source", "field_0", "target", "field_0")
        .add_field_lineage("source", "field_1", "target", "field_1")
        .add_field_lineage("source", "field_2", "target", "field_2")
        .build(source_dataset.urn)
    )

    builder.register_lineage("source", "target", lineage)

    # Base expectations based on term propagation configuration
    # TestTerm1_0 NOT in target_terms - should NOT propagate
    # TestTerm1_1 IS in target_terms - should propagate
    # TestTerm2_2 IS in target_terms - should propagate
    builder.base_expectations = [
        NoTermPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "field_0"),
        ),
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "field_1"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_1",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(source_dataset.urn, "field_1"),
        ),
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "field_2"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm2_2",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(source_dataset.urn, "field_2"),
        ),
        NoTermPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "field_3"),
        ),
        NoTermPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "field_4"),
        ),
    ]

    # Post-mutation expectations
    builder.post_mutation_expectations = [
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "field_0"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_2",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(source_dataset.urn, "field_0"),
        ),
    ]

    # Build scenario first, then add smart mutations
    scenario = builder.build()

    # Create mutation for live testing using smart mutations
    from tests.propagation.framework.plugins.term.mutations import (
        FieldTermAdditionMutation,
    )

    mutation = FieldTermAdditionMutation(
        dataset_urn=source_dataset.urn,  # Use URN directly from dataset
        field_name="field_0",
        term_urn="urn:li:glossaryTerm:TestTerm1_2",
    )
    scenario.add_mutation_objects([mutation])

    return scenario


def create_2_hop_scenario(test_action_urn: str) -> Any:
    """Create a 2-hop lineage scenario to test multi-hop propagation with via/origin tracking."""
    builder = PropagationScenarioBuilder(test_action_urn, "terms_2_hop")

    # Create datasets
    dataset1 = (
        builder.add_dataset("table_foo_0", "snowflake")
        .with_columns(["field_0", "field_1", "field_2", "field_3", "field_4"])
        .with_column_glossary_term("field_0", "urn:li:glossaryTerm:TestTerm1_1")
        .build()
    )

    dataset2 = (
        builder.add_dataset("table_foo_1", "snowflake")
        .with_columns(["field_0", "field_1", "field_2", "field_3", "field_4"])
        .build()
    )

    dataset3 = (
        builder.add_dataset("table_foo_2", "snowflake")
        .with_columns(["field_0", "field_1", "field_2", "field_3", "field_4"])
        .build()
    )

    # Register datasets
    builder.register_dataset("dataset1", dataset1)
    builder.register_dataset("dataset2", dataset2)
    builder.register_dataset("dataset3", dataset3)

    # Create lineage chain: dataset1 -> dataset2 -> dataset3
    lineage1 = (
        builder.add_lineage("dataset1", "dataset2")
        .add_field_lineage("dataset1", "field_0", "dataset2", "field_0")
        .build(dataset1.urn)
    )
    builder.register_lineage("dataset1", "dataset2", lineage1)

    lineage2 = (
        builder.add_lineage("dataset2", "dataset3")
        .add_field_lineage("dataset2", "field_0", "dataset3", "field_0")
        .build(dataset2.urn)
    )
    builder.register_lineage("dataset2", "dataset3", lineage2)

    # Base expectations - Bootstrap creates only 1-hop propagation
    builder.base_expectations = [
        # 1-hop: dataset1.field_0 -> dataset2.field_0 (should work during bootstrap)
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(dataset2.urn, "field_0"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_1",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(dataset1.urn, "field_0"),
            propagation_via=None,  # Direct 1-hop, no intermediate
            expected_depth=1,  # This is a 1-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),
        # 2-hop: dataset1.field_0 -> dataset2.field_0 -> dataset3.field_0 (should NOT work during bootstrap)
        NoTermPropagationExpectation(
            field_urn=make_schema_field_urn(dataset3.urn, "field_0"),
        ),
    ]

    # Post-mutation expectations - During live phase, mutations should trigger multi-hop propagation
    # Note: Only expect propagation for the NEW mutation, not re-establishment of bootstrap terms
    builder.post_mutation_expectations = [
        # New term should propagate 1-hop: dataset1.field_0 -> dataset2.field_0
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(dataset2.urn, "field_0"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_2",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(dataset1.urn, "field_0"),
            propagation_via=None,  # Direct 1-hop
            expected_depth=1,  # This is a 1-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),
        # New term should propagate 2-hop: dataset1.field_0 -> dataset2.field_0 -> dataset3.field_0
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(dataset3.urn, "field_0"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_2",  # New term from mutation
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(dataset1.urn, "field_0"),
            propagation_via=make_schema_field_urn(
                dataset2.urn, "field_0"
            ),  # Propagated via dataset2.field_0
            expected_depth=2,  # This is a 2-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),
        # Bootstrap terms (TestTerm1_1) are NOT re-established after rollback
        # The live mutation only propagates the new term, not the original dataset state
    ]

    # Build scenario first, then add smart mutations
    scenario = builder.build()

    # Create mutation to trigger 2-hop propagation during live phase using smart mutations
    from tests.propagation.framework.plugins.term.mutations import (
        FieldTermAdditionMutation,
    )

    # Add a new term to dataset1.field_0 to trigger live propagation
    mutation = FieldTermAdditionMutation(
        dataset_urn=dataset1.urn,  # Use URN directly from dataset
        field_name="field_0",
        term_urn="urn:li:glossaryTerm:TestTerm1_2",
    )
    scenario.add_mutation_objects([mutation])

    return scenario


def create_logical_field_to_field_scenario(test_action_urn: str) -> Any:
    """Create a logical field-to-field relationship scenario for term propagation.

    This scenario tests:
    - Parent dataset with fields that have terms
    - Multiple children datasets with corresponding fields
    - Field-level logical relationships (not dataset-level)
    - Term mutations on parent fields propagating to connected child fields
    """
    builder = PropagationScenarioBuilder(test_action_urn, "terms_logical_field")

    # Create parent dataset with terms on fields
    parent_dataset = (
        builder.add_dataset("parent_table", "snowflake")
        .with_description("Parent table with logical field relationships")
        .with_columns(["id", "name", "email", "status", "created_date"])
        .with_column_glossary_term(
            "id", "urn:li:glossaryTerm:TestTerm1_1"
        )  # Will propagate
        .with_column_glossary_term(
            "email", "urn:li:glossaryTerm:TestTerm2_2"
        )  # Will propagate
        .build()
    )

    # Create child dataset 1
    child1_dataset = (
        builder.add_dataset("child1_table", "snowflake")
        .with_description("Child table 1 with logical field connections")
        .with_columns(
            [
                "customer_id",
                "full_name",
                "email_address",
                "account_status",
                "signup_date",
            ]
        )
        .build()
    )

    # Create child dataset 2
    child2_dataset = (
        builder.add_dataset("child2_table", "snowflake")
        .with_description("Child table 2 with logical field connections")
        .with_columns(
            [
                "user_id",
                "display_name",
                "contact_email",
                "user_status",
                "registration_date",
            ]
        )
        .build()
    )

    # Register datasets
    builder.register_dataset("parent", parent_dataset)
    builder.register_dataset("child1", child1_dataset)
    builder.register_dataset("child2", child2_dataset)

    # TODO: Implement field-level logical relationships
    # This would create logical relationships between specific fields instead of entire datasets
    # Currently commented out due to API changes - needs investigation of correct LogicalParentClass usage

    # For now, we'll skip the logical relationship setup and focus on the mutation testing framework

    # Base expectations - Terms should propagate through logical field relationships during bootstrap
    builder.base_expectations = [
        # Parent.id -> Child1.customer_id (TestTerm1_1 should propagate)
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(child1_dataset.urn, "customer_id"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_1",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(parent_dataset.urn, "id"),
        ),
        # Parent.id -> Child2.user_id (TestTerm1_1 should propagate)
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(child2_dataset.urn, "user_id"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_1",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(parent_dataset.urn, "id"),
        ),
        # Parent.email -> Child1.email_address (TestTerm2_2 should propagate)
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(child1_dataset.urn, "email_address"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm2_2",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(parent_dataset.urn, "email"),
        ),
        # Parent.email -> Child2.contact_email (TestTerm2_2 should propagate)
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(child2_dataset.urn, "contact_email"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm2_2",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(parent_dataset.urn, "email"),
        ),
        # Fields without logical relationships should NOT have propagation
        NoTermPropagationExpectation(
            field_urn=make_schema_field_urn(child1_dataset.urn, "full_name"),
        ),
        NoTermPropagationExpectation(
            field_urn=make_schema_field_urn(child2_dataset.urn, "display_name"),
        ),
    ]

    # Post-mutation expectations - New terms should propagate to all connected child fields
    builder.post_mutation_expectations = [
        # Original terms should still be propagated
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(child1_dataset.urn, "customer_id"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_1",  # Original term
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(parent_dataset.urn, "id"),
        ),
        # New term from mutation1 should propagate to child1.customer_id
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(child1_dataset.urn, "customer_id"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_2",  # New term
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(parent_dataset.urn, "id"),
        ),
        # New term from mutation1 should propagate to child2.user_id
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(child2_dataset.urn, "user_id"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_2",  # New term
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(parent_dataset.urn, "id"),
        ),
        # New term from mutation2 should propagate to child1.email_address
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(child1_dataset.urn, "email_address"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_2",  # New term
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(parent_dataset.urn, "email"),
        ),
        # New term from mutation2 should propagate to child2.contact_email
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(child2_dataset.urn, "contact_email"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_2",  # New term
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(parent_dataset.urn, "email"),
        ),
    ]

    # Build scenario first, then add smart mutations
    scenario = builder.build()

    # Create mutations for live testing using smart mutations
    from tests.propagation.framework.plugins.term.mutations import (
        FieldTermAdditionMutation,
    )

    # Mutation 1: Add a new term to parent.id field
    mutation1 = FieldTermAdditionMutation(
        dataset_urn=parent_dataset.urn,  # Use URN directly from dataset
        field_name="id",
        term_urn="urn:li:glossaryTerm:TestTerm1_2",  # New term to add
    )

    # Mutation 2: Add a new term to parent.email field
    mutation2 = FieldTermAdditionMutation(
        dataset_urn=parent_dataset.urn,  # Use URN directly from dataset
        field_name="email",
        term_urn="urn:li:glossaryTerm:TestTerm1_2",  # Same new term
    )

    scenario.add_mutation_objects([mutation1, mutation2])

    return scenario


@pytest.mark.parametrize(
    "scenario_name,scenario_func",
    [
        ("1:1 Term Propagation", create_1_to_1_scenario),
        ("2-hop Term Propagation", create_2_hop_scenario),
    ],
    ids=["1_to_1", "2_hop"],
)
def test_term_propagation_scenarios(
    test_framework: PropagationTestFramework,
    test_action_urn: str,
    create_modern_test_action,
    scenario_name: str,
    scenario_func,
):
    """Test term propagation using the new framework.

    This test demonstrates how the new framework simplifies the original
    term propagation test by using:

    1. PropagationScenarioBuilder for readable scenario creation
    2. TermPropagationExpectation for type-safe expectations
    3. PropagationTestFramework for automated test execution
    4. Built-in cleanup and error handling

    The test maintains the same functionality as the original but with
    significantly less code and better readability.
    """
    logger.info(f"Starting {scenario_name} test with new framework")

    # Create scenario using the factory function
    scenario = scenario_func(test_action_urn)
    # Disable debug mode for faster execution
    scenario.debug_mcps = False

    # Run the test using the framework
    result = test_framework.run_propagation_test(
        scenario=scenario,
        test_action_urn=test_action_urn,
        test_type=TermPropagationTest(),
        scenario_name=scenario_name,
    )

    # Verify the test succeeded
    assert result.success, f"Test failed: {result.error_details}"

    logger.info(f"✅ {scenario_name} completed successfully")
    logger.info(result.get_summary())


def test_simple_term_propagation_example(
    test_framework: PropagationTestFramework,
    test_action_urn: str,
    create_modern_test_action,
):
    """Simple example showing how easy it is to create a term propagation test.

    This demonstrates the most basic usage of the framework.
    """
    # Build a simple scenario with the fluent API
    builder = PropagationScenarioBuilder(test_action_urn, "simple_terms")

    # Create source dataset with terms
    source = (
        builder.add_dataset("customers", "snowflake")
        .with_columns(["id", "name", "email"])
        .with_column_glossary_term("id", "urn:li:glossaryTerm:TestTerm1_1")
        .build()
    )

    # Create target dataset without terms
    target = (
        builder.add_dataset("analytics", "snowflake")
        .with_columns(["customer_id", "name", "email"])
        .build()
    )

    # Register datasets
    builder.register_dataset("source", source)
    builder.register_dataset("target", target)

    # Create simple 1:1 lineage
    lineage = (
        builder.add_lineage("source", "target")
        .add_field_lineage("source", "id", "target", "customer_id")
        .build(source.urn)
    )
    builder.register_lineage("source", "target", lineage)

    # Expect term to propagate (TestTerm1_1 is in target_terms)
    builder.base_expectations = [
        TermPropagationExpectation(
            field_urn=make_schema_field_urn(target.urn, "customer_id"),
            expected_term_urn="urn:li:glossaryTerm:TestTerm1_1",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(source.urn, "id"),
        )
    ]

    scenario = builder.build()

    # Run the test
    result = test_framework.run_propagation_test(
        scenario=scenario,
        test_action_urn=test_action_urn,
        test_type=TermPropagationTest(),
        scenario_name="Simple Term Propagation",
    )

    assert result.success, f"Simple test failed: {result.error_details}"
    logger.info("✅ Simple term propagation test completed successfully")


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
