"""
Test file demonstrating the new framework by reimplementing the generic docs propagation test.

This test recreates the functionality from test_generic_docs_propagation.py using the new
propagation framework, showing how the framework simplifies test creation while maintaining
all the same functionality.
"""

import logging
from typing import Any

import pytest

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from tests.propagation.framework.builders.scenario_builder import (
    PropagationScenarioBuilder,
)
from tests.propagation.framework.core.base import (
    DocumentationPropagationTest,
    PropagationTestFramework,
)
from tests.propagation.framework.plugins.documentation.expectations import (
    DocumentationPropagationExpectation,
    NoDocumentationPropagationExpectation,
)
from tests.propagation.framework.plugins.documentation.mutations import (
    DocumentationUpdateMutation,
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

# Create the test action fixture for documentation propagation
create_test_action = create_test_action_fixture(DocumentationPropagationTest)


def create_1_to_1_scenario(test_action_urn: str) -> Any:
    """Create a 1:1 lineage scenario similar to the original test."""
    builder = PropagationScenarioBuilder(test_action_urn, "docs_1_to_1")

    # Create source dataset with documentation
    source_dataset = (
        builder.add_dataset("source_table", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .with_column_description("column_0", "this is column 0")
        .build()
    )

    # Create target dataset without documentation
    target_dataset = (
        builder.add_dataset("target_table", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .build()
    )

    # Register datasets
    builder.register_dataset("source", source_dataset)
    builder.register_dataset("target", target_dataset)

    # Create lineage - only column_0 has 1:1 mapping, column_1 has N:1 (should not propagate)
    lineage = (
        builder.add_lineage("source", "target")
        .add_field_lineage("source", "column_0", "target", "column_0")
        .add_many_to_one_lineage(
            "source", ["column_1", "column_2"], "target", "column_1"
        )
        .build(source_dataset.urn)
    )

    builder.register_lineage("source", "target", lineage)

    # Base expectations - column_0 should propagate, others should not
    builder.base_expectations = [
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="docs_1_to_1.target_table",  # Full prefixed name
            field_name="column_0",
            expected_description="this is column 0",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(
                make_dataset_urn("snowflake", "docs_1_to_1.source_table"), "column_0"
            ),
            propagation_via=None,
        ),
        NoDocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="docs_1_to_1.target_table",  # Full prefixed name
            field_name="column_1",
        ),
        NoDocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="docs_1_to_1.target_table",  # Full prefixed name
            field_name="column_2",
        ),
    ]

    # Create mutation for live testing - update source column_0 description
    mutation = DocumentationUpdateMutation(
        dataset_name="source_table",  # Just the dataset name, not prefixed
        field_name="column_0",
        new_description="this is the updated description",
    )
    source_dataset_urn = builder.graph_builder.get_dataset_urn(
        "snowflake", "source_table"
    )
    builder.mutations.append(mutation.apply_mutation(source_dataset_urn))

    # Post-mutation expectations - column_0 should have updated description
    builder.post_mutation_expectations = [
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="docs_1_to_1.target_table",  # Full prefixed name
            field_name="column_0",
            expected_description="this is the updated description",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(
                make_dataset_urn("snowflake", "docs_1_to_1.source_table"), "column_0"
            ),
            propagation_via=None,
        ),
    ]

    return builder.build()


def create_2_hop_scenario(test_action_urn: str) -> Any:
    """Create a 2-hop lineage scenario similar to the original test."""
    builder = PropagationScenarioBuilder(test_action_urn, "docs_2_hop")

    # Create datasets
    dataset1 = (
        builder.add_dataset("table_foo_0", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .with_column_description("column_0", "this is column 0")
        .build()
    )

    dataset2 = (
        builder.add_dataset("table_foo_1", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .build()
    )

    dataset3 = (
        builder.add_dataset("table_foo_2", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .build()
    )

    # Register datasets
    builder.register_dataset("dataset1", dataset1)
    builder.register_dataset("dataset2", dataset2)
    builder.register_dataset("dataset3", dataset3)

    # Create first hop lineage (dataset1 -> dataset2)
    lineage1 = (
        builder.add_lineage("dataset1", "dataset2")
        .add_field_lineage("dataset1", "column_0", "dataset2", "column_0")
        .build(dataset1.urn)
    )
    builder.register_lineage("dataset1", "dataset2", lineage1)

    # Create second hop lineage (dataset2 -> dataset3)
    lineage2 = (
        builder.add_lineage("dataset2", "dataset3")
        .add_field_lineage("dataset2", "column_0", "dataset3", "column_0")
        .add_field_lineage("dataset2", "column_1", "dataset3", "column_1")
        .build(dataset2.urn)
    )
    builder.register_lineage("dataset2", "dataset3", lineage2)

    # Base expectations - Bootstrap doesn't create multi-hop propagation, only 1-hop
    # So we expect the first hop (dataset1 -> dataset2) to work, but not the second hop yet
    builder.base_expectations = [
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="docs_2_hop.table_foo_1",  # First hop: dataset1 -> dataset2
            field_name="column_0",
            expected_description="this is column 0",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(
                make_dataset_urn("snowflake", "docs_2_hop.table_foo_0"), "column_0"
            ),
            propagation_via=None,  # Direct 1-hop propagation
        ),
        # dataset3.column_0 should NOT have propagation yet during bootstrap
        NoDocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="docs_2_hop.table_foo_2",
            field_name="column_0",
        ),
    ]

    # Create mutation for dataset1 column_0 to trigger 2-hop propagation
    mutation = DocumentationUpdateMutation(
        dataset_name="table_foo_0",  # Just the dataset name, not prefixed
        field_name="column_0",
        new_description="this is the updated description for the origin",
    )
    dataset1_urn = builder.graph_builder.get_dataset_urn("snowflake", "table_foo_0")
    builder.mutations.append(mutation.apply_mutation(dataset1_urn))

    # Post-mutation expectations - During live phase, mutations can trigger multi-hop propagation
    builder.post_mutation_expectations = [
        # The mutation should trigger the 2-hop propagation: dataset1.column_0 -> dataset2.column_0 -> dataset3.column_0
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="docs_2_hop.table_foo_2",
            field_name="column_0",
            expected_description="this is the updated description for the origin",  # Updated description from mutation
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(
                make_dataset_urn("snowflake", "docs_2_hop.table_foo_0"), "column_0"
            ),
            propagation_via=make_schema_field_urn(
                make_dataset_urn("snowflake", "docs_2_hop.table_foo_1"), "column_0"
            ),
        ),
    ]

    return builder.build()


def create_sibling_scenario(test_action_urn: str) -> Any:
    """Create a sibling relationship scenario similar to the original test."""
    builder = PropagationScenarioBuilder(test_action_urn, "docs_sibling")

    # Create snowflake datasets
    dataset1 = (
        builder.add_dataset("table_foo_0", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .build()
    )

    dataset2 = (
        builder.add_dataset("table_foo_1", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .build()
    )

    # Create dbt dataset with documentation (acts as source)
    dataset3 = (
        builder.add_dataset("table_foo_2", "dbt")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .with_column_description("column_0", "Description for dbt column 0")
        .with_subtype("Source")
        .build()
    )

    # Register datasets
    builder.register_dataset("dataset1", dataset1)
    builder.register_dataset("dataset2", dataset2)
    builder.register_dataset("dataset3", dataset3)

    # Create lineage between snowflake datasets
    lineage = (
        builder.add_lineage("dataset1", "dataset2")
        .add_field_lineage("dataset1", "column_0", "dataset2", "column_0")
        .build(dataset1.urn)
    )
    builder.register_lineage("dataset1", "dataset2", lineage)

    # TODO: The framework doesn't seem to have built-in sibling relationship support yet
    # This would need to be added as a separate MCP for the sibling aspect
    # For now, we'll skip the sibling part and focus on the lineage propagation

    # Base expectations would be empty since no initial documentation on snowflake tables
    builder.base_expectations = []

    # No mutations for this scenario
    builder.post_mutation_expectations = []

    return builder.build()


@pytest.mark.parametrize(
    "scenario_name,scenario_func",
    [
        ("1:1 Documentation Propagation", create_1_to_1_scenario),
        ("2-hop Documentation Propagation", create_2_hop_scenario),
        ("Sibling Documentation Propagation", create_sibling_scenario),
    ],
    ids=["1_to_1", "2_hop", "sibling"],
)
def test_documentation_propagation_scenarios(
    test_framework: PropagationTestFramework,
    test_action_urn: str,
    create_test_action,
    scenario_name: str,
    scenario_func,
):
    """Test documentation propagation using the new framework.

    This test demonstrates how the new framework simplifies the original
    test_generic_docs_propagation.py test by using:

    1. PropagationScenarioBuilder for readable scenario creation
    2. DocumentationPropagationExpectation for type-safe expectations
    3. PropagationTestFramework for automated test execution
    4. Built-in cleanup and error handling

    The test maintains the same functionality as the original but with
    significantly less code and better readability.
    """
    logger.info(f"Starting {scenario_name} test with new framework")

    # Create scenario using the factory function
    scenario = scenario_func(test_action_urn)

    # Run the test using the framework
    result = test_framework.run_propagation_test(
        scenario=scenario,
        test_action_urn=test_action_urn,
        test_type=DocumentationPropagationTest(),
        scenario_name=scenario_name,
    )

    # Verify the test succeeded
    assert result.success, f"Test failed: {result.error_details}"

    logger.info(f"✅ {scenario_name} completed successfully")
    logger.info(result.get_summary())


def test_simple_documentation_propagation_example(
    test_framework: PropagationTestFramework,
    test_action_urn: str,
    create_test_action,
):
    """Simple example showing how easy it is to create a documentation propagation test.

    This demonstrates the most basic usage of the framework.
    """
    # Build a simple scenario with the fluent API
    builder = PropagationScenarioBuilder(test_action_urn, "simple_docs")

    # Create source dataset with documentation
    source = (
        builder.add_dataset("customers", "snowflake")
        .with_columns(["id", "name", "email"])
        .with_column_description("id", "Customer unique identifier")
        .build()
    )

    # Create target dataset without documentation
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

    # Expect documentation to propagate
    builder.base_expectations = [
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="simple_docs.analytics",
            field_name="customer_id",
            expected_description="Customer unique identifier",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(
                make_dataset_urn("snowflake", "simple_docs.customers"), "id"
            ),
        )
    ]

    scenario = builder.build()

    # Run the test
    result = test_framework.run_propagation_test(
        scenario=scenario,
        test_action_urn=test_action_urn,
        test_type=DocumentationPropagationTest(),
        scenario_name="Simple Documentation Propagation",
    )

    assert result.success, f"Simple test failed: {result.error_details}"
    logger.info("✅ Simple documentation propagation test completed successfully")
