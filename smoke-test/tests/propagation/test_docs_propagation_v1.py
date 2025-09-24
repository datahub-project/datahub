"""
Rewrite of test_docs_propagation.py using the new propagation framework.

This file contains tests for the documentation propagation feature using the new
framework architecture. The tests are designed to test the propagation of documentation
from one schema field to another via lineage relationships.

The tests cover the following scenarios from the original file:
- 1:1 lineage relationship (1x1 graph)
- 2-hop lineage relationship (2x2 graph)
- Sibling lineage relationship (2x2 graph with siblings)

Each scenario tests:
- Bootstrap: Documentation is propagated correctly at bootstrap
- Live: Documentation is propagated correctly during live mutations
- Rollback: Documentation is rolled back correctly
"""

import logging
from typing import Any

import pytest

from datahub.emitter.mce_builder import make_schema_field_urn
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
    FieldDocumentationUpdateMutation,
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


# Create the test action fixture for documentation propagation with correct action type
def create_configured_doc_test():
    return DocumentationPropagationTest(
        action_type="datahub_integrations.propagation.doc.doc_propagation_action.DocPropagationAction",
        recipe_filename="doc_propagation_action_recipe.yaml",
    )


create_test_action = create_test_action_fixture(create_configured_doc_test)


def create_1x1_graph_scenario(test_action_urn: str) -> Any:
    """
    Create a 1:1 lineage scenario equivalent to create_1_1_graph_lineage.

    Creates 2 datasets with 5 schema fields each.
    The first dataset has documentation on column 0.
    Column 0 has 1:1 lineage (should propagate)
    Columns 1,2 have N:1 lineage to column 1 (should not propagate)
    Other columns have no lineage.
    """
    builder = PropagationScenarioBuilder(test_action_urn, "graph1_1")

    # Create source dataset with documentation on column 0
    source_dataset = (
        builder.add_dataset("table_foo_0", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .with_column_description("column_0", "this is column 0")
        .build()
    )

    # Create target dataset without documentation
    target_dataset = (
        builder.add_dataset("table_foo_1", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .build()
    )

    # Register datasets
    builder.register_dataset("source", source_dataset)
    builder.register_dataset("target", target_dataset)

    # Create lineage: 1:1 for column_0, N:1 for columns 1,2 -> column 1
    lineage = (
        builder.add_lineage("source", "target")
        .add_field_lineage("source", "column_0", "target", "column_0")  # 1:1
        .add_many_to_one_lineage(
            "source",
            ["column_1", "column_2"],
            "target",
            "column_1",  # N:1
        )
        .build(source_dataset.urn)
    )

    builder.register_lineage("source", "target", lineage)

    # Base expectations - only column_0 should propagate (1:1), others should not
    builder.base_expectations = [
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "column_0"),
            expected_description="this is column 0",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(source_dataset.urn, "column_0"),
            propagation_via=None,
        ),
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "column_1"),
        ),
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "column_2"),
        ),
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "column_3"),
        ),
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "column_4"),
        ),
    ]

    # Post-mutation expectations - column_0 should have updated description
    builder.post_mutation_expectations = [
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "column_0"),
            expected_description="this is the updated description",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(source_dataset.urn, "column_0"),
            propagation_via=None,
        ),
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "column_1"),
        ),
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "column_2"),
        ),
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "column_3"),
        ),
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(target_dataset.urn, "column_4"),
        ),
    ]

    # Build scenario first, then add smart mutations
    scenario = builder.build()

    # Create mutation for live testing - update source column_0 description using smart mutations
    mutation = FieldDocumentationUpdateMutation(
        dataset_urn=source_dataset.urn,  # Use URN directly from dataset
        field_name="column_0",
        new_description="this is the updated description",
    )
    scenario.add_mutation_objects([mutation])

    return scenario


def create_2x2_graph_scenario(test_action_urn: str) -> Any:
    """
    Create a 2-hop lineage scenario equivalent to create_2_2_graph_lineage.

    Creates 3 datasets with 5 schema fields each.
    First hop: dataset1 -> dataset2 (1:1 lineage on column_0)
    Second hop: dataset2 -> dataset3 (1:1 lineage on all columns)
    Tests 2-hop propagation: dataset1.column_0 -> dataset2.column_0 -> dataset3.column_0
    """
    builder = PropagationScenarioBuilder(test_action_urn, "graph2_2")

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
        .add_field_lineage("dataset1", "column_0", "dataset2", "column_0")  # 1:1
        .add_many_to_one_lineage(
            "dataset1",
            ["column_1", "column_2"],
            "dataset2",
            "column_1",  # N:1
        )
        .build(dataset1.urn)
    )
    builder.register_lineage("dataset1", "dataset2", lineage1)

    # Create second hop lineage (dataset2 -> dataset3) - all 1:1
    lineage2 = (
        builder.add_lineage("dataset2", "dataset3")
        .add_field_lineage("dataset2", "column_0", "dataset3", "column_0")
        .add_field_lineage("dataset2", "column_1", "dataset3", "column_1")
        .add_field_lineage("dataset2", "column_2", "dataset3", "column_2")
        .add_field_lineage("dataset2", "column_3", "dataset3", "column_3")
        .add_field_lineage("dataset2", "column_4", "dataset3", "column_4")
        .build(dataset2.urn)
    )
    builder.register_lineage("dataset2", "dataset3", lineage2)

    # Base expectations - Bootstrap creates both 1-hop and 2-hop propagation with DocPropagationAction
    # dataset1.column_0 -> dataset2.column_0 (direct 1:1)
    # dataset1.column_0 -> dataset2.column_0 -> dataset3.column_0 (2-hop)
    builder.base_expectations = [
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(
                dataset2.urn, "column_0"
            ),  # First hop: dataset1 -> dataset2
            expected_description="this is column 0",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(dataset1.urn, "column_0"),
            propagation_via=None,  # Direct 1-hop propagation
        ),
        # dataset3.column_0 SHOULD have 2-hop propagation during bootstrap with DocPropagationAction
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(dataset3.urn, "column_0"),
            expected_description="this is column 0",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(dataset1.urn, "column_0"),
            propagation_via=make_schema_field_urn(
                dataset2.urn, "column_0"
            ),  # 2-hop propagation via dataset2
        ),
        # Other columns should not have propagation
        # Note: column_1 gets directly mutated during live phase, so mark as non-propagated
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(dataset2.urn, "column_1"),
            is_propagated=False,  # This field gets directly mutated, skip during rollback
        ),
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(dataset3.urn, "column_1"),
        ),
    ]

    # Post-mutation expectations - Only the new propagation from the live mutation
    builder.post_mutation_expectations = [
        # The new propagation from mutation: dataset2.column_1 -> dataset3.column_1
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(dataset3.urn, "column_1"),
            expected_description="this is the updated description",  # Updated description from mutation
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(dataset2.urn, "column_1"),
            propagation_via=None,  # Direct 1-hop propagation
        ),
        # Verify that column_0 has no propagation after rollback (the original bootstrap propagation was correctly removed)
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(dataset3.urn, "column_0"),
        ),
    ]

    # Build scenario first, then add smart mutations
    scenario = builder.build()

    # Create mutation for dataset2 column_1 to trigger additional propagation using smart mutations
    mutation = FieldDocumentationUpdateMutation(
        dataset_urn=dataset2.urn,  # Use URN directly from dataset
        field_name="column_1",
        new_description="this is the updated description",
    )
    scenario.add_mutation_objects([mutation])

    return scenario


def create_2x2_graph_siblings_scenario(test_action_urn: str) -> Any:
    """
    Create a sibling relationship scenario equivalent to create_2_2_graph_lineage_siblings.

    This builds upon the 1:1 graph (with column_docs=False) and adds a dbt dataset with sibling relationship.
    dataset_1 = snowflake, table_foo_0 (no docs initially)
    dataset_2 = snowflake, table_foo_1 (no docs initially)
    dataset_3 = dbt, table_foo_2 (has docs, sibling to dataset_1)

    Expected propagation: dbt.dataset_3 -> snowflake.dataset_1 (sibling) -> snowflake.dataset_2 (lineage)
    """
    builder = PropagationScenarioBuilder(test_action_urn, "graph2_2_lineage_siblings")

    # First, create the base 1:1 scenario without column docs (like original)
    # Create snowflake datasets without documentation
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

    # Register the base datasets
    builder.register_dataset("dataset1", dataset1)
    builder.register_dataset("dataset2", dataset2)

    # Create lineage between snowflake datasets (like original: 1:1 and N:1)
    lineage = (
        builder.add_lineage("dataset1", "dataset2")
        .add_field_lineage("dataset1", "column_0", "dataset2", "column_0")  # 1:1
        .add_many_to_one_lineage(
            "dataset1",
            ["column_1", "column_2"],
            "dataset2",
            "column_1",  # N:1
        )
        .build(dataset1.urn)
    )
    builder.register_lineage("dataset1", "dataset2", lineage)

    # Now create dbt dataset with documentation (acts as source)
    dataset3 = (
        builder.add_dataset("table_foo_2", "dbt")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .with_column_description("column_0", "Description for dbt column 0")
        .with_column_description("column_1", "Description for dbt column 1")
        .with_column_description("column_2", "Description for dbt column 2")
        .with_column_description("column_3", "Description for dbt column 3")
        .with_column_description("column_4", "Description for dbt column 4")
        .with_subtype("Source")
        .set_sibling(
            dataset1, is_primary=True
        )  # Set dataset1 as sibling, dataset3 is primary
        .build()
    )

    builder.register_dataset("dataset3", dataset3)

    # Base expectations - Both should work during bootstrap to match original test
    builder.base_expectations = [
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(dataset1.urn, "column_0"),
            expected_description="Description for dbt column 0",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(dataset3.urn, "column_0"),
            propagation_via=None,  # Direct sibling propagation
        ),
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(dataset2.urn, "column_0"),
            expected_description="Description for dbt column 0",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(dataset3.urn, "column_0"),
            propagation_via=make_schema_field_urn(dataset1.urn, "column_0"),
        ),
    ]

    # No mutations like the original sibling test
    builder.mutations = []
    builder.post_mutation_expectations = []

    return builder.build()


@pytest.mark.parametrize(
    "scenario_name,scenario_func",
    [
        ("1x1 graph", create_1x1_graph_scenario),
        ("2x2 graph", create_2x2_graph_scenario),
        ("2x2 graph with siblings", create_2x2_graph_siblings_scenario),
    ],
    ids=["1x1_graph", "2x2_graph", "2x2_graph_siblings"],
)
def test_documentation_propagation_scenarios(
    test_framework: PropagationTestFramework,
    test_action_urn: str,
    create_test_action,
    scenario_name: str,
    scenario_func,
):
    """
    Test documentation propagation using the new framework.

    This is a rewrite of the original test_main_loop function using the new
    propagation framework. It maintains the same test scenarios but with
    significantly simplified implementation:

    1. Uses PropagationScenarioBuilder for readable scenario creation
    2. Uses DocumentationPropagationExpectation for type-safe expectations
    3. Uses PropagationTestFramework for automated test execution
    4. Built-in cleanup and error handling

    The test covers the same functionality as the original:
    - Bootstrap phase: Tests that documentation propagates correctly at bootstrap
    - Live phase: Tests that documentation propagates correctly during mutations
    - Rollback phase: Tests that documentation is rolled back correctly
    """
    logger.info(f"Starting {scenario_name} test with new framework")

    # Create scenario using the factory function
    scenario = scenario_func(test_action_urn)

    # Run the test using the framework (action already configured in fixture)
    result = test_framework.run_propagation_test(
        scenario=scenario,
        test_action_urn=test_action_urn,
        test_type=create_configured_doc_test(),
        scenario_name=scenario_name,
    )

    # Verify the test succeeded
    assert result.success, f"Test failed: {result.error_details}"

    logger.info(f"✅ {scenario_name} completed successfully")
    logger.info(result.get_summary())
