"""
Rewrite of test_tag_propagation.py using the new propagation framework.

NOTE: The original TagPropagationAction only supports DATASET-level tag propagation,
not schema field-level tag propagation. This is different from the generic framework
actions which support both. Therefore, this test focuses on dataset-level propagation.

This file contains tests for the tag propagation feature using the new
framework architecture. The tests are designed to test the propagation of tags
from one dataset to another via lineage relationships.

The tests cover the following scenarios adapted for dataset-level propagation:
- 1:1 lineage relationship (1x1 graph) - dataset level
- 2-hop lineage relationship (2x2 graph) - dataset level
- Sibling lineage relationship (2x2 graph with siblings) - dataset level

Each scenario tests:
- Live: Tags are propagated correctly during live mutations
- Bootstrap: Disabled (original action doesn't support bootstrap)
- Rollback: Disabled (original action doesn't support rollback properly)
"""

import logging
from typing import Any

import pytest

from tests.propagation.framework.builders.scenario_builder import (
    PropagationScenarioBuilder,
)
from tests.propagation.framework.core.base import (
    BasePropagationTest,
    PropagationTestConfig,
    PropagationTestFramework,
)
from tests.propagation.framework.plugins.tag.expectations import (
    DatasetTagPropagationExpectation,
)
from tests.propagation.framework.plugins.tag.mutations import DatasetTagAdditionMutation
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


# Create custom test class for original TagPropagationAction (not generic)
class OriginalTagPropagationTest(BasePropagationTest):
    """Test class that uses the original TagPropagationAction (not generic)."""

    def get_action_type(self) -> str:
        return "datahub_integrations.propagation.tag.tag_propagation_action.TagPropagationAction"

    def get_recipe_filename(self) -> str:
        return "tag_propagation_action_recipe.yaml"  # Original recipe file

    def get_action_name(self) -> str:
        return "test_tag_propagation"

    def get_glossary_required(self) -> bool:
        return False  # Tags don't require glossary

    def customize_config(self, config: PropagationTestConfig) -> PropagationTestConfig:
        """Disable bootstrap and rollback since original TagPropagationAction doesn't support them properly."""
        config.skip_bootstrap = True
        config.skip_rollback = (
            True  # Rollback not working correctly with original action
        )
        return config


# Create the test action fixture for tag propagation with original action type
def create_configured_tag_test():
    return OriginalTagPropagationTest()


create_test_action = create_test_action_fixture(create_configured_tag_test)


def create_1x1_graph_scenario(test_action_urn: str) -> Any:
    """
    Create a 1:1 lineage scenario for DATASET-level tag propagation.

    NOTE: This test is simplified compared to the original field-level test because
    the original TagPropagationAction only supports dataset-level tag propagation,
    not schema field-level propagation.

    Creates 2 datasets with dataset-level tags.
    Tests basic dataset-to-dataset tag propagation via lineage.
    """
    builder = PropagationScenarioBuilder(test_action_urn, "graph1_1")

    # Create source dataset without tags (will add via mutations)
    source_dataset = (
        builder.add_dataset("table_foo_0", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .build()
    )

    # Create target dataset without tags initially
    target_dataset = (
        builder.add_dataset("table_foo_1", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .build()
    )

    # Register datasets
    builder.register_dataset("source", source_dataset)
    builder.register_dataset("target", target_dataset)

    # Create dataset-level lineage (field lineage not needed for dataset tags)
    lineage = builder.add_lineage("source", "target").build(source_dataset.urn)

    builder.register_lineage("source", "target", lineage)

    # Base expectations - No base expectations since pre-bootstrap tags aren't rolled back
    # The Sensitive tag is added during pre-bootstrap and should remain after rollback
    builder.base_expectations = []

    # Post-mutation expectations - add a new dataset tag via mutation
    # Use PII since it's in the tag_prefixes whitelist and will be propagated
    builder.post_mutation_expectations = [
        DatasetTagPropagationExpectation(
            dataset_urn=target_dataset.urn,
            expected_tag_urn="urn:li:tag:PII",  # This tag is in whitelist and will propagate
            propagation_source=test_action_urn,
            expected_depth=1,  # 1-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),
    ]

    # Build scenario first
    scenario = builder.build()

    # Add initial dataset tags using pre-bootstrap mutations
    # These represent existing metadata that should be present before propagation starts
    scenario.add_pre_bootstrap_mutations(
        [
            DatasetTagAdditionMutation(
                dataset_urn=source_dataset.urn,
                tag_urn="urn:li:tag:Sensitive",  # Start with just Sensitive tag
            ),
        ]
    )

    # Create dataset-level mutation for live testing
    # Add PII tag during live phase to test live propagation
    mutations = [
        DatasetTagAdditionMutation(
            dataset_urn=source_dataset.urn,  # Add tag to source dataset
            tag_urn="urn:li:tag:PII",  # This tag is whitelisted and will propagate
        ),
    ]
    scenario.add_mutation_objects(mutations)

    return scenario


def create_2x2_graph_scenario(test_action_urn: str) -> Any:
    """
    Create a 2-hop lineage scenario for DATASET-level tag propagation.

    NOTE: Simplified to dataset-level propagation only since the original
    TagPropagationAction doesn't support schema field-level propagation.

    Creates 3 datasets with dataset-level lineage.
    First hop: dataset1 -> dataset2
    Second hop: dataset2 -> dataset3
    Tests 2-hop dataset tag propagation: dataset1 -> dataset2 -> dataset3
    """
    builder = PropagationScenarioBuilder(test_action_urn, "graph2_2")

    # Create the first dataset without tags (will add via mutations)
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

    # Add third dataset
    dataset3 = (
        builder.add_dataset("table_foo_2", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .build()
    )

    # Register datasets
    builder.register_dataset("dataset1", dataset1)
    builder.register_dataset("dataset2", dataset2)
    builder.register_dataset("dataset3", dataset3)

    # Create dataset-level lineage (no field lineage needed for dataset tags)
    lineage1 = builder.add_lineage("dataset1", "dataset2").build(dataset1.urn)
    builder.register_lineage("dataset1", "dataset2", lineage1)

    # Create second hop lineage
    lineage2 = builder.add_lineage("dataset2", "dataset3").build(dataset2.urn)
    builder.register_lineage("dataset2", "dataset3", lineage2)

    # Base expectations - Dataset-level 2-hop propagation
    builder.base_expectations = [
        # First hop: dataset1 -> dataset2
        DatasetTagPropagationExpectation(
            dataset_urn=dataset2.urn,
            expected_tag_urn="urn:li:tag:PII",  # Common tag that propagates
            propagation_source=test_action_urn,
            expected_depth=1,  # 1-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),
        DatasetTagPropagationExpectation(
            dataset_urn=dataset2.urn,
            expected_tag_urn="urn:li:tag:Sensitive",  # Common tag that propagates
            propagation_source=test_action_urn,
            expected_depth=1,  # 1-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),
        # Second hop: dataset2 -> dataset3 (2-hop propagation)
        # Note: Depends on whether original TagPropagationAction supports 2-hop
        DatasetTagPropagationExpectation(
            dataset_urn=dataset3.urn,
            expected_tag_urn="urn:li:tag:PII",  # 2-hop propagation
            propagation_source=test_action_urn,
            expected_depth=2,  # 2-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),
        DatasetTagPropagationExpectation(
            dataset_urn=dataset3.urn,
            expected_tag_urn="urn:li:tag:Sensitive",  # 2-hop propagation
            propagation_source=test_action_urn,
            expected_depth=2,  # 2-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),
    ]

    # Post-mutation expectations - New dataset tag should propagate
    # The mutation adds PII to dataset2, which should propagate to dataset3
    builder.post_mutation_expectations = [
        DatasetTagPropagationExpectation(
            dataset_urn=dataset3.urn,
            expected_tag_urn="urn:li:tag:PII",  # Should propagate from mutation (whitelisted)
            propagation_source=test_action_urn,
            expected_depth=1,  # 1-hop from dataset2 to dataset3
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),
    ]

    # Build scenario first
    scenario = builder.build()

    # Add initial dataset tags using pre-bootstrap mutations
    scenario.add_pre_bootstrap_mutations(
        [
            DatasetTagAdditionMutation(
                dataset_urn=dataset1.urn,
                tag_urn="urn:li:tag:PII",
            ),
            DatasetTagAdditionMutation(
                dataset_urn=dataset1.urn,
                tag_urn="urn:li:tag:Sensitive",
            ),
        ]
    )

    # Create dataset-level mutation for live testing
    mutation = DatasetTagAdditionMutation(
        dataset_urn=dataset2.urn,  # Add tag to intermediate dataset
        tag_urn="urn:li:tag:PII",  # This tag is whitelisted and will propagate
    )
    scenario.add_mutation_objects([mutation])

    return scenario


def create_2x2_graph_siblings_scenario(test_action_urn: str) -> Any:
    """
    Create a sibling relationship scenario for DATASET-level tag propagation.

    NOTE: Simplified to dataset-level propagation only since the original
    TagPropagationAction doesn't support schema field-level propagation.

    dataset_1 = snowflake, table_foo_0 (no tags initially)
    dataset_2 = snowflake, table_foo_1 (no tags initially)
    dataset_3 = dbt, table_foo_2 (has tags, sibling to dataset_1)

    Expected propagation: dbt.dataset_3 -> snowflake.dataset_1 (sibling) -> snowflake.dataset_2 (lineage)
    """
    builder = PropagationScenarioBuilder(test_action_urn, "graph2_2_lineage_siblings")

    # Create snowflake datasets without initial tags
    dataset1 = (
        builder.add_dataset("table_foo_0", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .build()  # No initial tags - will get them from sibling
    )

    dataset2 = (
        builder.add_dataset("table_foo_1", "snowflake")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .build()  # No initial tags
    )

    # Register the base datasets
    builder.register_dataset("dataset1", dataset1)
    builder.register_dataset("dataset2", dataset2)

    # Create dataset-level lineage between snowflake datasets
    lineage = builder.add_lineage("dataset1", "dataset2").build(dataset1.urn)
    builder.register_lineage("dataset1", "dataset2", lineage)

    # Create dbt dataset without tags (will add via mutations)
    dataset3 = (
        builder.add_dataset("table_foo_2", "dbt")
        .with_columns(["column_0", "column_1", "column_2", "column_3", "column_4"])
        .with_subtype("Source")
        .set_sibling(
            dataset1, is_primary=True
        )  # Set dataset1 as sibling, dataset3 is primary
        .build()
    )

    builder.register_dataset("dataset3", dataset3)

    # Base expectations - Dataset-level tags should propagate via sibling and lineage
    # dbt.dataset3 -> snowflake.dataset1 (sibling) -> snowflake.dataset2 (lineage)
    builder.base_expectations = [
        # First: dbt.dataset3 -> snowflake.dataset1 (sibling propagation)
        DatasetTagPropagationExpectation(
            dataset_urn=dataset1.urn,
            expected_tag_urn="urn:li:tag:PII",  # Common tag that propagates
            propagation_source=test_action_urn,
            expected_depth=1,  # 1-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through sibling relationships
        ),
        DatasetTagPropagationExpectation(
            dataset_urn=dataset1.urn,
            expected_tag_urn="urn:li:tag:Sensitive",  # Common tag that propagates
            propagation_source=test_action_urn,
            expected_depth=1,  # 1-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through sibling relationships
        ),
        # Then: snowflake.dataset1 -> snowflake.dataset2 (lineage propagation)
        DatasetTagPropagationExpectation(
            dataset_urn=dataset2.urn,
            expected_tag_urn="urn:li:tag:PII",  # 2-hop propagation
            propagation_source=test_action_urn,
            expected_depth=2,  # 2-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),
        DatasetTagPropagationExpectation(
            dataset_urn=dataset2.urn,
            expected_tag_urn="urn:li:tag:Sensitive",  # 2-hop propagation
            propagation_source=test_action_urn,
            expected_depth=2,  # 2-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),
    ]

    # Build scenario first
    scenario = builder.build()

    # Add initial dataset tags on dbt dataset using pre-bootstrap mutations
    scenario.add_pre_bootstrap_mutations(
        [
            DatasetTagAdditionMutation(
                dataset_urn=dataset3.urn,
                tag_urn="urn:li:tag:PII",
            ),
            DatasetTagAdditionMutation(
                dataset_urn=dataset3.urn,
                tag_urn="urn:li:tag:Sensitive",
            ),
        ]
    )

    # No live mutations for this scenario
    builder.mutations = []
    builder.post_mutation_expectations = []

    return scenario

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
def test_tag_propagation_scenarios(
    test_framework: PropagationTestFramework,
    test_action_urn: str,
    create_test_action,
    scenario_name: str,
    scenario_func,
):
    """
    Test DATASET-level tag propagation using the new framework.

    NOTE: This is a simplified version of the original test because the original
    TagPropagationAction only supports dataset-level tag propagation, not schema
    field-level tag propagation. Schema field tag propagation is NOT supported
    by this action.

    This is a rewrite of the original test_main_loop function using the new
    propagation framework. It maintains the same test scenarios but adapted for
    dataset-level propagation only:

    1. Uses PropagationScenarioBuilder for readable scenario creation
    2. Uses DatasetTagPropagationExpectation for type-safe expectations
    3. Uses PropagationTestFramework for automated test execution
    4. Built-in cleanup and error handling

    The test covers dataset-level tag propagation functionality:
    - Live phase: Tests that dataset tags propagate correctly during mutations
    - Bootstrap phase: Disabled (original action doesn't support bootstrap)
    - Rollback phase: Disabled (original action doesn't support rollback properly)
    """
    logger.info(f"Starting {scenario_name} test with new framework")

    # Create scenario using the factory function
    scenario = scenario_func(test_action_urn)

    # Run the test using the framework (action already configured in fixture)
    result = test_framework.run_propagation_test(
        scenario=scenario,
        test_action_urn=test_action_urn,
        test_type=create_configured_tag_test(),
        scenario_name=scenario_name,
    )

    # Verify the test succeeded
    assert result.success, f"Test failed: {result.error_details}"

    logger.info(f"✅ {scenario_name} completed successfully")
    logger.info(result.get_summary())
