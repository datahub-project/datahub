"""Utility functions and patterns for propagation tests."""

import random
import time
from typing import TYPE_CHECKING, Iterator, List, Optional, Type

if TYPE_CHECKING:
    from tests.propagation.framework.core.models import PropagationTestScenario

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.propagation.framework.core.action_manager import ActionManager
from tests.propagation.framework.core.base import (
    BasePropagationTest,
    PropagationTestConfig,
    PropagationTestFramework,
)
from tests.utils import wait_for_writes_to_sync


def create_standard_fixtures():
    """Create standard pytest fixtures for propagation tests.

    Returns tuple of fixture functions that can be used in test files.
    """

    @pytest.fixture(scope="module")
    def test_resources_dir(pytestconfig):
        """Path to test resources directory."""
        return pytestconfig.rootpath / "tests/propagation/"

    @pytest.fixture(scope="function")
    def test_action_urn():
        """Unique URN for test action."""

        return f"urn:li:dataHubAction:{int(time.time() * 1000)}_{random.randint(1000, 9999)}"

    @pytest.fixture(scope="module")
    def load_glossary(auth_session, test_resources_dir):
        """Load test glossary terms for term propagation tests."""
        glossary_file = test_resources_dir / "test_glossary.dhub.yaml"

        pipeline = {
            "source": {
                "type": "datahub-business-glossary",
                "config": {"file": str(glossary_file)},
            },
            "sink": {
                "type": "datahub-rest",
                "config": {
                    "server": auth_session.gms_url(),
                    "token": auth_session.gms_token(),
                },
            },
        }

        ingest_pipeline = Pipeline.create(config_dict=pipeline)
        ingest_pipeline.run()
        ingest_pipeline.raise_from_status()
        wait_for_writes_to_sync()

    @pytest.fixture(scope="function")
    def test_framework(
        auth_session, graph_client, test_resources_dir
    ) -> PropagationTestFramework:
        """Create enhanced test framework instance."""
        return PropagationTestFramework(
            auth_session, graph_client, str(test_resources_dir)
        )

    @pytest.fixture(scope="function")
    def resilient_test_framework(
        auth_session, graph_client, test_resources_dir
    ) -> PropagationTestFramework:
        """Create test framework with resilient configuration for environment issues."""
        config = PropagationTestConfig(
            skip_bootstrap=False,  # Try bootstrap but handle failures gracefully
            bootstrap_timeout=60,  # Shorter timeout
            live_timeout=60,
            verbose_logging=True,
            fail_fast=False,  # Continue even if bootstrap fails
        )
        return PropagationTestFramework(
            auth_session, graph_client, str(test_resources_dir), config
        )

    def create_test_action_fixture(test_type: Type[BasePropagationTest]):
        """Create a test action fixture for a specific propagation type."""

        @pytest.fixture(scope="function")
        def create_test_action(
            auth_session, graph_client, test_resources_dir, test_action_urn
        ) -> Iterator[None]:
            """Create and clean up test action."""
            action_manager = ActionManager(
                auth_session, graph_client, str(test_resources_dir)
            )
            test_instance = test_type()
            yield from action_manager.create_test_action(
                action_urn=test_action_urn,
                recipe_filename=test_instance.get_recipe_filename(),
                action_name=test_instance.get_action_name(),
                action_type=test_instance.get_action_type(),
            )

        return create_test_action

    # Return all fixtures
    return (
        test_resources_dir,
        test_action_urn,
        load_glossary,
        test_framework,
        resilient_test_framework,
        create_test_action_fixture,
    )


def run_simple_propagation_test(
    test_framework: PropagationTestFramework,
    test_action_urn: str,
    scenario_func,
    test_type: BasePropagationTest,
    scenario_name: str = "Propagation Test",
):
    """Simple helper function to run a propagation test.

    Args:
        test_framework: The test framework instance
        test_action_urn: URN for the test action
        scenario_func: Function that creates the test scenario
        test_type: Type of propagation test
        scenario_name: Descriptive name for the test
    """
    scenario = scenario_func(test_action_urn)
    result = test_framework.run_propagation_test(
        scenario, test_action_urn, test_type, scenario_name
    )

    # Assert success
    assert result.success, f"Test failed: {result.error_details}"

    return result


def create_parameterized_test(scenarios: list, test_type: Type[BasePropagationTest]):
    """Create a parameterized test function for multiple scenarios.

    Args:
        scenarios: List of (scenario_name, scenario_function) tuples
        test_type: Type of propagation test

    Returns:
        Parameterized test function
    """

    @pytest.mark.parametrize(
        "scenario_name,scenario_func",
        scenarios,
        ids=[s[0] for s in scenarios],
    )
    def test_propagation(
        test_framework: PropagationTestFramework,
        test_action_urn: str,
        create_test_action,
        scenario_name: str,
        scenario_func,
    ) -> None:
        """Parameterized propagation test."""
        test_instance = test_type()
        scenario = scenario_func(test_action_urn)
        result = test_framework.run_propagation_test(
            scenario, test_action_urn, test_instance, scenario_name
        )
        assert result.success, f"Test failed: {result.error_details}"

    return test_propagation


class TestScenarioLibrary:
    """Library of common test scenario patterns.

    Provides reusable scenario builders for common propagation test patterns.
    """

    @staticmethod
    def simple_1_to_1_lineage(
        test_action_urn: str,
        upstream_term: str = "urn:li:glossaryTerm:TestTerm1_1",
        prefix: str = "simple",
    ) -> "PropagationTestScenario":
        """Create a simple 1:1 lineage scenario with term propagation."""
        from tests.propagation.framework.builders.scenario_builder import (
            PropagationScenarioBuilder,
        )

        builder = PropagationScenarioBuilder(test_action_urn, prefix)

        # Create datasets
        upstream = (
            builder.add_dataset("upstream")
            .with_description("Upstream dataset")
            .with_columns(["column_0", "column_1", "column_2"])
            .with_column_glossary_term("column_0", upstream_term)
            .build()
        )

        downstream = (
            builder.add_dataset("downstream")
            .with_description("Downstream dataset")
            .with_columns(["column_0", "column_1", "column_2"])
            .build()
        )

        # Register datasets
        builder.register_dataset("upstream", upstream)
        builder.register_dataset("downstream", downstream)

        # Add lineage
        lineage = (
            builder.add_lineage("upstream", "downstream")
            .add_field_lineage("upstream", "column_0", "downstream", "column_0")
            .build(upstream.urn)
        )

        builder.register_lineage("upstream", "downstream", lineage)

        # Note: Expectations should be added by test code using modern plugins
        return builder.build()

    @staticmethod
    def multi_hop_lineage(
        test_action_urn: str,
        term: str = "urn:li:glossaryTerm:TestTerm1_1",
        prefix: str = "multi_hop",
    ) -> "PropagationTestScenario":
        """Create a multi-hop lineage scenario."""
        from tests.propagation.framework.builders.scenario_builder import (
            PropagationScenarioBuilder,
        )

        builder = PropagationScenarioBuilder(test_action_urn, prefix)

        # Create 3 datasets
        dataset1 = (
            builder.add_dataset("dataset1")
            .with_description("First dataset")
            .with_columns(["column_0", "column_1", "column_2"])
            .with_column_glossary_term("column_0", term)
            .build()
        )

        dataset2 = (
            builder.add_dataset("dataset2")
            .with_description("Second dataset")
            .with_columns(["column_0", "column_1", "column_2"])
            .build()
        )

        dataset3 = (
            builder.add_dataset("dataset3")
            .with_description("Third dataset")
            .with_columns(["column_0", "column_1", "column_2"])
            .build()
        )

        # Register datasets
        builder.register_dataset("dataset1", dataset1)
        builder.register_dataset("dataset2", dataset2)
        builder.register_dataset("dataset3", dataset3)

        # Add lineages
        lineage1_2 = (
            builder.add_lineage("dataset1", "dataset2")
            .add_field_lineage("dataset1", "column_0", "dataset2", "column_0")
            .build(dataset1.urn)
        )

        lineage2_3 = (
            builder.add_lineage("dataset2", "dataset3")
            .add_field_lineage("dataset2", "column_0", "dataset3", "column_0")
            .build(dataset2.urn)
        )

        builder.register_lineage("dataset1", "dataset2", lineage1_2)
        builder.register_lineage("dataset2", "dataset3", lineage2_3)

        # Add live mutation to trigger second hop
        builder.add_term_mutation("dataset2", "column_0", term)

        # Note: Expectations should be added by test code using modern plugins
        return builder.build()

    @staticmethod
    def many_to_one_lineage(
        test_action_urn: str,
        terms: Optional[List[str]] = None,
        prefix: str = "many_to_one",
    ) -> "PropagationTestScenario":
        """Create an N:1 lineage scenario."""
        if terms is None:
            terms = [
                "urn:li:glossaryTerm:TestTerm1_1",
                "urn:li:glossaryTerm:TestTerm1_2",
            ]

        from tests.propagation.framework.builders.scenario_builder import (
            PropagationScenarioBuilder,
        )

        builder = PropagationScenarioBuilder(test_action_urn, prefix)

        # Create datasets
        upstream = (
            builder.add_dataset("upstream")
            .with_description("Upstream dataset with multiple terms")
            .with_columns(["column_0", "column_1", "column_2"])
            .with_column_glossary_term("column_0", terms[0])
            .with_column_glossary_term("column_1", terms[1])
            .build()
        )

        downstream = (
            builder.add_dataset("downstream")
            .with_description("Downstream dataset")
            .with_columns(["column_0", "column_1", "column_2"])
            .build()
        )

        # Register datasets
        builder.register_dataset("upstream", upstream)
        builder.register_dataset("downstream", downstream)

        # Add N:1 lineage
        lineage = (
            builder.add_lineage("upstream", "downstream")
            .add_many_to_one_lineage(
                "upstream", ["column_0", "column_1"], "downstream", "column_0"
            )
            .build(upstream.urn)
        )

        builder.register_lineage("upstream", "downstream", lineage)

        # Note: Expectations should be added by test code using modern plugins
        return builder.build()
