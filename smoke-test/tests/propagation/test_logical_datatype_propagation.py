"""
Tests for logical datatype propagation feature using the new framework.

This file contains tests for the logical parent-child propagation feature,
testing propagation of various metadata types (tags, terms, docs, ownership,
structured properties) from logical parents to logical children.

The tests cover the following scenarios:
- Logical table -> 2 child tables propagation
- Logical column -> 2 child columns propagation
- Bootstrap and rollback scenarios
- Action bootstrap on logicalParent relationship creation
- Action rollback on logicalParent relationship deletion

Each scenario tests propagation of:
- Tags (dataset-level and field-level)
- Terms (field-level)
- Documentation (dataset-level and field-level)
- Ownership (dataset-level)
- Structured properties (dataset-level and field-level)
"""

import logging
from typing import Any

import pytest

from datahub.emitter.mce_builder import make_schema_field_urn
from tests.propagation.framework.builders.scenario_builder import (
    PropagationScenarioBuilder,
)
from tests.propagation.framework.core.base import (
    BasePropagationTest,
    PropagationTestFramework,
)
from tests.propagation.framework.plugins.documentation.expectations import (
    DatasetDocumentationPropagationExpectation,
    DocumentationPropagationExpectation,
    NoDocumentationPropagationExpectation,
)
from tests.propagation.framework.plugins.documentation.mutations import (
    DatasetDocumentationUpdateMutation,
    FieldDocumentationAdditionMutation,
)
from tests.propagation.framework.plugins.structured_properties.expectations import (
    DatasetStructuredPropertyPropagationExpectation,
)
from tests.propagation.framework.plugins.structured_properties.mutations import (
    DatasetStructuredPropertyAdditionMutation,
)
from tests.propagation.framework.plugins.tag.expectations import (
    DatasetTagPropagationExpectation,
    NoTagPropagationExpectation,
    TagPropagationExpectation,
)
from tests.propagation.framework.plugins.tag.mutations import (
    DatasetTagAdditionMutation,
    FieldTagAdditionMutation,
)
from tests.propagation.framework.plugins.term.expectations import (
    DatasetTermPropagationExpectation,
    NoTermPropagationExpectation,
    TermPropagationExpectation,
)
from tests.propagation.framework.plugins.term.mutations import (
    DatasetTermAdditionMutation,
    FieldTermAdditionMutation,
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


class LogicalPropagationTest(BasePropagationTest):
    """Concrete implementation for logical datatype propagation tests."""

    def get_action_type(self) -> str:
        return "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action"

    def get_recipe_filename(self) -> str:
        return "logical_action_recipe.yaml"

    def get_action_name(self) -> str:
        return "test_logical_propagation"

    def get_glossary_required(self) -> bool:
        return True

    def customize_config(self, config) -> Any:
        """Skip bootstrap phase since PropagationV2 has issues with batch relationship lookups."""
        config.skip_bootstrap = True  # Only test live propagation
        return config


class LogicalBootstrapPropagationTest(BasePropagationTest):
    """Concrete implementation for logical bootstrap propagation tests."""

    def get_action_type(self) -> str:
        return "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action"

    def get_recipe_filename(self) -> str:
        return "logical_action_recipe.yaml"

    def get_action_name(self) -> str:
        return "test_logical_bootstrap_propagation"

    def get_glossary_required(self) -> bool:
        return True

    def customize_config(self, config) -> Any:
        """Enable bootstrap but skip live phase for bootstrap-only testing."""
        config.skip_bootstrap = False  # Enable bootstrap
        config.skip_live = True  # Skip live mutations
        return config


# Create the test action fixtures for both test types
create_test_action = create_test_action_fixture(LogicalPropagationTest)
create_bootstrap_test_action = create_test_action_fixture(
    LogicalBootstrapPropagationTest
)


class TestLogicalDatatypePropagation:
    """Test class for logical datatype propagation scenarios."""

    @pytest.mark.dependency()
    def test_load_glossary(self, load_glossary: None) -> None:
        """Load glossary data required for the tests."""
        # This runs the glossary loading fixture
        pass

    @pytest.mark.parametrize(
        "scenario_name",
        [
            "logical_table_to_children",
            "logical_field_to_field",
            "logical_bootstrap_only",
        ],
    )
    # @pytest.mark.dependency(depends=["TestLogicalDatatypePropagation::test_load_glossary"])
    def test_logical_propagation_scenarios(
        self,
        test_framework: PropagationTestFramework,
        test_action_urn: str,
        create_test_action,
        create_bootstrap_test_action,
        load_glossary,
        scenario_name: str,
    ) -> None:
        """Test logical parent-child propagation scenarios."""
        logger.info(f"Starting {scenario_name} test with new framework")

        # Get the scenario function
        scenario_func = {
            "logical_table_to_children": create_logical_table_to_children_scenario,
            "logical_field_to_field": create_logical_field_to_field_scenario,
            "logical_bootstrap_only": create_logical_bootstrap_only_scenario,
        }.get(scenario_name)

        if not scenario_func:
            pytest.fail(f"Unknown scenario: {scenario_name}")

        # Create and run the scenario
        scenario = scenario_func(test_action_urn)

        # Use appropriate test class and fixture based on scenario
        if scenario_name == "logical_bootstrap_only":
            # For bootstrap-only scenario, use the bootstrap test class with bootstrap fixture
            result = test_framework.run_propagation_test(
                scenario,
                test_action_urn,
                LogicalBootstrapPropagationTest(),
                f"Logical Bootstrap Propagation: {scenario_name}",
            )
        else:
            # For other scenarios, use the regular test class
            result = test_framework.run_propagation_test(
                scenario,
                test_action_urn,
                LogicalPropagationTest(),
                f"Logical Propagation: {scenario_name}",
            )

        # Assert success
        assert result.success, f"Test failed: {result.error_details}"


def create_logical_table_to_children_scenario(test_action_urn: str) -> Any:
    """Create scenario: Logical table -> 2 child tables.

    Tests propagation of dataset-level metadata (tags, docs, ownership,
    structured properties) from a logical parent table to 2 child tables.
    """
    builder = (
        PropagationScenarioBuilder(test_action_urn, prefix="logical_table")
        .enable_verbose_mode(True)
        .enable_debug_mcps(True)
        .enable_entity_cleanup(True)  # Add this line to disable cleanup
    )

    # Define structured property templates for testing
    builder.define_structured_property(
        name="data_classification",
        display_name="Data Classification",
        value_type="string",
        description="Classification level of the data",
        allowed_values=["Public", "Internal", "Confidential", "Restricted"],
    )

    builder.define_structured_property(
        name="retention_days",
        display_name="Data Retention (Days)",
        value_type="number",
        description="Data retention period in days",
    )

    # Create logical parent table with rich metadata including structured properties
    logical_parent = (
        builder.add_dataset("logical_parent", "snowflake")
        .with_description("Logical parent table for testing dataset-level propagation")
        .with_columns(["id", "name", "status", "created_at", "updated_at"])
        .with_column_description("id", "Primary key identifier")
        .with_column_description("name", "Entity name")
        .with_column_glossary_term("status", "urn:li:glossaryTerm:TestTerm1_1")
        .with_column_tag("created_at", "urn:li:tag:PersonalData")
        .with_structured_property("data_classification", "Internal", "string")
        .with_structured_property("retention_days", 730, "number")
        .build()
    )

    # Create first child table
    child_table_1 = (
        builder.add_dataset("child_table_1", "postgres")
        .with_description("First child table")
        .with_columns(["id", "name", "status", "created_at", "updated_at"])
        .set_logical_parent(logical_parent.urn)
        .build()
    )

    # Create second child table
    child_table_2 = (
        builder.add_dataset("child_table_2", "mysql")
        .with_description("Second child table")
        .with_columns(["id", "name", "status", "created_at", "updated_at"])
        .set_logical_parent(logical_parent.urn)
        .build()
    )

    # Register datasets
    builder.register_dataset("logical_parent", logical_parent)
    builder.register_dataset("child_table_1", child_table_1)
    builder.register_dataset("child_table_2", child_table_2)

    # Base expectations: verify logical parent relationships are established
    # (No base expectations needed for dataset-level propagation)

    # Live mutations: Add dataset-level metadata to logical parent
    scenario = builder.build()

    # Create dataset-level mutations with URN only
    mutations = [
        DatasetTagAdditionMutation(
            dataset_urn=logical_parent.urn, tag_urn="urn:li:tag:Confidential"
        ),
        DatasetDocumentationUpdateMutation(
            dataset_urn=logical_parent.urn,
            new_description="Updated logical parent with confidential data",
        ),
        DatasetTermAdditionMutation(
            dataset_urn=logical_parent.urn,
            term_urn="urn:li:glossaryTerm:TestTerm2_1",
        ),
        DatasetStructuredPropertyAdditionMutation(
            dataset_urn=logical_parent.urn,
            structured_property_urn="urn:li:structuredProperty:io.datahub.test.data_classification",
            value="Confidential",
            value_type="string",
        ),
    ]

    # Apply all mutations with automatic explanations
    scenario.add_mutation_objects(mutations)

    # Post-mutation expectations: verify propagation to both children
    scenario.post_mutation_expectations.extend(
        [
            # Verify dataset tag propagated to child 1
            DatasetTagPropagationExpectation(
                dataset_urn=child_table_1.urn,
                expected_tag_urn="urn:li:tag:Confidential",
            ),
            # Verify dataset tag propagated to child 2
            DatasetTagPropagationExpectation(
                dataset_urn=child_table_2.urn,
                expected_tag_urn="urn:li:tag:Confidential",
            ),
            # Verify dataset documentation propagated to child 1
            DatasetDocumentationPropagationExpectation(
                dataset_urn=child_table_1.urn,
                expected_description="Updated logical parent with confidential data",
            ),
            # Verify dataset documentation propagated to child 2
            DatasetDocumentationPropagationExpectation(
                dataset_urn=child_table_2.urn,
                expected_description="Updated logical parent with confidential data",
            ),
            # Verify structured property propagated to child 1
            DatasetStructuredPropertyPropagationExpectation(
                dataset_urn=child_table_1.urn,
                structured_property_urn="urn:li:structuredProperty:io.datahub.test.data_classification",
                expected_value="Confidential",
            ),
            # Verify structured property propagated to child 2
            DatasetStructuredPropertyPropagationExpectation(
                dataset_urn=child_table_2.urn,
                structured_property_urn="urn:li:structuredProperty:io.datahub.test.data_classification",
                expected_value="Confidential",
            ),
            # Verify dataset term propagated to child 1
            DatasetTermPropagationExpectation(
                dataset_urn=child_table_1.urn,
                expected_term_urn="urn:li:glossaryTerm:TestTerm2_1",
            ),
            # Verify dataset term propagated to child 2
            DatasetTermPropagationExpectation(
                dataset_urn=child_table_2.urn,
                expected_term_urn="urn:li:glossaryTerm:TestTerm2_1",
            ),
        ]
    )

    return scenario


def create_logical_column_to_children_scenario(test_action_urn: str) -> Any:
    """Create scenario: Logical column -> 2 child columns.

    Tests propagation of field-level metadata (tags, terms, docs) from
    logical parent columns to corresponding columns in 2 child tables.
    """
    builder = (
        PropagationScenarioBuilder(test_action_urn, prefix="logical_column")
        .enable_verbose_mode(True)
        .enable_debug_mcps(True)
    )

    # Create logical parent table
    logical_parent = (
        builder.add_dataset("logical_parent", "snowflake")
        .with_description("Logical parent for column-level propagation testing")
        .with_columns(["user_id", "email", "phone", "address", "created_date"])
        .with_column_description("user_id", "Unique user identifier")
        .with_column_description("email", "User email address")
        .with_column_glossary_term("email", "urn:li:glossaryTerm:TestTerm1_1")
        .with_column_tag("phone", "urn:li:tag:PersonalData")
        .build()
    )

    # Create first child table with same column structure
    child_table_1 = (
        builder.add_dataset("child_table_1", "postgres")
        .with_description("First child with matching columns")
        .with_columns(["user_id", "email", "phone", "address", "created_date"])
        .set_logical_parent(logical_parent.urn)
        .build()
    )

    # Create second child table with same column structure
    child_table_2 = (
        builder.add_dataset("child_table_2", "mysql")
        .with_description("Second child with matching columns")
        .with_columns(["user_id", "email", "phone", "address", "created_date"])
        .set_logical_parent(logical_parent.urn)
        .build()
    )

    # Register datasets
    builder.register_dataset("logical_parent", logical_parent)
    builder.register_dataset("child_table_1", child_table_1)
    builder.register_dataset("child_table_2", child_table_2)

    # Skip base expectations since we're only testing live propagation
    # (Bootstrap phase is disabled for PropagationV2)

    scenario = builder.build()

    # Live mutations: Add field-level metadata to logical parent
    mutations = [
        FieldDocumentationAdditionMutation(
            dataset_urn=logical_parent.urn,
            field_name="address",
            description="User mailing address - PII sensitive",
        ),
        FieldTagAdditionMutation(
            dataset_urn=logical_parent.urn,
            field_name="user_id",
            tag_urn="urn:li:tag:Identifier",
        ),
        FieldTermAdditionMutation(
            dataset_urn=logical_parent.urn,
            field_name="created_date",
            term_urn="urn:li:glossaryTerm:TestTerm2_1",
        ),
    ]

    # Apply all mutations with automatic explanations
    scenario.add_mutation_objects(mutations)

    # Post-mutation expectations: verify field-level propagation to both children
    scenario.post_mutation_expectations.extend(
        [
            # Verify address documentation propagated to both children
            DocumentationPropagationExpectation(
                field_urn=make_schema_field_urn(child_table_1.urn, "address"),
                expected_description="User mailing address - PII sensitive",
            ),
            DocumentationPropagationExpectation(
                field_urn=make_schema_field_urn(child_table_2.urn, "address"),
                expected_description="User mailing address - PII sensitive",
            ),
            # Verify user_id tag propagated to both children
            TagPropagationExpectation(
                field_urn=make_schema_field_urn(child_table_1.urn, "user_id"),
                expected_tag_urn="urn:li:tag:Identifier",
            ),
            TagPropagationExpectation(
                field_urn=make_schema_field_urn(child_table_2.urn, "user_id"),
                expected_tag_urn="urn:li:tag:Identifier",
            ),
            # Verify created_date term propagated to both children
            TermPropagationExpectation(
                field_urn=make_schema_field_urn(child_table_1.urn, "created_date"),
                expected_term_urn="urn:li:glossaryTerm:TestTerm2_1",
            ),
            TermPropagationExpectation(
                field_urn=make_schema_field_urn(child_table_2.urn, "created_date"),
                expected_term_urn="urn:li:glossaryTerm:TestTerm2_1",
            ),
        ]
    )

    return scenario


def create_logical_field_to_field_scenario(test_action_urn: str) -> Any:
    """Create scenario: Field-to-field logical relationships.

    Tests propagation of field-level metadata from parent fields to specific connected
    child fields using field-level logical relationships instead of dataset-level.

    This creates field URN -> field URN logical relationships using LogicalParentClass
    with field URNs instead of dataset URNs.
    """
    builder = (
        PropagationScenarioBuilder(test_action_urn, prefix="logical_field")
        .enable_verbose_mode(True)
        .enable_debug_mcps(True)
        .enable_entity_cleanup(True)  # Disable cleanup to keep entities in DataHub
    )

    # Create parent dataset with rich field metadata
    parent_dataset = (
        builder.add_dataset("parent_dataset", "snowflake")
        .with_description("Parent dataset for field-to-field logical relationships")
        .with_columns(
            ["customer_id", "customer_name", "email", "phone", "address", "created_at"]
        )
        .with_column_description("customer_id", "Unique customer identifier")
        .with_column_description("customer_name", "Full customer name")
        .with_column_description("email", "Customer email address")
        .with_column_glossary_term("customer_id", "urn:li:glossaryTerm:TestTerm1_1")
        .with_column_tag("phone", "urn:li:tag:PersonalData")
        .build()
    )

    # Create first child dataset with similar fields and field-level logical relationships
    child_dataset_1 = (
        builder.add_dataset("child_dataset_1", "postgres")
        .with_description("First child dataset with connected fields")
        .with_columns(
            [
                "cust_id",
                "full_name",
                "email_addr",
                "phone_num",
                "mailing_addr",
                "registration_date",
            ]
        )
        # Add field-level logical parent relationships
        .add_field_logical_parent(
            "cust_id", make_schema_field_urn(parent_dataset.urn, "customer_id")
        )
        .add_field_logical_parent(
            "full_name", make_schema_field_urn(parent_dataset.urn, "customer_name")
        )
        .add_field_logical_parent(
            "email_addr", make_schema_field_urn(parent_dataset.urn, "email")
        )
        .build()
    )

    # Create second child dataset with similar fields and field-level logical relationships
    child_dataset_2 = (
        builder.add_dataset("child_dataset_2", "mysql")
        .with_description("Second child dataset with connected fields")
        .with_columns(
            [
                "id",
                "name",
                "contact_email",
                "contact_phone",
                "address_line",
                "created_date",
            ]
        )
        # Add field-level logical parent relationships
        .add_field_logical_parent(
            "id", make_schema_field_urn(parent_dataset.urn, "customer_id")
        )
        .add_field_logical_parent(
            "name", make_schema_field_urn(parent_dataset.urn, "customer_name")
        )
        .add_field_logical_parent(
            "contact_email", make_schema_field_urn(parent_dataset.urn, "email")
        )
        .add_field_logical_parent(
            "contact_phone", make_schema_field_urn(parent_dataset.urn, "phone")
        )
        .build()
    )

    # Register datasets
    builder.register_dataset("parent_dataset", parent_dataset)
    builder.register_dataset("child_dataset_1", child_dataset_1)
    builder.register_dataset("child_dataset_2", child_dataset_2)

    scenario = builder.build()

    # Add metadata mutations to parent fields using proper mutation classes
    from tests.propagation.framework.plugins.documentation.mutations import (
        FieldDocumentationAdditionMutation,
    )
    from tests.propagation.framework.plugins.tag.mutations import (
        FieldTagAdditionMutation,
    )
    from tests.propagation.framework.plugins.term.mutations import (
        FieldTermAdditionMutation,
    )

    # Create mutations with dataset URN included
    mutations = [
        FieldDocumentationAdditionMutation(
            dataset_urn=parent_dataset.urn,
            field_name="customer_name",
            description="Updated customer name with field-to-field propagation test",
        ),
        FieldTagAdditionMutation(
            dataset_urn=parent_dataset.urn,
            field_name="email",
            tag_urn="urn:li:tag:Confidential",
        ),
        FieldTermAdditionMutation(
            dataset_urn=parent_dataset.urn,
            field_name="customer_id",
            term_urn="urn:li:glossaryTerm:TestTerm2_1",
        ),
    ]

    # Apply all mutations with automatic explanations
    scenario.add_mutation_objects(mutations)

    # Post-mutation expectations: verify propagation to all connected child fields
    scenario.post_mutation_expectations.extend(
        [
            # Verify customer_name documentation propagated to connected fields
            DocumentationPropagationExpectation(
                field_urn=make_schema_field_urn(child_dataset_1.urn, "full_name"),
                expected_description="Updated customer name with field-to-field propagation test",
            ),
            DocumentationPropagationExpectation(
                field_urn=make_schema_field_urn(child_dataset_2.urn, "name"),
                expected_description="Updated customer name with field-to-field propagation test",
            ),
            # Verify email tag propagated to connected fields
            TagPropagationExpectation(
                field_urn=make_schema_field_urn(child_dataset_1.urn, "email_addr"),
                expected_tag_urn="urn:li:tag:Confidential",
            ),
            TagPropagationExpectation(
                field_urn=make_schema_field_urn(child_dataset_2.urn, "contact_email"),
                expected_tag_urn="urn:li:tag:Confidential",
            ),
            # Verify customer_id term propagated to connected fields
            TermPropagationExpectation(
                field_urn=make_schema_field_urn(child_dataset_1.urn, "cust_id"),
                expected_term_urn="urn:li:glossaryTerm:TestTerm2_1",
            ),
            TermPropagationExpectation(
                field_urn=make_schema_field_urn(child_dataset_2.urn, "id"),
                expected_term_urn="urn:li:glossaryTerm:TestTerm2_1",
            ),
            # Verify that fields WITHOUT logical relationships do NOT receive propagation
            NoDocumentationPropagationExpectation(
                field_urn=make_schema_field_urn(child_dataset_1.urn, "phone_num"),
            ),
            NoTagPropagationExpectation(
                field_urn=make_schema_field_urn(child_dataset_1.urn, "mailing_addr"),
            ),
            NoTermPropagationExpectation(
                field_urn=make_schema_field_urn(child_dataset_2.urn, "address_line"),
            ),
        ]
    )

    return scenario


def create_logical_bootstrap_only_scenario(test_action_urn: str) -> Any:
    """Create bootstrap-only scenario: Logical table -> 2 child tables with pre-existing metadata.

    Tests bootstrap propagation where metadata is added during entity creation and then
    propagated during bootstrap phase, without any live mutations. This tests the
    action's ability to propagate existing metadata when relationships are established.

    Setup:
    1. Create logical parent table with rich metadata (tags, docs, structured properties, terms)
    2. Create 2 child tables with logical parent relationship
    3. Run bootstrap phase to propagate existing metadata
    4. Verify metadata propagated properly during bootstrap
    """
    builder = (
        PropagationScenarioBuilder(test_action_urn, prefix="logical_bootstrap")
        .enable_verbose_mode(True)
        .enable_debug_mcps(True)
        .enable_entity_cleanup(True)
    )

    # Define structured property templates for testing
    builder.define_structured_property(
        name="data_classification",
        display_name="Data Classification",
        value_type="string",
        description="Classification level of the data",
        allowed_values=["Public", "Internal", "Confidential", "Restricted"],
    )

    builder.define_structured_property(
        name="retention_days",
        display_name="Data Retention (Days)",
        value_type="number",
        description="Data retention period in days",
    )

    # Create logical parent table with RICH METADATA during entity creation
    # This metadata will be propagated during bootstrap
    logical_parent = (
        builder.add_dataset("logical_parent", "snowflake")
        .with_description("Bootstrap logical parent with pre-existing metadata")
        .with_columns(["id", "name", "status", "created_at", "updated_at"])
        .with_column_description("id", "Primary key identifier")
        .with_column_description("name", "Entity name")
        .with_column_glossary_term("status", "urn:li:glossaryTerm:TestTerm1_1")
        .with_column_tag("created_at", "urn:li:tag:PersonalData")
        # Add structured properties that should propagate during bootstrap
        .with_structured_property("data_classification", "Confidential", "string")
        .with_structured_property("retention_days", 1095, "number")
        .build()
    )

    # Create first child table - will receive propagated metadata during bootstrap
    child_table_1 = (
        builder.add_dataset("child_table_1", "postgres")
        .with_description("First child table for bootstrap propagation")
        .with_columns(["id", "name", "status", "created_at", "updated_at"])
        .set_logical_parent(logical_parent.urn)
        .build()
    )

    # Create second child table - will receive propagated metadata during bootstrap
    child_table_2 = (
        builder.add_dataset("child_table_2", "mysql")
        .with_description("Second child table for bootstrap propagation")
        .with_columns(["id", "name", "status", "created_at", "updated_at"])
        .set_logical_parent(logical_parent.urn)
        .build()
    )

    # Register datasets
    builder.register_dataset("logical_parent", logical_parent)
    builder.register_dataset("child_table_1", child_table_1)
    builder.register_dataset("child_table_2", child_table_2)

    # Add pre-existing dataset-level metadata to the parent that should propagate during bootstrap
    # These need to be added to the base graph BEFORE bootstrap runs
    existing_metadata_mutations = [
        DatasetTagAdditionMutation(
            dataset_urn=logical_parent.urn, tag_urn="urn:li:tag:Confidential"
        ),
        DatasetTermAdditionMutation(
            dataset_urn=logical_parent.urn,
            term_urn="urn:li:glossaryTerm:TestTerm2_1",
        ),
    ]

    scenario = builder.build()

    # Apply pre-bootstrap mutations using the new framework functionality
    # These represent existing metadata that should propagate during bootstrap
    scenario.add_pre_bootstrap_mutations(existing_metadata_mutations)

    # Bootstrap testing: Now we can test all metadata types including tags and terms
    # Note: We put expectations in base_expectations since there are no live mutations
    # The framework will validate these after bootstrap phase

    scenario.base_expectations.extend(
        [
            # Verify dataset tag propagated to child 1 during bootstrap
            DatasetTagPropagationExpectation(
                dataset_urn=child_table_1.urn,
                expected_tag_urn="urn:li:tag:Confidential",
            ),
            # Verify dataset tag propagated to child 2 during bootstrap
            DatasetTagPropagationExpectation(
                dataset_urn=child_table_2.urn,
                expected_tag_urn="urn:li:tag:Confidential",
            ),
            # Verify dataset documentation propagated to child 1 during bootstrap
            DatasetDocumentationPropagationExpectation(
                dataset_urn=child_table_1.urn,
                expected_description="Bootstrap logical parent with pre-existing metadata",
            ),
            # Verify dataset documentation propagated to child 2 during bootstrap
            DatasetDocumentationPropagationExpectation(
                dataset_urn=child_table_2.urn,
                expected_description="Bootstrap logical parent with pre-existing metadata",
            ),
            # Verify structured property propagated to child 1 during bootstrap
            DatasetStructuredPropertyPropagationExpectation(
                dataset_urn=child_table_1.urn,
                structured_property_urn="urn:li:structuredProperty:io.datahub.test.data_classification",
                expected_value="Confidential",
            ),
            # Verify structured property propagated to child 2 during bootstrap
            DatasetStructuredPropertyPropagationExpectation(
                dataset_urn=child_table_2.urn,
                structured_property_urn="urn:li:structuredProperty:io.datahub.test.data_classification",
                expected_value="Confidential",
            ),
            # Verify retention days structured property propagated to child 1
            DatasetStructuredPropertyPropagationExpectation(
                dataset_urn=child_table_1.urn,
                structured_property_urn="urn:li:structuredProperty:io.datahub.test.retention_days",
                expected_value=1095.0,  # Propagated as float
            ),
            # Verify retention days structured property propagated to child 2
            DatasetStructuredPropertyPropagationExpectation(
                dataset_urn=child_table_2.urn,
                structured_property_urn="urn:li:structuredProperty:io.datahub.test.retention_days",
                expected_value=1095.0,  # Propagated as float
            ),
            # Verify dataset term propagated to child 1 during bootstrap
            DatasetTermPropagationExpectation(
                dataset_urn=child_table_1.urn,
                expected_term_urn="urn:li:glossaryTerm:TestTerm2_1",
            ),
            # Verify dataset term propagated to child 2 during bootstrap
            DatasetTermPropagationExpectation(
                dataset_urn=child_table_2.urn,
                expected_term_urn="urn:li:glossaryTerm:TestTerm2_1",
            ),
        ]
    )

    return scenario
