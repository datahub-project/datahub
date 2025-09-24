# DataHub Propagation Testing Framework - Tutorial

A step-by-step guide to writing your first propagation test using the DataHub testing framework.

## What You'll Learn

By the end of this tutorial, you'll understand how to:

- Create datasets with metadata using the framework
- Define lineage relationships between datasets
- Set up expectations for propagation testing
- Add live mutations to test dynamic propagation
- Run a complete 4-phase propagation test

## Prerequisites

- Basic understanding of DataHub concepts (datasets, lineage, metadata)
- Python knowledge
- Pytest experience (helpful but not required)

## Tutorial Overview

We'll build a test that verifies documentation propagation from a customer dataset to an analytics dataset. This mirrors a real-world scenario where business glossary definitions should flow through your data pipeline.

## Step 1: Understanding the Test Scenario

Our test will verify that when we add documentation to a source dataset field, it automatically propagates to downstream datasets through lineage relationships.

**Scenario:**

- Source: `customers` table with documented fields
- Target: `analytics` table (initially without documentation)
- Lineage: `customers.customer_id → analytics.id`
- Test: Documentation should propagate through this lineage

## Step 2: Setting Up the Test File

Create a new test file `test_my_first_propagation.py`:

```python
"""
My First Propagation Test - Tutorial Example

This test demonstrates the basic usage of the DataHub propagation testing framework
by testing documentation propagation from a customer source to an analytics target.
"""

import pytest
from datahub.emitter.mce_builder import make_schema_field_urn

from tests.propagation.framework.builders.scenario_builder import PropagationScenarioBuilder
from tests.propagation.framework.core.base import (
    PropagationTestFramework,
    DocumentationPropagationTest,
)
from tests.propagation.framework.plugins.documentation.expectations import (
    DocumentationPropagationExpectation,
    NoDocumentationPropagationExpectation,
)
from tests.propagation.framework.plugins.documentation.mutations import (
    FieldDocumentationUpdateMutation,
)
from tests.propagation.framework.utils.test_utilities import create_standard_fixtures

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
```

## Step 3: Creating Your First Test Function

Now let's create the main test function:

```python
def test_my_first_propagation(
    test_framework: PropagationTestFramework,
    test_action_urn: str,
    create_test_action,
):
    """My first propagation test - documentation propagation example."""

    # Step 3.1: Initialize the scenario builder
    builder = PropagationScenarioBuilder(test_action_urn, "my_first_test")

    # We'll add more code here in the following steps...
```

## Step 4: Creating Datasets

Add dataset creation to your test function:

```python
def test_my_first_propagation(
    test_framework: PropagationTestFramework,
    test_action_urn: str,
    create_test_action,
):
    """My first propagation test - documentation propagation example."""

    # Step 3.1: Initialize the scenario builder
    builder = PropagationScenarioBuilder(test_action_urn, "my_first_test")

    # Step 4.1: Create source dataset with documentation
    customers = (
        builder.add_dataset("customers", "snowflake")
        .with_columns(["customer_id", "name", "email", "phone"])
        .with_column_description("customer_id", "Unique identifier for each customer")
        .with_column_description("email", "Customer's primary email address")
        .with_dataset_description("Master customer data from our CRM system")
        .build()
    )

    # Step 4.2: Create target dataset (initially without documentation)
    analytics = (
        builder.add_dataset("customer_analytics", "snowflake")
        .with_columns(["id", "customer_name", "contact_email", "phone_number"])
        .build()
    )

    # Step 4.3: Register datasets for easy reference
    builder.register_dataset("customers", customers)
    builder.register_dataset("analytics", analytics)
```

**What's happening here?**

- We create two datasets using the fluent builder API
- The source dataset (`customers`) has documentation on some fields
- The target dataset (`analytics`) starts without documentation
- We register both datasets with friendly names for later reference

## Step 5: Defining Lineage Relationships

Add lineage creation to connect your datasets:

```python
    # Step 5.1: Create lineage between datasets
    lineage = (
        builder.add_lineage("customers", "analytics")
        .add_field_lineage("customers", "customer_id", "analytics", "id")
        .add_field_lineage("customers", "email", "analytics", "contact_email")
        .build(customers.urn)
    )

    # Step 5.2: Register the lineage
    builder.register_lineage("customers", "analytics", lineage)
```

**What's happening here?**

- We create field-level lineage showing data flow
- `customer_id → id` (1:1 mapping)
- `email → contact_email` (1:1 mapping)
- The framework will use this lineage to determine where documentation should propagate

## Step 6: Setting Up Expectations

Define what you expect to happen during the test:

```python
    # Step 6.1: Base expectations (tested after bootstrap)
    builder.base_expectations = [
        # This should propagate (1:1 lineage)
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(analytics.urn, "id"),
            expected_description="Unique identifier for each customer",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(customers.urn, "customer_id"),
            expected_depth=1,  # Validate 1-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),

        # This should also propagate (1:1 lineage)
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(analytics.urn, "contact_email"),
            expected_description="Customer's primary email address",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(customers.urn, "email"),
            expected_depth=1,  # Validate 1-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),

        # This should NOT propagate (no lineage)
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(analytics.urn, "customer_name"),
        ),
    ]
```

**What's happening here?**

- We define expectations for what should happen during the bootstrap phase
- Fields with 1:1 lineage should have their documentation propagated
- Fields without lineage should not receive any documentation
- We use URN-based parameters for type safety and consistency
- **New**: We validate attribution details like propagation depth, direction, and relationship type for comprehensive testing

## Step 7: Adding Live Mutations (Optional)

Add a live mutation to test dynamic propagation:

```python
    # Step 7.1: Create a mutation for live testing
    mutation = FieldDocumentationUpdateMutation(
        dataset_urn=customers.urn,
        field_name="customer_id",
        new_description="Updated unique customer identifier"
    )

    # Step 7.2: Apply the mutation using smart mutation handling
    scenario = builder.build()
    scenario.add_mutation_objects([mutation])

    # Step 7.3: Define post-mutation expectations
    builder.post_mutation_expectations = [
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(analytics.urn, "id"),
            expected_description="Updated unique customer identifier",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(customers.urn, "customer_id"),
            expected_depth=1,  # Validate propagation depth
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        )
    ]
```

**What's happening here?**

- We create a mutation that updates documentation on the source field
- The framework will apply this during the live phase and provide automatic explanations
- We expect the updated documentation to propagate to the target field
- **New**: Mutations now provide automatic explanations and enhanced logging during execution

## Step 8: Building and Running the Test

Complete your test by building the scenario and running it:

```python
    # Step 8.1: Build the final scenario (if not already built in step 7)
    if 'scenario' not in locals():
        scenario = builder.build()

    # Step 8.2: Run the complete 4-phase test
    result = test_framework.run_propagation_test(
        scenario=scenario,
        test_action_urn=test_action_urn,
        test_type=DocumentationPropagationTest(),
        scenario_name="My First Propagation Test"
    )

    # Step 8.3: Verify the test succeeded
    assert result.success, f"Test failed: {result.error_details}"

    print("🎉 My first propagation test completed successfully!")
    print(result.get_summary())
```

## Step 9: Complete Test File

Here's your complete test file:

```python
"""
My First Propagation Test - Tutorial Example

This test demonstrates the basic usage of the DataHub propagation testing framework
by testing documentation propagation from a customer source to an analytics target.
"""

import pytest
from datahub.emitter.mce_builder import make_schema_field_urn

from tests.propagation.framework.builders.scenario_builder import PropagationScenarioBuilder
from tests.propagation.framework.core.base import (
    PropagationTestFramework,
    DocumentationPropagationTest,
)
from tests.propagation.framework.plugins.documentation.expectations import (
    DocumentationPropagationExpectation,
    NoDocumentationPropagationExpectation,
)
from tests.propagation.framework.plugins.documentation.mutations import (
    FieldDocumentationUpdateMutation,
)
from tests.propagation.framework.utils.test_utilities import create_standard_fixtures

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


def test_my_first_propagation(
    test_framework: PropagationTestFramework,
    test_action_urn: str,
    create_test_action,
):
    """My first propagation test - documentation propagation example."""

    # Step 1: Initialize the scenario builder
    builder = PropagationScenarioBuilder(test_action_urn, "my_first_test")

    # Step 2: Create source dataset with documentation
    customers = (
        builder.add_dataset("customers", "snowflake")
        .with_columns(["customer_id", "name", "email", "phone"])
        .with_column_description("customer_id", "Unique identifier for each customer")
        .with_column_description("email", "Customer's primary email address")
        .with_dataset_description("Master customer data from our CRM system")
        .build()
    )

    # Step 3: Create target dataset (initially without documentation)
    analytics = (
        builder.add_dataset("customer_analytics", "snowflake")
        .with_columns(["id", "customer_name", "contact_email", "phone_number"])
        .build()
    )

    # Step 4: Register datasets for easy reference
    builder.register_dataset("customers", customers)
    builder.register_dataset("analytics", analytics)

    # Step 5: Create lineage between datasets
    lineage = (
        builder.add_lineage("customers", "analytics")
        .add_field_lineage("customers", "customer_id", "analytics", "id")
        .add_field_lineage("customers", "email", "analytics", "contact_email")
        .build(customers.urn)
    )
    builder.register_lineage("customers", "analytics", lineage)

    # Step 6: Base expectations (tested after bootstrap)
    builder.base_expectations = [
        # This should propagate (1:1 lineage)
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(analytics.urn, "id"),
            expected_description="Unique identifier for each customer",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(customers.urn, "customer_id"),
            expected_depth=1,  # Validate 1-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),

        # This should also propagate (1:1 lineage)
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(analytics.urn, "contact_email"),
            expected_description="Customer's primary email address",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(customers.urn, "email"),
            expected_depth=1,  # Validate 1-hop propagation
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        ),

        # This should NOT propagate (no lineage)
        NoDocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(analytics.urn, "customer_name"),
        ),
    ]

    # Step 7: Create a mutation for live testing
    mutation = FieldDocumentationUpdateMutation(
        dataset_urn=customers.urn,
        field_name="customer_id",
        new_description="Updated unique customer identifier"
    )

    # Step 8: Build scenario and apply mutation
    scenario = builder.build()
    scenario.add_mutation_objects([mutation])

    # Step 9: Define post-mutation expectations
    builder.post_mutation_expectations = [
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(analytics.urn, "id"),
            expected_description="Updated unique customer identifier",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(customers.urn, "customer_id"),
            expected_depth=1,  # Validate propagation depth
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        )
    ]

    # Step 10: Run the complete 4-phase test
    result = test_framework.run_propagation_test(
        scenario=scenario,
        test_action_urn=test_action_urn,
        test_type=DocumentationPropagationTest(),
        scenario_name="My First Propagation Test"
    )

    # Step 11: Verify the test succeeded
    assert result.success, f"Test failed: {result.error_details}"

    print("🎉 My first propagation test completed successfully!")
    print(result.get_summary())
```

## Step 10: Running Your Test

Run your test using pytest:

```bash
cd smoke-test
python -m pytest tests/propagation/test_my_first_propagation.py::test_my_first_propagation -v
```

## Understanding the Test Phases

Your test will automatically execute through 4 phases:

### Phase 1: Bootstrap 🏗️

- Applies your base graph data (datasets, lineage, metadata)
- Runs bootstrap action to establish initial propagation
- Validates that expected documentation propagated correctly with comprehensive attribution checking

### Phase 2: Rollback ↩️

- Executes rollback action to clean up propagated metadata
- Verifies that all propagated content was properly removed
- **Enhanced**: Uses intelligent filtering to only validate propagated content during rollback

### Phase 3: Live 🔴

- Starts live action to monitor real-time changes
- Applies your mutations (documentation updates) with automatic explanations
- Validates that new changes propagated correctly with detailed attribution validation

### Phase 4: Live Rollback ↩️🔴

- Executes final rollback to clean up all test data
- Verifies complete cleanup of both bootstrap and live propagations
- **Enhanced**: Provides comprehensive cleanup validation with detailed logging

## Common Patterns and Tips

### Creating Multiple Lineage Relationships

```python
# Complex lineage with many-to-one mappings
lineage = (
    builder.add_lineage("customers", "analytics")
    .add_field_lineage("customers", "customer_id", "analytics", "id")
    .add_many_to_one_lineage("customers", ["first_name", "last_name"], "analytics", "full_name")
    .build(customers.urn)
)
```

### Batch Creating Expectations

```python
# Create multiple similar expectations
base_expectations = []
for field_mapping in [("customer_id", "id"), ("email", "contact_email")]:
    source_field, target_field = field_mapping
    base_expectations.append(
        DocumentationPropagationExpectation(
            field_urn=make_schema_field_urn(analytics.urn, target_field),
            expected_description=f"Propagated from {source_field}",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(customers.urn, source_field),
            expected_depth=1,  # All are 1-hop propagations
            expected_direction="down",  # Downstream propagation
            expected_relationship="lineage",  # Through lineage relationships
        )
    )
builder.base_expectations = base_expectations
```

### Testing Different Propagation Types

The framework supports multiple propagation types:

```python
# Documentation propagation
from tests.propagation.framework.plugins.documentation.expectations import DocumentationPropagationExpectation

# Term propagation
from tests.propagation.framework.plugins.term.expectations import TermPropagationExpectation

# Tag propagation
from tests.propagation.framework.plugins.tag.expectations import TagPropagationExpectation
```

## Troubleshooting Common Issues

### Test Timeout

If your test times out, try increasing the timeout:

```python
from tests.propagation.framework.core.base import PropagationTestConfig

config = PropagationTestConfig(
    bootstrap_timeout=180,  # 3 minutes
    live_timeout=180
)
framework = PropagationTestFramework(auth_session, graph_client, test_resources_dir, config)
```

### URN Construction Errors

Make sure you're using the correct dataset URNs:

```python
# Correct - use the dataset object's URN
field_urn = make_schema_field_urn(customers.urn, "customer_id")

# Incorrect - don't construct URNs manually
field_urn = "urn:li:schemaField:..."  # This is error-prone
```

### Missing Propagation

If propagation isn't working:

1. Check that lineage relationships are correctly defined
2. Verify field names match exactly between source and target
3. Ensure the propagation action supports your metadata type
4. Check attribution validation parameters (depth, direction, relationship) match your scenario

## Next Steps

Now that you've mastered the basics, explore more advanced topics:

1. **Multi-hop propagation**: Testing propagation across multiple datasets
2. **Sibling relationships**: Testing propagation between related datasets
3. **Custom expectations**: Creating your own expectation types
4. **Performance testing**: Optimizing tests for large-scale scenarios

## Additional Examples

Check out these existing test files for more examples:

- `test_framework_term_propagation.py` - Term propagation examples
- `test_framework_tag_propagation.py` - Tag propagation examples
- `test_logical_datatype_propagation.py` - Complex multi-dataset scenarios

Congratulations! You've successfully created your first DataHub propagation test. The framework handles all the complex setup, execution, and cleanup, letting you focus on defining your test scenarios.
