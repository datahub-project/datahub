# DataHub Propagation Test Framework

A framework for creating DataHub propagation tests with comprehensive phase management and automated test lifecycle handling.

## 🚀 Key Framework Features

### **Comprehensive Test Lifecycle Management**

- **4-Phase Test Execution**: Automatic Bootstrap → Rollback → Live → Live Rollback
- **Smart Expectation Validation**: Same expectations work for both positive (bootstrap/live) and negative (rollback) testing
- **Configurable Phase Control**: Skip bootstrap, rollback, or live phases based on propagation type support
- **Rich Result Tracking**: Detailed timing, success metrics, and error reporting per phase

### **Developer Experience**

- **Fluent Builder APIs** for intuitive test creation
- **Type-safe** propagation test definitions with compile-time validation
- **Automatic lifecycle management** with built-in cleanup and error recovery

### **Extensible Plugin Architecture**

- **Feature-based plugins** for terms, tags, documentation, structured properties, and custom propagation types
- **Modular design** allowing independent component usage
- **Easy extension** for new propagation types without framework changes

### **Type Safety & Consistency**

- **Full mypy compliance** with strict type checking for all framework components
- **Simplified expectation parameters** - All expectations use clean `expected_*` field names (`expected_tag_urn`, `expected_term_urn`)
- **Complete URN migration** - All expectations and tests use URN-based parameters (`dataset_urn`, `field_urn`) for consistency with mutations
- **Smart mutation handling** with automatic explanations, MCP generation, and dataset name extraction from URNs
- **Reliable test isolation** with unique URN generation preventing state leakage between tests

## 🏗️ **Current Architecture**

The framework is organized into core components and feature-specific plugins:

```
framework/
├── core/                           # Core framework components
│   ├── base.py                     # PropagationTestFramework with 4-phase execution
│   ├── models.py                   # PropagationTestScenario and expectation base classes
│   ├── expectations.py             # ExpectationBase and validation system with type safety
│   ├── mutations.py                # Mutation base classes
│   └── action_manager.py           # Action lifecycle management (bootstrap/rollback/live)
├── builders/                       # Scenario building utilities
│   └── scenario_builder.py         # PropagationScenarioBuilder with fluent API
├── plugins/                        # Feature-specific implementations
│   ├── documentation/              # Documentation propagation
│   │   ├── expectations.py         # DocumentationPropagationExpectation
│   │   └── mutations.py            # DocumentationUpdateMutation, etc.
│   ├── term/                       # Glossary term propagation
│   │   ├── expectations.py         # TermPropagationExpectation
│   │   └── mutations.py            # TermMutationPlugin
│   ├── tag/                        # Tag propagation
│   │   ├── expectations.py         # TagPropagationExpectation
│   │   └── mutations.py            # TagMutationPlugin
│   └── structured_properties/      # Structured property propagation
│       ├── expectations.py         # DatasetStructuredPropertyPropagationExpectation
│       ├── mutations.py            # StructuredProperty mutations
│       ├── models.py               # StructuredProperty data models
│       └── templates.py            # Property template creation utilities
└── utils/                          # Utilities and helpers
    ├── graph_utils.py              # Graph generation utilities
    └── test_utilities.py           # Helper functions
```

## 🔄 **4-Phase Test Execution Model**

The framework automatically executes a comprehensive 4-phase test lifecycle:

### **Phase 1: Bootstrap (🏗️)**

```python
# 1. Apply base graph data (datasets, lineage, metadata)
for mcp in scenario.base_graph:
    graph_client.emit(mcp, emit_mode=EmitMode.SYNC_PRIMARY)

# 2. Wait for writes to sync
wait_for_writes_to_sync()

# 3. Run bootstrap action
action_manager.run_bootstrap(test_action_urn)

# 4. Validate base expectations (rollback=False)
validate_expectations(
    scenario.base_expectations,
    graph_client,
    rollback=False  # ← Checks FOR propagation
)
```

### **Phase 2: Rollback (↩️)**

```python
# 1. Execute rollback action
action_manager.run_rollback(test_action_urn)

# 2. Validate cleanup (rollback=True)
validate_expectations(
    scenario.base_expectations,
    graph_client,
    rollback=True  # ← Checks propagation is REMOVED
)
```

### **Phase 3: Live (🔴)**

```python
# 1. Start live action
action_manager.start_live_action(test_action_urn)

# 2. Apply mutations
for mcp in scenario.mutations:
    graph_client.emit(mcp, emit_mode=EmitMode.SYNC_PRIMARY)

# 3. Wait for action to process events
wait_until_action_has_processed_event(test_action_urn, audit_timestamp)

# 4. Validate post-mutation expectations (rollback=False)
validate_expectations(
    scenario.post_mutation_expectations,
    graph_client,
    rollback=False  # ← Checks FOR new propagation
)
```

### **Phase 4: Live Rollback (↩️🔴)**

```python
# 1. Execute rollback action
action_manager.run_rollback(test_action_urn)

# 2. Validate complete cleanup (rollback=True)
validate_expectations(
    scenario.base_expectations + scenario.post_mutation_expectations,
    graph_client,
    rollback=True  # ← Checks ALL propagation is REMOVED
)
```

## 🎯 **Smart Expectation Validation**

The same expectation objects work for both **positive** (bootstrap/live) and **negative** (rollback) testing:

### **Bootstrap/Live Mode (`rollback=False`)**

```python
def check_expectation(self, graph_client, action_urn=None, rollback=False):
    documentation_aspect = get_documentation_aspect(graph_client, schema_field_urn)

    if self.propagation_found and not rollback:
        # ✅ Verify propagation EXISTS
        if not documentation_aspect or not documentation_aspect.documentations:
            raise AssertionError("Expected documentation propagation but none found")

        # ✅ Verify content matches expectation
        first_element = documentation_aspect.documentations[0]
        if first_element.documentation != self.expected_description:
            raise AssertionError(f"Documentation mismatch")

        # ✅ Verify comprehensive attribution (source, origin, via, depth, direction, relationship)
        if action_urn and first_element.attribution:
            self.check_attribution(
                action_urn,
                first_element.attribution,
                expected_source=self.propagation_source,
                expected_origin=self.propagation_origin,
                expected_via=self.propagation_via,
                expected_depth=self.expected_depth,
                expected_direction=self.expected_direction,
                expected_relationship=self.expected_relationship,
            )
```

### **Rollback Mode (`rollback=True`)**

```python
def check_expectation(self, graph_client, action_urn=None, rollback=False):
    documentation_aspect = get_documentation_aspect(graph_client, schema_field_urn)

    if rollback:
        # ✅ Verify propagation is REMOVED
        if documentation_aspect and documentation_aspect.documentations:
            raise AssertionError("Documentation should have been rolled back")
```

### **Intelligent Rollback Validation**

The framework automatically distinguishes between **propagated content** (which should be removed during rollback) and **directly added content** (which should remain) using the `is_propagated` flag:

```python
# Expectation for propagated content (default behavior)
DocumentationPropagationExpectation(
    field_urn=make_schema_field_urn(target_dataset.urn, "customer_id"),
    expected_description="Propagated description",
    is_propagated=True,  # Default: will be validated during rollback
)

# Expectation for directly added content
NoDocumentationPropagationExpectation(
    field_urn=make_schema_field_urn(source_dataset.urn, "id"),
    is_propagated=False,  # Skip during rollback validation
)
```

During rollback, the framework automatically filters expectations:

```python
# Filter expectations for rollback mode - only check propagated content
filtered_expectations = expectations
if rollback:
    filtered_expectations = [exp for exp in expectations if exp.is_propagated]
    if len(filtered_expectations) < len(expectations):
        skipped_count = len(expectations) - len(filtered_expectations)
        logger.info(f"Rollback mode: Skipping {skipped_count} non-propagated expectations")
```

## ⚙️ **Configuration & Phase Control**

### **PropagationTestConfig**

Configure test execution behavior, including **disabling phases** for propagation types that don't support certain operations:

```python
class PropagationTestConfig:
    def __init__(
        self,
        skip_bootstrap: bool = False,      # Skip bootstrap phase entirely
        skip_live: bool = False,           # Skip live mutations phase
        skip_rollback: bool = False,       # Skip all rollback phases
        bootstrap_timeout: int = 120,      # Bootstrap timeout in seconds
        live_timeout: int = 120,          # Live phase timeout in seconds
        verbose_logging: bool = True,      # Enable detailed logging
        fail_fast: bool = True,           # Stop on first phase failure
    ):
```

### **Disabling Bootstrap/Rollback for Unsupported Propagation Types**

Some propagation types may not support bootstrap or rollback operations. Configure accordingly:

#### **Example: Propagation Type Without Bootstrap Support**

```python
class StreamingOnlyPropagationTest(BasePropagationTest):
    def get_action_type(self) -> str:
        return "streaming.propagation.Action"

    def customize_config(self, config: PropagationTestConfig) -> PropagationTestConfig:
        # This propagation type only works in live mode
        config.skip_bootstrap = True  # ← Disable bootstrap + first rollback
        return config

# Usage
config = PropagationTestConfig()
framework = PropagationTestFramework(auth_session, graph_client, test_resources_dir, config)

result = framework.run_propagation_test(
    scenario,
    test_action_urn,
    StreamingOnlyPropagationTest(),  # ← Will automatically skip bootstrap
    "Streaming Only Test"
)
```

#### **Example: Live-Only Testing**

```python
# Skip bootstrap entirely, only test live mutations
config = PropagationTestConfig(
    skip_bootstrap=True,  # No bootstrap or initial rollback
    skip_live=False       # Keep live mutation testing
)

# Execution flow: Live → Live Rollback only
```

#### **Example: Bootstrap-Only Testing**

```python
# Test only initial propagation, no live mutations
config = PropagationTestConfig(
    skip_bootstrap=False,  # Keep bootstrap testing
    skip_live=True        # Skip live mutations
)

# Execution flow: Bootstrap → Rollback only
```

#### **Example: Skip All Rollback Testing**

```python
# Run propagation tests but skip all rollback validation
# Useful for propagation types that don't support cleanup
config = PropagationTestConfig(
    skip_rollback=True  # Skip all rollback phases (both bootstrap rollback and live rollback)
)

# Execution flow: Bootstrap → Live only (no rollback validation)
```

#### **Example: Rollback-Only Testing**

```python
# Skip propagation, only test rollback cleanup
config = PropagationTestConfig(
    skip_bootstrap=True,  # Skip initial propagation
    skip_live=True,      # Skip live mutations
    skip_rollback=False   # Keep rollback testing (of existing data)
)

# Execution flow: Manual rollback testing only
```

#### **Example: Environment-Specific Resilient Configuration**

```python
# For environments where bootstrap is unreliable
config = PropagationTestConfig(
    skip_bootstrap=False,    # Try bootstrap but don't fail the test
    bootstrap_timeout=60,    # Shorter timeout
    fail_fast=False         # Continue even if bootstrap fails
)
```

### **Scenario-Level Phase Control**

Control phases at the scenario level:

```python
scenario = PropagationTestScenario(
    base_graph=base_mcps,
    base_expectations=bootstrap_expectations,
    mutations=live_mutations,
    post_mutation_expectations=live_expectations,
    run_bootstrap=False,  # ← Disable bootstrap for this scenario
    skip_bootstrap_on_timeout=True,  # Skip bootstrap if stats endpoint fails
)
```

### **Phase Configuration Matrix**

Here's a comprehensive matrix showing all possible phase combinations:

| Configuration                             | Bootstrap | Rollback | Live | Live Rollback | Use Case                      |
| ----------------------------------------- | --------- | -------- | ---- | ------------- | ----------------------------- |
| **Default**                               | ✅        | ✅       | ✅   | ✅            | Full 4-phase testing          |
| `skip_bootstrap=True`                     | ❌        | ❌       | ✅   | ✅            | Live-only propagation         |
| `skip_live=True`                          | ✅        | ✅       | ❌   | ❌            | Bootstrap-only testing        |
| `skip_rollback=True`                      | ✅        | ❌       | ✅   | ❌            | No cleanup validation         |
| `skip_bootstrap=True, skip_rollback=True` | ❌        | ❌       | ✅   | ❌            | Live propagation only         |
| `skip_live=True, skip_rollback=True`      | ✅        | ❌       | ❌   | ❌            | Bootstrap propagation only    |
| `skip_bootstrap=True, skip_live=True`     | ❌        | ❌       | ❌   | ❌            | No propagation (cleanup only) |
| **Resilient Mode**                        | ✅        | ✅       | ✅   | ✅            | Continue on failures          |

**Example Custom Configurations:**

```python
# Example: Propagation type that only supports live mode and no cleanup
class NoCleanupPropagationTest(BasePropagationTest):
    def customize_config(self, config: PropagationTestConfig) -> PropagationTestConfig:
        config.skip_bootstrap = True  # Only live mode
        config.skip_rollback = True   # No cleanup support
        return config

# Execution flow: Live only

# Example: Append-only propagation (no rollback support)
class AppendOnlyPropagationTest(BasePropagationTest):
    def customize_config(self, config: PropagationTestConfig) -> PropagationTestConfig:
        config.skip_rollback = True  # Cannot rollback appended data
        return config

# Execution flow: Bootstrap → Live only

# Example: Batch-only propagation (no live updates)
class BatchOnlyPropagationTest(BasePropagationTest):
    def customize_config(self, config: PropagationTestConfig) -> PropagationTestConfig:
        config.skip_live = True  # Only bootstrap mode
        return config

# Execution flow: Bootstrap → Rollback only
```

## 🚀 **Primary Usage: PropagationScenarioBuilder**

The **PropagationScenarioBuilder** is the main way to create propagation tests. It provides a fluent, readable API that handles all the complexity behind the scenes.

### **Step 1: Create Your Test Scenario**

```python
from tests.propagation.framework.builders.scenario_builder import PropagationScenarioBuilder
from tests.propagation.framework.plugins.documentation.expectations import (
    DocumentationPropagationExpectation
)

# Initialize the builder
builder = PropagationScenarioBuilder(test_action_urn, "my_test")
```

### **Step 2: Create Datasets with the Fluent API**

The builder provides a fluent API for creating datasets with metadata:

```python
# Create source dataset with documentation
source_dataset = (
    builder.add_dataset("customers", "snowflake")
    .with_columns(["id", "name", "email", "phone", "address"])
    .with_column_description("id", "Customer unique identifier")
    .with_column_description("email", "Customer email address")
    .with_dataset_description("Customer master data")
    .build()
)

# Create target dataset (initially without documentation)
target_dataset = (
    builder.add_dataset("analytics", "snowflake")
    .with_columns(["customer_id", "customer_name", "customer_email"])
    .build()
)

# Register datasets for easy reference
builder.register_dataset("source", source_dataset)
builder.register_dataset("target", target_dataset)
```

**Available Dataset Methods:**

- `.with_columns(["col1", "col2", ...])` - Add columns to the dataset
- `.with_column_description("col", "description")` - Add field documentation
- `.with_dataset_description("description")` - Add dataset documentation
- `.with_subtype("Source")` - Set dataset subtype
- `.set_sibling(dataset_or_urn, is_primary=True)` - Set sibling relationship
- `.set_logical_parent(parent_dataset_or_urn)` - Set logical parent relationship
- `.build()` - Finalize the dataset

### **Step 3: Define Lineage Relationships**

Create lineage between your datasets:

```python
# Simple 1:1 field lineage
simple_lineage = (
    builder.add_lineage("source", "target")
    .add_field_lineage("source", "id", "target", "customer_id")
    .add_field_lineage("source", "name", "target", "customer_name")
    .build(source_dataset.urn)
)

# Complex lineage with many-to-one relationships
complex_lineage = (
    builder.add_lineage("source", "target")
    .add_field_lineage("source", "id", "target", "customer_id")
    .add_many_to_one_lineage("source", ["name", "email"], "target", "customer_info")
    .build(source_dataset.urn)
)

# Register lineage
builder.register_lineage("source", "target", simple_lineage)
```

**Available Lineage Methods:**

- `.add_field_lineage(source_ds, source_field, target_ds, target_field)` - 1:1 field mapping
- `.add_many_to_one_lineage(source_ds, [source_fields], target_ds, target_field)` - N:1 field mapping
- `.build(dataset_urn)` - Finalize lineage for the dataset

## 🔗 **Dataset Relationships**

The framework supports advanced DataHub relationship types for comprehensive propagation testing:

### **Sibling Relationships**

Create sibling relationships between datasets (e.g., dbt source → physical table):

```python
# Create dbt source dataset
dbt_source = (
    builder.add_dataset("customer_source", "dbt")
    .with_columns(["id", "name", "email"])
    .with_column_description("id", "Customer identifier from dbt docs")
    .with_subtype("Source")
    .build()
)

# Create physical dataset as sibling
snowflake_table = (
    builder.add_dataset("customers", "snowflake")
    .with_columns(["id", "name", "email"])
    .set_sibling(dbt_source, is_primary=True)  # dbt source is primary
    .build()
)

# Framework automatically generates SiblingsClass MCP
scenario = builder.build()  # Sibling relationship included
```

### **Logical Parent Relationships**

Create logical parent-child hierarchies for organizational datasets:

```python
# Create parent dataset (e.g., domain-level)
domain_dataset = (
    builder.add_dataset("customer_domain", "snowflake")
    .with_columns(["domain_id", "domain_name"])
    .with_column_description("domain_id", "Customer domain identifier")
    .build()
)

# Create child dataset with logical parent
child_dataset = (
    builder.add_dataset("customer_profiles", "snowflake")
    .with_columns(["profile_id", "customer_id", "profile_data"])
    .set_logical_parent(domain_dataset)  # Set hierarchical relationship
    .build()
)

# Framework automatically generates LogicalParentClass MCP
scenario = builder.build()  # Logical parent relationship included
```

### **Supported Parameter Types**

Both relationship methods accept flexible input types with proper validation:

```python
# Using dataset objects (recommended)
child.set_logical_parent(parent_dataset)
child.set_sibling(sibling_dataset, is_primary=False)

# Using DatasetUrn instances (type-safe)
from datahub.metadata.urns.urn_defs import DatasetUrn
parent_urn = DatasetUrn.from_string("urn:li:dataset:(urn:li:dataPlatform:snowflake,parent,PROD)")
child.set_logical_parent(parent_urn)

# Using URN strings (validated)
child.set_logical_parent("urn:li:dataset:(urn:li:dataPlatform:snowflake,parent,PROD)")
```

### **Relationship-Based Propagation Testing**

Test propagation across relationship types:

```python
def test_sibling_propagation():
    """Test propagation from dbt source to physical table via sibling relationship."""

    # dbt source with documentation
    dbt_source = (
        builder.add_dataset("customer_source", "dbt")
        .with_columns(["id", "name"])
        .with_column_description("id", "Customer ID from dbt docs")
        .with_subtype("Source")
        .build()
    )

    # Physical table as sibling (no initial docs)
    physical_table = (
        builder.add_dataset("customers", "snowflake")
        .with_columns(["id", "name"])
        .set_sibling(dbt_source, is_primary=True)  # dbt is primary
        .build()
    )

    # Analytics table with lineage from physical table
    analytics = (
        builder.add_dataset("customer_analytics", "snowflake")
        .with_columns(["customer_id", "customer_name"])
        .build()
    )

    # Register datasets
    builder.register_dataset("dbt_source", dbt_source)
    builder.register_dataset("physical", physical_table)
    builder.register_dataset("analytics", analytics)

    # Create lineage: physical → analytics
    lineage = (
        builder.add_lineage("physical", "analytics")
        .add_field_lineage("physical", "id", "analytics", "customer_id")
        .build(physical_table.urn)
    )
    builder.register_lineage("physical", "analytics", lineage)

    # Expect 2-hop propagation: dbt → physical (sibling) → analytics (lineage)
    builder.base_expectations = [
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="test.customers",
            field_name="id",
            expected_description="Customer ID from dbt docs",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(dbt_source.urn, "id"),
            propagation_via=None,  # Direct sibling propagation
        ),
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="test.customer_analytics",
            field_name="customer_id",
            expected_description="Customer ID from dbt docs",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(dbt_source.urn, "id"),
            propagation_via=make_schema_field_urn(physical_table.urn, "id"),  # Via physical table
        )
    ]
```

### **Complex Relationship Scenarios**

Combine multiple relationship types for comprehensive testing:

```python
def test_hierarchical_sibling_propagation():
    """Test complex scenario: parent-child + sibling + lineage relationships."""

    # Domain parent
    domain = builder.add_dataset("customer_domain", "domain").build()

    # dbt source (child of domain)
    dbt_source = (
        builder.add_dataset("customer_source", "dbt")
        .set_logical_parent(domain)
        .with_subtype("Source")
        .build()
    )

    # Physical table (sibling of dbt + child of domain)
    physical = (
        builder.add_dataset("customers", "snowflake")
        .set_logical_parent(domain)
        .set_sibling(dbt_source, is_primary=True)
        .build()
    )

    # Analytics (child of domain + lineage from physical)
    analytics = (
        builder.add_dataset("customer_analytics", "snowflake")
        .set_logical_parent(domain)
        .build()
    )

    # All relationships automatically managed by framework
    scenario = builder.build()
```

### **Step 4: Define Propagation Expectations**

Set up expectations for what should happen during bootstrap and live phases:

```python
# Base expectations (tested after bootstrap)
builder.base_expectations = [
    DocumentationPropagationExpectation(
        platform="snowflake",
        dataset_name="my_test.analytics",
        field_name="customer_id",
        expected_description="Customer unique identifier",
        propagation_source=test_action_urn,
        propagation_origin=make_schema_field_urn(source_dataset.urn, "id"),
    ),
    # Negative expectation - should NOT propagate (N:1 relationship)
    NoDocumentationPropagationExpectation(
        platform="snowflake",
        dataset_name="my_test.analytics",
        field_name="customer_info",  # N:1 mapping should not propagate
    )
]
```

### **Step 5: Add Live Mutations (Optional)**

Define changes that will be tested in live mode using the smart mutation handling:

```python
from tests.propagation.framework.plugins.documentation.mutations import (
    FieldDocumentationUpdateMutation
)
from tests.propagation.framework.plugins.tag.mutations import (
    FieldTagAdditionMutation
)

# Create mutations with only dataset URN (no dataset_name needed)
mutations = [
    FieldDocumentationUpdateMutation(
        dataset_urn=source_dataset.urn,
        field_name="id",
        new_description="Updated customer unique identifier"
    ),
    FieldTagAdditionMutation(
        dataset_urn=source_dataset.urn,
        field_name="email",
        tag_urn="urn:li:tag:PII"
    )
]

# Apply mutations with automatic explanations and MCP handling
scenario.add_mutation_objects(mutations)

# Define expectations after the mutation
builder.post_mutation_expectations = [
    DocumentationPropagationExpectation(
        platform="snowflake",
        dataset_name="my_test.analytics",
        field_name="customer_id",
        expected_description="Updated customer unique identifier",
        propagation_source=test_action_urn,
        propagation_origin=make_schema_field_urn(source_dataset.urn, "id"),
    )
]
```

### **Step 6: Build and Run the Test**

```python
from tests.propagation.framework.core.base import (
    PropagationTestFramework,
    DocumentationPropagationTest
)

# Build the complete scenario
scenario = builder.build()

# Create framework instance
framework = PropagationTestFramework(
    auth_session, graph_client, test_resources_dir
)

# Run the test
result = framework.run_propagation_test(
    scenario=scenario,
    test_action_urn=test_action_urn,
    test_type=DocumentationPropagationTest(),
    scenario_name="My Documentation Propagation Test"
)

# Verify results
assert result.success, f"Test failed: {result.error_details}"
print(result.get_summary())
```

### **Complete Example: Customer Data Propagation**

Here's a complete, realistic example:

```python
import pytest
from datahub.emitter.mce_builder import make_schema_field_urn

def test_customer_data_propagation(
    auth_session, graph_client, test_resources_dir, test_action_urn, create_test_action
):
    """Test documentation propagation from customer source to analytics target."""

    # Step 1: Initialize builder
    builder = PropagationScenarioBuilder(test_action_urn, "customer_analytics")

    # Step 2: Create datasets
    customers = (
        builder.add_dataset("customers", "snowflake")
        .with_columns(["customer_id", "first_name", "last_name", "email", "created_date"])
        .with_column_description("customer_id", "Unique customer identifier")
        .with_column_description("email", "Customer email address")
        .with_dataset_description("Customer master data from CRM")
        .build()
    )

    analytics = (
        builder.add_dataset("customer_analytics", "snowflake")
        .with_columns(["id", "full_name", "contact_email", "signup_date", "segment"])
        .with_dataset_description("Customer analytics and segmentation data")
        .build()
    )

    builder.register_dataset("customers", customers)
    builder.register_dataset("analytics", analytics)

    # Step 3: Define lineage
    lineage = (
        builder.add_lineage("customers", "analytics")
        .add_field_lineage("customers", "customer_id", "analytics", "id")
        .add_field_lineage("customers", "email", "analytics", "contact_email")
        .add_many_to_one_lineage("customers", ["first_name", "last_name"], "analytics", "full_name")
        .build(customers.urn)
    )
    builder.register_lineage("customers", "analytics", lineage)

    # Step 4: Define expectations
    builder.base_expectations = [
        # These should propagate (1:1 mappings)
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="customer_analytics.customer_analytics",
            field_name="id",
            expected_description="Unique customer identifier",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(customers.urn, "customer_id"),
        ),
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="customer_analytics.customer_analytics",
            field_name="contact_email",
            expected_description="Customer email address",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(customers.urn, "email"),
        ),
        # This should NOT propagate (N:1 mapping)
        NoDocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="customer_analytics.customer_analytics",
            field_name="full_name",
        ),
    ]

    # Step 5: Add live mutation
    mutation = DocumentationUpdateMutation(
        field_name="customer_id",
        new_description="Updated unique customer identifier"
    )
    builder.mutations = [mutation.apply_mutation(customers.urn)]

    builder.post_mutation_expectations = [
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="customer_analytics.customer_analytics",
            field_name="id",
            expected_description="Updated unique customer identifier",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(customers.urn, "customer_id"),
        )
    ]

    # Step 6: Build and run
    scenario = builder.build()

    framework = PropagationTestFramework(
        auth_session, graph_client, test_resources_dir
    )

    result = framework.run_propagation_test(
        scenario=scenario,
        test_action_urn=test_action_urn,
        test_type=DocumentationPropagationTest(),
        scenario_name="Customer Data Documentation Propagation"
    )

    assert result.success, f"Test failed: {result.error_details}"
    print(f"✅ Test completed successfully in {result.total_time:.2f}s")
```

### **Common Builder Patterns**

**Multi-hop Lineage:**

```python
# Dataset A → Dataset B → Dataset C
builder.register_lineage("A", "B", lineage_a_to_b)
builder.register_lineage("B", "C", lineage_b_to_c)
```

**Complex Field Mappings:**

```python
lineage = (
    builder.add_lineage("source", "target")
    .add_field_lineage("source", "id", "target", "customer_id")        # 1:1
    .add_field_lineage("source", "email", "target", "contact_email")   # 1:1
    .add_many_to_one_lineage("source", ["fname", "lname"], "target", "name")  # N:1
    .build(source_urn)
)
```

**Batch Expectations:**

```python
# Add multiple expectations at once
base_expectations = []
for field in ["id", "email", "phone"]:
    base_expectations.append(
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="target_table",
            field_name=field,
            expected_description=f"Expected description for {field}",
            propagation_source=test_action_urn,
        )
    )
builder.base_expectations = base_expectations
```

### **Type-Safe Propagation Test Classes**

Define propagation-specific behavior:

```python
class DocumentationPropagationTest(BasePropagationTest):
    def get_action_type(self) -> str:
        return "datahub_integrations.propagation.propagation.generic_propagation_action.GenericPropagationAction"

    def get_recipe_filename(self) -> str:
        return "docs_propagation_generic_action_recipe.yaml"

    def get_action_name(self) -> str:
        return "test_docs_propagation"

    def customize_config(self, config: PropagationTestConfig) -> PropagationTestConfig:
        # Documentation supports all phases
        return config

class TermPropagationTest(BasePropagationTest):
    def get_action_type(self) -> str:
        return "datahub_integrations.propagation.propagation.generic_propagation_action.GenericPropagationAction"

    def get_recipe_filename(self) -> str:
        return "term_propagation_generic_action_recipe.yaml"

    def get_action_name(self) -> str:
        return "test_term_propagation"

    def get_glossary_required(self) -> bool:
        return True  # Terms require glossary setup
```

## 🔧 **Reliable Test Isolation**

The framework ensures reliable test isolation through unique URN generation and proper cleanup:

### **Unique Test Action URNs**

Each test gets a completely unique action URN to prevent state interference:

```python
@pytest.fixture(scope="function")  # Function scope ensures per-test uniqueness
def test_action_urn():
    """Unique URN for test action."""
    import random
    return f"urn:li:dataHubAction:{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
```

### **Automatic Entity Cleanup**

The framework automatically cleans up test entities between tests:

```python
def cleanup_entities_and_actions(
    auth_session: Any,
    graph_client: DataHubGraph,
    urns: Iterable[str],
    test_resources_dir: str,
    remove_actions: bool = True,
):
    """Clean up test entities and actions after test completion."""
    for urn in urns:
        graph_client.delete_entity(urn, hard=True)
```

### **Preventing Test Interference**

- **Function-scoped fixtures** ensure each test gets fresh URNs
- **High-precision timestamps** with randomization prevent URN collisions
- **Automatic cleanup** removes test data between runs
- **Hard entity deletion** ensures complete state reset

This ensures tests pass both **individually** and **when run together** without state leakage.

## 🔧 **URN-Based Parameters**

The framework uses URN-based parameter handling for all expectations, providing type safety, consistency, and better integration:

### **URN-Based Parameters**

```python
# URN-based parameters (recommended)
from datahub.emitter.mce_builder import make_schema_field_urn

expectation = DocumentationPropagationExpectation(
    field_urn=make_schema_field_urn(target_dataset.urn, "customer_id"),
    expected_description="Customer unique identifier",
    propagation_source=test_action_urn,
    propagation_origin=make_schema_field_urn(source_dataset.urn, "id"),
)
```

### **Benefits of URN-Based Parameters**

The URN approach provides several key benefits:

1. **Consistency**: Expectations and mutations both use the same URN-based approach
2. **Type Safety**: URNs are validated and parsed automatically
3. **Builder Integration**: Works seamlessly with `PropagationScenarioBuilder` dataset objects
4. **Future-proof**: Handles complex URN structures and evolving DataHub schemas
5. **Test Logic Accuracy**: Fixed propagation behavior expectations for live vs bootstrap phases

### **Automatic URN Parsing & Validation**

The framework automatically parses field URNs and validates them using Pydantic model validators:

```python
@model_validator(mode="before")
@classmethod
def parse_field_urn(cls, values: Any) -> Dict[str, Any]:
    """Parse field_urn into platform, dataset_name, field_name if provided."""
    if isinstance(values, dict) and "field_urn" in values:
        field_urn = values["field_urn"]
        try:
            parsed_urn = Urn.create_from_string(field_urn)
            dataset_urn = parsed_urn.entity_ids[0]
            field_name = parsed_urn.entity_ids[1]

            # Parse dataset URN to extract platform and dataset name
            dataset_parsed = Urn.create_from_string(dataset_urn)
            values["platform"] = dataset_parsed.entity_ids[0].split(":")[-1]
            values["dataset_name"] = dataset_parsed.entity_ids[1]
            values["field_name"] = field_name

            values.pop("field_urn", None)  # Remove field_urn after parsing
        except Exception as e:
            raise ValueError(f"Failed to parse field_urn '{field_urn}': {e}")

    return values
```

### **Benefits of Field URN Syntax**

1. **Concise**: Single parameter instead of three separate ones
2. **Copy-paste friendly**: Easy to copy URNs from DataHub UI or logs
3. **Type-safe**: Full validation with clear error messages on parse failures
4. **Flexible**: Works alongside traditional parameter syntax
5. **Future-proof**: Handles complex URN structures automatically

### **Supported Across All Plugins**

Field URN shorthand works for all expectation types:

```python
# Documentation expectations
DocumentationPropagationExpectation(field_urn="...", expected_description="...")

# Term expectations
TermPropagationExpectation(field_urn="...", expected_term_urn="...")

# Tag expectations
TagPropagationExpectation(field_urn="...", expected_tag_urn="...")

# Custom expectations (inherit the behavior automatically)
CustomPropagationExpectation(field_urn="...", custom_property="...")
```

## 🧠 **Smart Mutation Handling**

The framework provides intelligent mutation handling that eliminates boilerplate code and provides automatic explanations for all mutations. All mutation classes now support the new API with full type safety and mypy compliance:

### **Mutation Handling**

```python
# New way - automatic handling with explanations
from tests.propagation.framework.plugins.documentation.mutations import (
    FieldDocumentationUpdateMutation
)
from tests.propagation.framework.plugins.tag.mutations import (
    FieldTagAdditionMutation
)

# Create mutations with only essential parameters
mutations = [
    FieldDocumentationUpdateMutation(
        dataset_urn=source_dataset.urn,  # Only URN needed
        field_name="id",
        new_description="Updated customer identifier"
    ),
    FieldTagAdditionMutation(
        dataset_urn=source_dataset.urn,
        field_name="email",
        tag_urn="urn:li:tag:PII"
    )
]

# Smart application with automatic explanations
scenario.add_mutation_objects(mutations)
```

### **Key Benefits of Smart Mutation Handling**

1. **Automatic Dataset Name Extraction**: Dataset names are automatically extracted from URNs using `Urn.from_string()` with fallback handling
2. **Self-Describing Mutations**: All mutations provide `explain()` methods that generate human-readable descriptions
3. **Automatic MCP Generation**: MCPs are generated and applied automatically with proper error handling
4. **Built-in Merging**: Conflicting mutations (e.g., multiple field changes to the same dataset) are automatically merged to avoid overwrites
5. **Visual Feedback**: Mutations display helpful 📝 emojis and explanations during execution

### **Automatic Dataset Name Extraction**

The framework automatically extracts dataset names from URNs, removing the need for redundant parameters:

```python
# BaseMutation class automatically provides dataset_name
@dataclass(frozen=True)
class BaseMutation(ABC):
    dataset_urn: str  # Only this is required

    @property
    def dataset_name(self) -> str:
        """Extract dataset name from URN automatically."""
        from datahub.utilities.urns.urn import Urn
        try:
            parsed_urn = Urn.from_string(self.dataset_urn)
            return parsed_urn.entity_ids[1]  # Dataset name from URN
        except Exception:
            return self.dataset_urn  # Fallback to full URN
```

### **Self-Describing Mutations**

Every mutation provides automatic explanations that are displayed during test execution:

```python
# Example explain() methods in mutations
class FieldDocumentationUpdateMutation(FieldMutation):
    def explain(self) -> str:
        return f"Updating documentation on field {self.dataset_name}.{self.field_name}: '{self.new_description}'"

class FieldTagAdditionMutation(FieldMutation):
    def explain(self) -> str:
        return f"Adding tag {self.tag_urn} to field {self.dataset_name}.{self.field_name}"

class DatasetTermAdditionMutation(BaseMutation):
    def explain(self) -> str:
        return f"Adding term {self.term_urn} to dataset {self.dataset_name}"
```

### **Automatic Mutation Merging**

The framework automatically handles conflicting mutations that target the same dataset by merging them intelligently:

```python
# Multiple field mutations on the same dataset are automatically merged
mutations = [
    FieldDocumentationUpdateMutation(
        dataset_urn=source_urn,
        field_name="id",
        new_description="Customer ID"
    ),
    FieldTagAdditionMutation(
        dataset_urn=source_urn,  # Same dataset
        field_name="email",
        tag_urn="urn:li:tag:PII"
    ),
    FieldTermAdditionMutation(
        dataset_urn=source_urn,  # Same dataset
        field_name="phone",
        term_urn="urn:li:glossaryTerm:ContactInfo"
    )
]

# These are automatically merged into a single EditableSchemaMetadataClass MCP
scenario.add_mutation_objects(mutations)
```

### **Enhanced Logging and Visual Execution Feedback**

During test execution, both mutations and expectations provide comprehensive feedback with attribution details:

**Mutation Logging:**

```
📝 Updating documentation on field customers.id: 'Updated customer identifier'
📝 Adding tag urn:li:tag:PII to field customers.email
📝 Adding term urn:li:glossaryTerm:ContactInfo to field customers.phone
ℹ️ Merged 3 mutations into 1 MCP for dataset customers
```

**Enhanced Expectation Logging:**

```
ℹ️ Expected field tag 'urn:li:tag:PII' on field dataset.table.column (depth=1, direction=down, origin=source.field)
✅ Field tag propagation validated successfully with attribution details
ℹ️ Expected removal of field tag 'urn:li:tag:PII' from field dataset.table.column (rollback mode)
✅ Field tag rollback validated successfully
```

### **Smart Mutation API Reference**

The `add_mutation_objects()` method provides the smart mutation interface:

```python
def add_mutation_objects(self, mutations: List[Any], verbose: bool = True) -> None:
    """Add mutation objects with automatic handling.

    Args:
        mutations: List of mutation objects (FieldMutation, BaseMutation, etc.)
        verbose: Whether to print mutation explanations (default: True)
    """
    for mutation in mutations:
        if verbose:
            print(f"📝 {mutation.explain()}")  # Automatic explanation

        mcps = mutation.apply_mutation()  # Generate MCPs
        self.mutations.extend(mcps)       # Add to scenario

    self._merge_editable_schema_mutations()  # Auto-merge conflicts
```

### **Supported Mutation Types**

Smart mutation handling works with all framework mutation types:

**Field-Level Mutations:**

- `FieldDocumentationUpdateMutation`, `FieldDocumentationAdditionMutation`, `FieldDocumentationRemovalMutation`
- `FieldTagAdditionMutation`, `FieldTagRemovalMutation`, `FieldTagUpdateMutation`
- `FieldTermAdditionMutation`, `FieldTermRemovalMutation`, `FieldTermUpdateMutation`

**Dataset-Level Mutations:**

- `DatasetDocumentationUpdateMutation`
- `DatasetTagAdditionMutation`, `DatasetTagRemovalMutation`
- `DatasetTermAdditionMutation`, `DatasetTermRemovalMutation`
- `DatasetStructuredPropertyAdditionMutation`, `DatasetStructuredPropertyUpdateMutation`

**Multi-Field Mutations:**

- `MultiFieldTermMutation` (atomic multiple field changes)
- `MultipleStructuredPropertiesAdditionMutation`

All mutation types automatically provide explanations and support the smart handling API.

### **Type Safety & MyPy Compliance**

The entire framework is fully mypy compliant with strict type checking:

```python
# All mutation base classes use proper typing
@abstractmethod
def apply_mutation(
    self,
) -> Union[
    List["MetadataChangeProposalWrapper"], List["MetadataChangeProposalClass"]
]:
    """Apply this mutation and return the appropriate MCPs."""
    pass

# Automatic dataset name extraction with type safety
@property
def dataset_name(self) -> str:
    """Extract dataset name from URN automatically."""
    from datahub.utilities.urns.urn import Urn
    try:
        parsed_urn = Urn.from_string(self.dataset_urn)
        return parsed_urn.entity_ids[1]
    except Exception:
        return self.dataset_urn  # Fallback with proper typing
```

This ensures compile-time validation of all mutation parameters and prevents runtime type errors.

### **Legacy Migration Support**

For migrating from old mutation patterns, simply update parameters:

```python
# Before: Old API with dataset_name parameter
mutation = FieldTermAdditionMutation(
    dataset_name="customers",  # Remove this
    field_name="id",
    term_urn="urn:li:glossaryTerm:TestTerm"
)
mcps = mutation.apply_mutation(dataset_urn)  # Manual URN passing

# After: New API with dataset_urn parameter
mutation = FieldTermAdditionMutation(
    dataset_urn=dataset_urn,  # Use URN directly
    field_name="id",
    term_urn="urn:li:glossaryTerm:TestTerm"
)
mcps = mutation.apply_mutation()  # No parameters needed
```

## 📦 **Feature Plugin System**

### **Documentation Plugin**

Complete documentation propagation support:

```python
# Type-safe expectations
from tests.propagation.framework.plugins.documentation.expectations import (
    DocumentationPropagationExpectation,
    NoDocumentationPropagationExpectation,
    DatasetDocumentationPropagationExpectation,
)

# Mutation support
from tests.propagation.framework.plugins.documentation.mutations import (
    FieldDocumentationUpdateMutation,
    FieldDocumentationAdditionMutation,
    FieldDocumentationRemovalMutation,
)

# Usage
expectation = DocumentationPropagationExpectation(
    platform="snowflake",
    dataset_name="target_table",
    field_name="column_0",
    expected_description="Expected description",
    propagation_source=test_action_urn,
    propagation_origin=make_schema_field_urn(source_urn, "column_0"),
    propagation_via=None,  # Direct propagation
)

mutation = DocumentationUpdateMutation(
    field_name="column_0",
    new_description="Updated description"
)
mcp = mutation.apply_mutation(dataset_urn)
```

### **Term Plugin**

Glossary term propagation with validation:

```python
from tests.propagation.framework.plugins.term.expectations import (
    TermPropagationExpectation,
    NoTermPropagationExpectation,
)

expectation = TermPropagationExpectation(
    platform="snowflake",
    dataset_name="target_table",
    field_name="column_0",
    expected_term_urn="urn:li:glossaryTerm:CustomerID",
    propagation_source=test_action_urn,
    expected_depth=1,  # Validate propagation depth
    expected_direction="down",  # Validate propagation direction
    expected_relationship="lineage",  # Validate relationship type
)

# Alternative: Use field_urn shorthand syntax
expectation = TermPropagationExpectation(
    field_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,target_table,PROD),column_0)",
    expected_term_urn="urn:li:glossaryTerm:CustomerID",
    propagation_source=test_action_urn,
)
```

### **Tag Plugin**

Tag propagation with dataset and field-level support:

```python
from tests.propagation.framework.plugins.tag.expectations import (
    TagPropagationExpectation,
    DatasetTagPropagationExpectation,
)

field_tag_expectation = TagPropagationExpectation(
    platform="snowflake",
    dataset_name="target_table",
    field_name="column_0",
    expected_tag_urn="urn:li:tag:PII",
)

dataset_tag_expectation = DatasetTagPropagationExpectation(
    platform="snowflake",
    dataset_name="target_table",
    expected_tag_urn="urn:li:tag:CustomerData",
)
```

### **Structured Properties Plugin**

Structured property propagation for dataset-level metadata:

```python
from tests.propagation.framework.plugins.structured_properties.expectations import (
    DatasetStructuredPropertyPropagationExpectation,
)
from tests.propagation.framework.plugins.structured_properties.mutations import (
    DatasetStructuredPropertyAdditionMutation,
    DatasetStructuredPropertyUpdateMutation,
)
from tests.propagation.framework.plugins.structured_properties.templates import (
    create_structured_property_template,
    create_common_structured_property_templates,
)

# Create structured property template
property_template = create_structured_property_template(
    qualified_name="io.datahub.test.data_classification",
    display_name="Data Classification",
    value_type="string",
    allowed_values=["Public", "Internal", "Confidential", "Restricted"],
    description="Classification level of the data"
)

# Expectation for structured property propagation
structured_prop_expectation = DatasetStructuredPropertyPropagationExpectation(
    platform="snowflake",
    dataset_name="target_table",
    structured_property_urn="urn:li:structuredProperty:io.datahub.test.data_classification",
    expected_value="Confidential",
    expected_value_type="string"
)

# Mutation for adding structured property
mutation = DatasetStructuredPropertyAdditionMutation(
    structured_property_urn="urn:li:structuredProperty:io.datahub.test.data_classification",
    value="Internal",
    value_type="string"
)
```

## 📋 **Complete Usage Example**

Here's a full example showing all framework capabilities:

```python
import pytest
from tests.propagation.framework.core.base import (
    PropagationTestFramework,
    PropagationTestConfig,
    DocumentationPropagationTest,
)
from tests.propagation.framework.builders.scenario_builder import PropagationScenarioBuilder
from tests.propagation.framework.plugins.documentation.expectations import (
    DocumentationPropagationExpectation,
)
from tests.propagation.framework.plugins.documentation.mutations import (
    DocumentationUpdateMutation,
)

def test_complete_documentation_propagation(
    auth_session, graph_client, test_resources_dir, test_action_urn, create_test_action
):
    """Complete example showing all framework phases."""

    # Configure framework
    config = PropagationTestConfig(
        skip_bootstrap=False,    # Test full lifecycle
        skip_live=False,
        verbose_logging=True,
        fail_fast=True,
    )

    framework = PropagationTestFramework(
        auth_session, graph_client, test_resources_dir, config
    )

    # Build scenario
    builder = PropagationScenarioBuilder(test_action_urn, "complete_test")

    # Create source dataset with documentation
    source = (
        builder.add_dataset("customers", "snowflake")
        .with_columns(["id", "name", "email"])
        .with_column_description("id", "Customer identifier")
        .build()
    )

    # Create target dataset
    target = (
        builder.add_dataset("analytics", "snowflake")
        .with_columns(["customer_id", "name", "email"])
        .build()
    )

    builder.register_dataset("source", source)
    builder.register_dataset("target", target)

    # Create lineage
    lineage = (
        builder.add_lineage("source", "target")
        .add_field_lineage("source", "id", "target", "customer_id")
        .build(source.urn)
    )
    builder.register_lineage("source", "target", lineage)

    # Base expectations (tested after bootstrap)
    builder.base_expectations = [
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="complete_test.analytics",
            field_name="customer_id",
            expected_description="Customer identifier",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(source.urn, "id"),
        )
    ]

    # Mutation for live testing
    mutation = DocumentationUpdateMutation(
        field_name="id",
        new_description="Updated customer identifier"
    )
    builder.mutations = [mutation.apply_mutation(source.urn)]

    # Post-mutation expectations (tested after live mutations)
    builder.post_mutation_expectations = [
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="complete_test.analytics",
            field_name="customer_id",
            expected_description="Updated customer identifier",
            propagation_source=test_action_urn,
            propagation_origin=make_schema_field_urn(source.urn, "id"),
        )
    ]

    scenario = builder.build()

    # Run complete 4-phase test
    result = framework.run_propagation_test(
        scenario=scenario,
        test_action_urn=test_action_urn,
        test_type=DocumentationPropagationTest(),
        scenario_name="Complete Documentation Propagation Test"
    )

    # Verify success
    assert result.success, f"Test failed: {result.error_details}"

    # Check phase-specific results
    assert result.phase_results[TestPhase.BOOTSTRAP], "Bootstrap phase failed"
    assert result.phase_results[TestPhase.ROLLBACK], "Rollback phase failed"
    assert result.phase_results[TestPhase.LIVE], "Live phase failed"
    assert result.phase_results[TestPhase.LIVE_ROLLBACK], "Live rollback phase failed"

    print(result.get_summary())
```

## 🔧 **Advanced Usage: Running Without the Framework**

For advanced users who need more control or want to integrate with existing test infrastructure, you can use the framework components directly.

### **Manual Scenario Creation**

Instead of using PropagationScenarioBuilder, you can create scenarios manually:

```python
from tests.propagation.framework.core.models import PropagationTestScenario
from tests.propagation.framework.plugins.documentation.expectations import (
    DocumentationPropagationExpectation
)

# Create scenario manually
scenario = PropagationTestScenario(
    base_graph=[
        # Your MCPs here - datasets, lineage, etc.
        dataset_mcp,
        lineage_mcp,
    ],
    base_expectations=[
        DocumentationPropagationExpectation(
            platform="snowflake",
            dataset_name="target_table",
            field_name="column_0",
            expected_description="Expected description",
            propagation_source=test_action_urn,
        )
    ],
    mutations=[
        # Your mutation MCPs here
        mutation_mcp,
    ],
    post_mutation_expectations=[
        # Post-mutation expectations
    ],
)
```

### **Direct Framework Usage**

Run specific phases manually:

```python
from tests.propagation.framework.core.base import PropagationTestFramework

framework = PropagationTestFramework(auth_session, graph_client, test_resources_dir)

# Run individual phases
try:
    # Bootstrap phase
    framework._execute_bootstrap_phase(scenario, test_action_urn, 120)

    # Rollback phase
    framework._execute_rollback_phase(scenario, test_action_urn, post_mutation=False)

    # Live phase
    framework._execute_live_phase(scenario, test_action_urn, 120)

    # Live rollback phase
    framework._execute_rollback_phase(scenario, test_action_urn, post_mutation=True)

except Exception as e:
    print(f"Phase failed: {e}")
```

### **Custom Validation Logic**

Use the validation system directly:

```python
from tests.propagation.framework.core.base import validate_expectations

# Validate expectations manually
validate_expectations(
    expectations=scenario.base_expectations,
    graph_client=graph_client,
    action_urn=test_action_urn,
    rollback=False,  # or True for rollback validation
    verbose=True,
)
```

### **Action Manager Direct Usage**

Manage actions without the framework:

```python
from tests.propagation.framework.core.action_manager import ActionManager

action_manager = ActionManager(auth_session, graph_client, test_resources_dir)

# Create action manually
with action_manager.create_test_action(
    action_urn=test_action_urn,
    recipe_filename="docs_propagation_generic_action_recipe.yaml",
    action_name="test_docs_propagation",
    action_type="datahub_integrations.propagation.propagation.generic_propagation_action.GenericPropagationAction",
):
    # Run bootstrap
    action_manager.run_bootstrap(test_action_urn)

    # Run rollback
    action_manager.run_rollback(test_action_urn)

    # Start/stop live action
    action_manager.start_live_action(test_action_urn)
    # ... apply mutations ...
    action_manager.stop_live_action(test_action_urn)
```

### **Integration with Existing Test Suites**

Integrate framework components with existing pytest fixtures:

```python
@pytest.fixture
def custom_propagation_test():
    """Custom test fixture that uses framework components."""

    def _run_test(scenario, action_urn, test_type):
        framework = PropagationTestFramework(
            auth_session, graph_client, test_resources_dir
        )

        # Custom pre-test setup
        setup_custom_environment()

        try:
            result = framework.run_propagation_test(
                scenario, action_urn, test_type, "Custom Test"
            )

            # Custom post-test validation
            validate_custom_requirements(result)

            return result
        finally:
            # Custom cleanup
            cleanup_custom_environment()

    return _run_test


def test_with_custom_fixture(custom_propagation_test):
    scenario = create_my_scenario()
    result = custom_propagation_test(scenario, test_action_urn, DocumentationPropagationTest())
    assert result.success
```

### **Custom Result Tracking**

Create your own result tracking:

```python
from tests.propagation.framework.core.base import PropagationTestResult, TestPhase

class CustomTestResult(PropagationTestResult):
    def __init__(self):
        super().__init__()
        self.custom_metrics = {}

    def record_custom_metric(self, name: str, value: any):
        self.custom_metrics[name] = value

    def get_custom_summary(self) -> str:
        lines = [self.get_summary()]  # Base summary
        lines.append("Custom Metrics:")
        for name, value in self.custom_metrics.items():
            lines.append(f"  {name}: {value}")
        return "\n".join(lines)

# Usage
result = CustomTestResult()
result.record_custom_metric("entities_created", 15)
result.record_custom_metric("propagation_depth", 3)
```

### **Plugin Development**

Create custom expectation plugins:

```python
from tests.propagation.framework.core.expectations import ExpectationBase
from pydantic import Field

class CustomPropagationExpectation(ExpectationBase):
    platform: str = Field(..., min_length=1)
    dataset_name: str = Field(..., min_length=1)
    field_name: str = Field(..., min_length=1)
    custom_property: str = Field(..., min_length=1)

    def get_expectation_type(self) -> str:
        return "custom_propagation"

    def check_expectation(self, graph_client, action_urn=None, rollback=False):
        # Your custom validation logic here
        if rollback:
            # Validate that custom property was removed
            pass
        else:
            # Validate that custom property exists and matches expected value
            pass

    def explain(self) -> str:
        return f"Expect custom property '{self.custom_property}' on {self.dataset_name}.{self.field_name}"

# Use your custom expectation
scenario.base_expectations = [
    CustomPropagationExpectation(
        platform="snowflake",
        dataset_name="target_table",
        field_name="column_0",
        custom_property="expected_value",
    )
]
```

### **When to Use Manual Approach**

Use the manual approach when you need:

- **Custom test phases** beyond the standard 4-phase model
- **Integration with existing test infrastructure**
- **Fine-grained control** over timing and execution order
- **Custom validation logic** that doesn't fit the expectation model
- **Performance optimization** for specific use cases
- **Debugging** framework behavior

Most users should start with **PropagationScenarioBuilder** and only move to manual usage when specific requirements demand it.

## 🎯 **Adding New Propagation Types**

### 1. Create Test Type Class

```python
class OwnershipPropagationTest(BasePropagationTest):
    def get_action_type(self) -> str:
        return "custom.ownership.propagation.Action"

    def get_recipe_filename(self) -> str:
        return "ownership_propagation_recipe.yaml"

    def get_action_name(self) -> str:
        return "test_ownership_propagation"

    def customize_config(self, config: PropagationTestConfig) -> PropagationTestConfig:
        # Ownership doesn't support bootstrap
        config.skip_bootstrap = True
        return config
```

### 2. Create Expectation Classes

```python
class OwnershipPropagationExpectation(ExpectationBase):
    platform: str = Field(..., min_length=1)
    dataset_name: str = Field(..., min_length=1)
    field_name: str = Field(..., min_length=1)
    expected_owner: str = Field(..., min_length=1)

    def get_expectation_type(self) -> str:
        return "ownership_propagation"

    def check_expectation(self, graph_client, action_urn=None, rollback=False):
        # Custom ownership validation logic
        pass

    def explain(self) -> str:
        return f"Expect owner '{self.expected_owner}' on {self.dataset_name}.{self.field_name}"
```

### 3. Use in Tests

```python
def test_ownership_propagation(framework, test_action_urn, create_ownership_action):
    # Build scenario with ownership expectations
    scenario = build_ownership_scenario(test_action_urn)

    result = framework.run_propagation_test(
        scenario,
        test_action_urn,
        OwnershipPropagationTest(),  # ← Will skip bootstrap automatically
        "Ownership Propagation Test"
    )

    assert result.success
```
