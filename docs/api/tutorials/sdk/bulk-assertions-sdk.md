import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Bulk Creating Smart Assertions with Python SDK

<FeatureAvailability saasOnly />

This guide specifically covers how to use the DataHub Cloud Python SDK for **bulk creating smart assertions**, including:

- Smart Freshness Assertions
- Smart Volume Assertions
- Smart Column Metric Assertions

This is particularly useful for applying data quality checks across many tables and columns at scale.

## Why Would You Use Bulk Assertion Creation?

Bulk creating assertions with the Python SDK allows you to:

- **Scale data quality**: Apply consistent assertions across hundreds or thousands of tables
- **Automate assertion management**: Programmatically create and update assertions based on metadata patterns
- **Implement governance policies**: Ensure all critical tables have appropriate data quality checks
- **Save time**: Avoid manually creating assertions one by one through the UI

## Prerequisites

You need:

- DataHub Cloud Python SDK installed (`pip install acryl-datahub-cloud`)
- Valid DataHub Cloud credentials configured (server URL and access token with appropriate permissions)

The actor making API calls must have the `Edit Assertions` and `Edit Monitors` privileges for the datasets at hand.

:::note
Before creating assertions, you need to ensure the target datasets are already present in your DataHub instance.
If you attempt to create assertions for entities that do not exist, your operation will fail.
:::

### Goal Of This Guide

This guide will show you how to programmatically create large numbers of smart assertions using the DataHub Cloud Python SDK.

## Overview

The bulk assertion creation process follows these steps:

1. **Discover tables**: Use search or direct table queries to find datasets
2. **Create table-level assertions**: Add freshness and volume assertions for each table
3. **Get column information**: Retrieve schema details for each table
4. **Create column-level assertions**: Add column metric assertions for each relevant column
5. **Store assertion URNs**: Save assertion identifiers for future updates

## Setup

First, initialize the SDK client. For detailed client setup options and authentication methods, see the [DataHub Python SDK documentation](../../python-sdk/).

```python
from datahub.sdk import DataHubClient

# Initialize the DataHub client
client = DataHubClient(
    server="https://your-datahub-instance.com",  # http://localhost:8080 if you are running locally in dev mode
    token="your-access-token",
)

# Alternatively, initialize via using the from_env() method after setting the DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN env vars
# or by creating a ~/.datahubenv file via running `datahub init`.
client = DataHubClient.from_env()
```

## Step 1: Discover Tables

### Option A: Get Specific Tables

```python
from datahub.metadata.urns import DatasetUrn

# Define specific tables you want to add assertions to
table_urns = [
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.users,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.orders,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.products,PROD)",
]

# Convert to DatasetUrn objects
datasets = [DatasetUrn.from_string(urn) for urn in table_urns]
```

### Option B: Search for Tables by Pattern

For comprehensive search capabilities and filter options, see the [Search API documentation](../sdk/search_client.md).

```python
from datahub.sdk.search_filters import FilterDsl
from datahub.metadata.urns import DatasetUrn

# Search for tables matching criteria
def find_tables_by_pattern(client, platform="snowflake", name_pattern="production_*"):
    """Find tables matching a specific pattern."""
    # Create filters for datasets on a specific platform with name pattern
    filters = FilterDsl.and_(
        FilterDsl.entity_type("dataset"),
        FilterDsl.platform(platform),
        FilterDsl.custom_filter("name", "EQUAL", [name_pattern])
    )

    # Use the search client to find matching datasets
    urns = list(client.search.get_urns(filter=filters))
    return [DatasetUrn.from_string(str(urn)) for urn in urns]

# Use the search function
datasets = find_tables_by_pattern(client, platform="snowflake", name_pattern="production_*")
```

### Option C: Get Tables by Tag or Domain

```python
def find_tables_by_tag(client, tag_name="critical"):
    """Find tables with a specific tag."""
    # Create filters for datasets with a specific tag
    filters = FilterDsl.and_(
        FilterDsl.entity_type("dataset"),
        FilterDsl.custom_filter("tags", "EQUAL", [f"urn:li:tag:{tag_name}"])
    )

    # Use the search client to find matching datasets
    urns = list(client.search.get_urns(filter=filters))
    return [DatasetUrn.from_string(str(urn)) for urn in urns]

# Find all tables tagged as "critical"
critical_datasets = find_tables_by_tag(client, "critical")
```

## Step 2: Create Table-Level Assertions

### Smart Freshness Assertions

```python
import json
from datetime import datetime

# Storage for assertion URNs (for later updates)
assertion_registry = {
    "freshness": {},
    "volume": {},
    "column_metrics": {}
}

def create_freshness_assertions(datasets, client, registry):
    """Create smart freshness assertions for multiple datasets."""

    for dataset_urn in datasets:
        try:
            # Create smart freshness assertion
            freshness_assertion = client.assertions.sync_smart_freshness_assertion(
                dataset_urn=dataset_urn,
                display_name=f"Freshness Anomaly Monitor",
                # Detection mechanism - information_schema is recommended
                detection_mechanism="information_schema",
                # Smart sensitivity setting
                sensitivity="medium",  # options: "low", "medium", "high"
                # Tags for grouping (supports urns or plain tag names!)
                tags=["automated", "freshness", "data_quality"],
                # Enable the assertion
                enabled=True
            )

            # Store the assertion URN for future reference
            registry["freshness"][str(dataset_urn)] = str(freshness_assertion.urn)

            print(f"✅ Created freshness assertion for {dataset_urn.name}: {freshness_assertion.urn}")

        except Exception as e:
            print(f"❌ Failed to create freshness assertion for {dataset_urn.name}: {e}")

# Create freshness assertions for all datasets
create_freshness_assertions(datasets, client, assertion_registry)
```

### Smart Volume Assertions

```python
def create_volume_assertions(datasets, client, registry):
    """Create smart volume assertions for multiple datasets."""

    for dataset_urn in datasets:
        try:
            # Create smart volume assertion
            volume_assertion = client.assertions.sync_smart_volume_assertion(
                dataset_urn=dataset_urn,
                display_name=f"Smart Volume Check",
                # Detection mechanism options
                detection_mechanism="information_schema",
                # Smart sensitivity setting
                sensitivity="medium",
                # Tags for grouping
                tags=["automated", "volume", "data_quality"],
                # Schedule (optional - defaults to hourly)
                schedule="0 */6 * * *",  # Every 6 hours
                # Enable the assertion
                enabled=True
            )

            # Store the assertion URN
            registry["volume"][str(dataset_urn)] = str(volume_assertion.urn)

            print(f"✅ Created volume assertion for {dataset_urn.name}: {volume_assertion.urn}")

        except Exception as e:
            print(f"❌ Failed to create volume assertion for {dataset_urn.name}: {e}")

# Create volume assertions for all datasets
create_volume_assertions(datasets, client, assertion_registry)
```

## Step 3: Get Column Information

```python
def get_dataset_columns(client, dataset_urn):
    """Get column information for a dataset."""
    try:
        # Get dataset using the entities client
        dataset = client.entities.get(dataset_urn)
        if dataset and hasattr(dataset, 'schema') and dataset.schema:
            return [
                {
                    "name": field.field_path,
                    "type": field.native_data_type,
                    "nullable": field.nullable if hasattr(field, 'nullable') else True
                }
                for field in dataset.schema.fields
            ]
        return []
    except Exception as e:
        print(f"❌ Failed to get columns for {dataset_urn}: {e}")
        return []

# Get columns for each dataset
dataset_columns = {}
for dataset_urn in datasets:
    columns = get_dataset_columns(client, dataset_urn)
    dataset_columns[str(dataset_urn)] = columns
    print(f"📊 Found {len(columns)} columns in {dataset_urn.name}")
```

## Step 4: Create Column-Level Assertions

### Smart Column Metric Assertions

```python
def create_column_assertions(datasets, columns_dict, client, registry):
    """Create smart column metric assertions for multiple datasets and columns."""

    # Define rules for which columns should get which assertions
    assertion_rules = {
        # Null count checks for critical columns
        "null_checks": {
            "column_patterns": ["id", "*_id", "user_id", "email"],
            "metric_type": "null_count",
            "operator": "equal_to",
            "value": 0
        },
        # Unique count checks for ID columns
        "unique_checks": {
            "column_patterns": ["*_id", "email", "username"],
            "metric_type": "unique_percentage",
            "operator": "greater_than_or_equal_to",
            "value": 0.95
        },
        # Range checks for numeric columns
        "range_checks": {
            "column_patterns": ["amount", "price", "quantity", "score"],
            "metric_type": "min",
            "operator": "greater_than_or_equal_to",
            "value": 0
        }
    }

    for dataset_urn in datasets:
        dataset_key = str(dataset_urn)
        columns = columns_dict.get(dataset_key, [])

        if not columns:
            print(f"⚠️ No columns found for {dataset_urn.name}")
            continue

        registry["column_metrics"][dataset_key] = {}

        for column in columns:
            column_name = column["name"]
            column_type = column["type"].upper()

            # Apply assertion rules based on column name and type
            for rule_name, rule_config in assertion_rules.items():
                if should_apply_rule(column_name, column_type, rule_config):
                    try:
                        assertion = client.assertions.sync_smart_column_metric_assertion(
                            dataset_urn=dataset_urn,
                            column_name=column_name,
                            metric_type=rule_config["metric_type"],
                            operator=rule_config["operator"],
                            criteria_parameters=rule_config["value"],
                            display_name=f"{rule_name.replace('_', ' ').title()} - {column_name}",
                            # Detection mechanism for column metrics
                            detection_mechanism="all_rows_query_datahub_dataset_profile",
                            # Tags (plain names automatically converted to URNs)
                            tags=["automated", "column_quality", rule_name],
                            enabled=True
                        )

                        # Store assertion URN
                        if column_name not in registry["column_metrics"][dataset_key]:
                            registry["column_metrics"][dataset_key][column_name] = {}
                        registry["column_metrics"][dataset_key][column_name][rule_name] = str(assertion.urn)

                        print(f"✅ Created {rule_name} assertion for {dataset_urn.name}.{column_name}")

                    except Exception as e:
                        print(f"❌ Failed to create {rule_name} assertion for {dataset_urn.name}.{column_name}: {e}")

def should_apply_rule(column_name, column_type, rule_config):
    """Determine if a rule should be applied to a column."""
    import fnmatch

    # Check column name patterns
    for pattern in rule_config["column_patterns"]:
        if fnmatch.fnmatch(column_name.lower(), pattern.lower()):
            return True

    # Add type-based rules if needed
    if rule_config.get("column_types"):
        return any(col_type in column_type for col_type in rule_config["column_types"])

    return False

# Create column assertions
create_column_assertions(datasets, dataset_columns, client, assertion_registry)
```

## Step 5: Store Assertion URNs

### Save to File

```python
import json
from datetime import datetime

def save_assertion_registry(registry, filename=None):
    """Save assertion URNs to a file for future reference."""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"assertion_registry_{timestamp}.json"

    # Add metadata
    registry_with_metadata = {
        "created_at": datetime.now().isoformat(),
        "total_assertions": {
            "freshness": len(registry["freshness"]),
            "volume": len(registry["volume"]),
            "column_metrics": sum(
                len(cols) for cols in registry["column_metrics"].values()
            )
        },
        "assertions": registry
    }

    with open(filename, 'w') as f:
        json.dump(registry_with_metadata, f, indent=2)

    print(f"💾 Saved assertion registry to {filename}")
    return filename

# Save the registry
registry_file = save_assertion_registry(assertion_registry)
```

### Load from File (for updates)

```python
def load_assertion_registry(filename):
    """Load assertion URNs from a previously saved file."""
    with open(filename, 'r') as f:
        data = json.load(f)
    return data["assertions"]

# Later, load for updates
# assertion_registry = load_assertion_registry("assertion_registry_20240101_120000.json")
```

## Step 6: Update Existing Assertions

```python
def update_existing_assertions(registry, client):
    """Update existing assertions using stored URNs."""

    # Update freshness assertions
    for dataset_urn, assertion_urn in registry["freshness"].items():
        try:
            updated_assertion = client.assertions.sync_smart_freshness_assertion(
                dataset_urn=dataset_urn,
                urn=assertion_urn,  # Provide existing URN for updates
                # Update any parameters as needed
                sensitivity="high",  # Change sensitivity
                tags=["automated", "freshness", "data_quality", "updated"],
                enabled=True
            )
            print(f"🔄 Updated freshness assertion {assertion_urn}")
        except Exception as e:
            print(f"❌ Failed to update freshness assertion {assertion_urn}: {e}")

# Update assertions when needed
# update_existing_assertions(assertion_registry, client)
```

## Advanced Patterns

### Conditional Assertion Creation

```python
def create_conditional_assertions(datasets, client):
    """Create assertions based on dataset metadata conditions."""

    for dataset_urn in datasets:
        try:
            # Get dataset metadata
            dataset = client.entities.get(dataset_urn)

            # Check if dataset has specific tags
            if dataset.tags and any("critical" in str(tag.tag) for tag in dataset.tags):
                # Create more stringent assertions for critical datasets
                client.assertions.sync_smart_freshness_assertion(
                    dataset_urn=dataset_urn,
                    sensitivity="high",
                    detection_mechanism="information_schema",
                    tags=["critical", "automated", "freshness"]
                )

            # Check dataset size and apply appropriate volume checks
            if dataset.dataset_properties:
                # Create different volume assertions based on table characteristics
                pass

        except Exception as e:
            print(f"❌ Error processing {dataset_urn}: {e}")
```

### Batch Processing with Error Handling

```python
import time
from typing import List, Dict, Any

def batch_create_assertions(
    datasets: List[DatasetUrn],
    client: DataHubClient,
    batch_size: int = 10,
    delay_seconds: float = 1.0
) -> Dict[str, Any]:
    """Create assertions in batches with error handling and rate limiting."""

    results = {
        "successful": [],
        "failed": [],
        "total_processed": 0
    }

    for i in range(0, len(datasets), batch_size):
        batch = datasets[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}: {len(batch)} datasets")

        for dataset_urn in batch:
            try:
                # Create assertion
                assertion = client.assertions.sync_smart_freshness_assertion(
                    dataset_urn=dataset_urn,
                    tags=["batch_created", "automated"],
                    enabled=True
                )
                results["successful"].append({
                    "dataset_urn": str(dataset_urn),
                    "assertion_urn": str(assertion.urn)
                })

            except Exception as e:
                results["failed"].append({
                    "dataset_urn": str(dataset_urn),
                    "error": str(e)
                })

            results["total_processed"] += 1

        # Rate limiting between batches
        if i + batch_size < len(datasets):
            time.sleep(delay_seconds)

    return results

# Use batch processing
batch_results = batch_create_assertions(datasets, client, batch_size=5)
print(f"Batch results: {batch_results['total_processed']} processed, "
      f"{len(batch_results['successful'])} successful, "
      f"{len(batch_results['failed'])} failed")
```

## Best Practices

### 1. **Tag Strategy**

- Use consistent tag names for grouping assertions: `["automated", "freshness", "critical"]`
- Plain tag names are automatically converted to URNs: `"my_tag"` → `"urn:li:tag:my_tag"`
- Create a tag hierarchy for different assertion types and priorities

### 2. **Error Handling**

- Always wrap assertion creation in try-catch blocks
- Log failures for later investigation
- Implement retry logic for transient failures

### 3. **URN Management**

- Store assertion URNs in a persistent location (file, database, etc.)
- Use meaningful file naming with timestamps
- Include metadata about when and why assertions were created

### 4. **Performance Considerations**

- Process datasets in batches to avoid overwhelming the API
- Add delays between batch processing if needed
- Consider running during off-peak hours for large bulk operations

:::important Production Considerations
Our backend is designed to handle large scale operations. However, since writes are submitted asynchronously onto a Kafka queue, you may experience significant delays in the operations being applied. If you run into any issues, here are some tips that may help:

1. **Consider running off peak** to prevent causing spikes in Kafka lag
2. **Before you re-run sync** (i.e. to update), wait for GMS to complete processing the previous run to prevent inconsistencies and duplicating: i.e., check if last ingested item has reflected in GMS
3. **Monitor processing status** through the DataHub UI or API to ensure operations complete successfully
   :::

### 5. **Testing Strategy**

- Start with a small subset of datasets for testing
- Validate assertion creation before bulk processing
- Test update scenarios with existing assertions

## Complete Example Script

```python
#!/usr/bin/env python3
"""
Complete example script for bulk creating smart assertions.
"""

import json
import time
from datetime import datetime
from typing import List, Dict, Any

from datahub.sdk import DataHubClient
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn

def main():
    # Initialize the DataHub client
    client = DataHubClient(
        server="https://your-datahub-instance.com",
        token="your-access-token",
    )

    # The client provides both search and entity access

    # Define target datasets
    table_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.analytics.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.analytics.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.analytics.products,PROD)",
    ]

    datasets = [DatasetUrn.from_string(urn) for urn in table_urns]

    # Registry to store assertion URNs
    assertion_registry = {
        "freshness": {},
        "volume": {},
        "column_metrics": {}
    }

    print(f"🚀 Starting bulk assertion creation for {len(datasets)} datasets")

    # Step 1: Create table-level assertions
    print("\n📋 Creating freshness assertions...")
    create_freshness_assertions(datasets, client, assertion_registry)

    print("\n📊 Creating volume assertions...")
    create_volume_assertions(datasets, client, assertion_registry)

    # Step 2: Get column information and create column assertions
    print("\n🔍 Analyzing columns and creating column assertions...")
    dataset_columns = {}
    for dataset_urn in datasets:
        columns = get_dataset_columns(client, dataset_urn)
        dataset_columns[str(dataset_urn)] = columns

    create_column_assertions(datasets, dataset_columns, client, assertion_registry)

    # Step 3: Save results
    print("\n💾 Saving assertion registry...")
    registry_file = save_assertion_registry(assertion_registry)

    # Summary
    total_assertions = (
        len(assertion_registry["freshness"]) +
        len(assertion_registry["volume"]) +
        sum(len(cols) for cols in assertion_registry["column_metrics"].values())
    )

    print(f"\n✅ Bulk assertion creation complete!")
    print(f"   📈 Total assertions created: {total_assertions}")
    print(f"   🕐 Freshness assertions: {len(assertion_registry['freshness'])}")
    print(f"   📊 Volume assertions: {len(assertion_registry['volume'])}")
    print(f"   🎯 Column assertions: {sum(len(cols) for cols in assertion_registry['column_metrics'].values())}")
    print(f"   💾 Registry saved to: {registry_file}")

if __name__ == "__main__":
    main()
```

This guide provides a comprehensive approach to bulk creating smart assertions using the DataHub Cloud Python SDK. The new tag name auto-conversion feature makes it easier to organize and manage your assertions with simple, readable tag names that are automatically converted to proper URN format.
