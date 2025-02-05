import FeatureAvailability from '@site/src/components/FeatureAvailability';

# DataHub Iceberg Catalog

<FeatureAvailability/>

> Note that this feature is currently in open **Beta**. With any questions or issues, please reach out to your Acryl 
> representative.

> Open Source DataHub: 1.0.0

## Introduction

DataHub Iceberg Catalog provides integration with Apache Iceberg, allowing you to manage Iceberg tables through DataHub. This tutorial walks through setting up and using the DataHub Iceberg Catalog to create, read, and manage Iceberg tables. The catalog provides a secure way to manage Iceberg tables through DataHub's permissions model  while also enabling discovery use-cases for humans instantly.

## Use Cases

- Create and manage Iceberg tables through DataHub
- Maintain consistent metadata across DataHub and Iceberg
- Facilitate data discovery by exposing Iceberg table metadata in DataHub
- Enable secure access to Iceberg tables through DataHub's permissions model

## Conceptual Mapping

| Iceberg Concept | DataHub Concept | Notes |
|-----------------|-----------------|--------|
| Overall platform | dataPlatform | `iceberg` platform |
| Warehouse | dataPlatformInstance | Stores info such as storage credentials |
| Namespace | container | Hierarchical containers of subtype "Namespace" |
| Tables, Views | dataset | Dataset URN is UUID that persists across renames |
| Table/View Names | platformResource | Points to dataset, changes with rename operations |

## Prerequisites

Before starting, ensure you have:

1. DataHub installed and running locally, and `datahub` cli is installed and setup to point to it. 
2. AWS credentials and appropriate permissions configured.
3. The following environment variables set:
   ```bash
   DH_ICEBERG_CLIENT_ID="your_client_id"
   DH_ICEBERG_CLIENT_SECRET="your_client_secret" 
   DH_ICEBERG_AWS_ROLE="arn:aws:iam::123456789012:role/your-role-name"  # Example format
   DH_ICEBERG_DATA_ROOT="s3://your-bucket/path"

   ```
4. Configure pyiceberg to use your local datahub using one of its supported ways. For example, create `~/.pyiceberg.yaml` with
```commandline
catalog:
  local_datahub:
    uri: http://localhost:8080/iceberg
    warehouse: arctic_warehouse
```

Note: The python code snippets in this tutorial are available in the `metadata-ingestion/examples/iceberg` folder of the DataHub repository. These snippets require `pyiceberg[duckdb] >=0.8.1` to be installed.

### Required AWS Permissions

The AWS role must have read and write permissions for the S3 location specified in `DH_ICEBERG_DATA_ROOT`.

Note: These permissions must be granted for the specific S3 bucket and path prefix where your Iceberg tables will be stored (as specified in `DH_ICEBERG_DATA_ROOT`). Additionally, the role must have a trust policy that allows it to be assumed using the AWS credentials provided in `DH_ICEBERG_CLIENT_ID` and `DH_ICEBERG_CLIENT_SECRET`.

## Setup Instructions

### 1. Provision a Warehouse

First, create an Iceberg warehouse in DataHub using the provided script (`provision_warehouse.py`):

```python
import os

from constants import warehouse

# Assert that env variables are present

assert os.environ.get("DH_ICEBERG_CLIENT_ID"), (
   "DH_ICEBERG_CLIENT_ID variable is not present"
)
assert os.environ.get("DH_ICEBERG_CLIENT_SECRET"), (
   "DH_ICEBERG_CLIENT_SECRET variable is not present"
)
assert os.environ.get("DH_ICEBERG_AWS_ROLE"), (
   "DH_ICEBERG_AWS_ROLE variable is not present"
)
assert os.environ.get("DH_ICEBERG_DATA_ROOT"), (
   "DH_ICEBERG_DATA_ROOT variable is not present"
)

assert os.environ.get("DH_ICEBERG_DATA_ROOT", "").startswith("s3://")

os.system(
   f"datahub iceberg create --warehouse {warehouse} --data_root $DH_ICEBERG_DATA_ROOT/{warehouse} --client_id $DH_ICEBERG_CLIENT_ID --client_secret $DH_ICEBERG_CLIENT_SECRET --region 'us-east-1' --role $DH_ICEBERG_AWS_ROLE"
)
```

After provisioning the warehouse, ensure your DataHub user has the following privileges to the resource type Data Platform Instance, which were introduced with Iceberg support:
- `DATA_MANAGE_VIEWS_PRIVILEGE`
- `DATA_MANAGE_TABLES_PRIVILEGE`
- `DATA_MANAGE_NAMESPACES_PRIVILEGE`
- `DATA_LIST_ENTITIES_PRIVILEGE`

You can grant these privileges through the DataHub UI under the Policies section.

### 2. Create a Table

You can create Iceberg tables using PyIceberg with a defined schema. Here's an example creating a ski resort metrics table:

```python
from constants import namespace, table_name, warehouse

from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestampType
from pyiceberg.catalog import load_catalog
from datahub.ingestion.graph.client import get_default_graph

# Get DataHub graph client for authentication
graph = get_default_graph()

# Define schema with documentation
schema = Schema(
    NestedField(
        field_id=1,
        name="resort_id",
        field_type=LongType(),
        required=True,
        doc="Unique identifier for each ski resort"
    ),
    NestedField(
        field_id=2,
        name="resort_name",
        field_type=StringType(),
        required=True,
        doc="Official name of the ski resort"
    ),
    NestedField(
        field_id=3,
        name="daily_snowfall",
        field_type=LongType(),
        required=False,
        doc="Amount of new snow in inches during the last 24 hours"
    ),
    NestedField(
        field_id=4,
        name="conditions",
        field_type=StringType(),
        required=False,
        doc="Current snow conditions description"
    ),
    NestedField(
        field_id=5,
        name="last_updated",
        field_type=TimestampType(),
        required=False,
        doc="Timestamp of when the snow report was last updated"
    )
)

# Load catalog and create table
catalog = load_catalog("local_datahub", warehouse=warehouse, token=graph.config.token)
catalog.create_namespace(namespace)
catalog.create_table(f"{namespace}.{table_name}", schema)
```

### 3. Write Data

You can write data to your Iceberg table using PyArrow. Note the importance of matching the schema exactly:

```python
import pyarrow as pa
from datetime import datetime

# Create PyArrow schema to match Iceberg schema
pa_schema = pa.schema([
    ("resort_id", pa.int64(), False),  # False means not nullable
    ("resort_name", pa.string(), False),
    ("daily_snowfall", pa.int64(), True),
    ("conditions", pa.string(), True),
    ("last_updated", pa.timestamp("us"), True),
])

# Create sample data
sample_data = pa.Table.from_pydict(
    {
        "resort_id": [1, 2, 3],
        "resort_name": ["Snowpeak Resort", "Alpine Valley", "Glacier Heights"],
        "daily_snowfall": [12, 8, 15],
        "conditions": ["Powder", "Packed", "Fresh Powder"],
        "last_updated": [
            datetime.now(),
            datetime.now(),
            datetime.now()
        ]
    },
    schema=pa_schema
)

# Write to table
table = catalog.load_table(f"{namespace}.{table_name}")
table.overwrite(sample_data)

# Refresh table to see changes
table.refresh()
```

### 4. Read Data

Reading data from an Iceberg table using DuckDB integration:

```python
from pyiceberg.catalog import load_catalog

# Get DataHub graph client for authentication
graph = get_default_graph()


catalog = load_catalog("local_datahub", warehouse=warehouse, token=graph.config.token)
table = catalog.load_table(f"{namespace}.{table_name}")
con = table.scan().to_duckdb(table_name=table_name)

# Query the data
print("\nResort Metrics Data:")
print("-" * 50)
for row in con.execute(f"SELECT * FROM {table_name}").fetchall():
    print(row)
```

## Integration with Compute Engines

### Spark Integration

You can connect to the DataHub Iceberg Catalog using Spark SQL by defining `$GMS_HOST`, `$GMS_PORT`, `$WAREHOUSE` to connect to and `$USER_PAT` - the DataHub Personal Access Token used to connect to the catalog:

```sql
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=rest \
    --conf spark.sql.catalog.local.uri=http://$GMS_HOST:$GMS_PORT/iceberg/ \
    --conf spark.sql.catalog.local.warehouse=$WAREHOUSE \
    --conf spark.sql.catalog.local.token=$USER_PAT \
    --conf spark.sql.catalog.local.header.X-Iceberg-Access-Delegation=vended-credentials \
    --conf spark.sql.defaultCatalog=local
```

## Reference Information

### Trust Policy Example
When setting up your AWS role, you'll need to configure a trust policy. Here's an example:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123456789012:user/iceberg-user"  // The IAM user or role associated with your credentials
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```

### Namespace and Configuration Requirements

1. **Namespace Creation**: A namespace must be created before any tables can be created within it.

2. **Spark SQL Configuration**: To simplify SQL statements by avoiding the need to prefix catalog name and namespace, you can set defaults in your Spark configuration:
   ```
   --conf spark.sql.defaultCatalog=<default-catalog-name> \
   --conf spark.sql.catalog.local.default-namespace=<default-namespace>
   ```

### DataHub Iceberg CLI

The DataHub CLI provides several commands for managing Iceberg warehouses:

1. **List Warehouses**:
   ```
   datahub iceberg list
   ```

2. **Update Warehouse Configuration**:
   ```
   datahub iceberg update \
     -w $WAREHOUSE_NAME \
     -d $DH_ICEBERG_DATA_ROOT \
     -i $DH_ICEBERG_CLIENT_ID \
     --client_secret $DH_ICEBERG_CLIENT_ID \
     --region $DH_ICEBERG_REGION \
     --role DH_ICEBERG_AWS_ROLE
   ```

3. **Delete Warehouse**:
   ```
   datahub iceberg delete -w $WAREHOUSE_NAME
   ```
   Note: This command deletes Containers associated with Namespaces and Datasets associated with Tables and Views in this warehouse. However, it does not delete any backing data in the warehouse data_root location - it only deletes entries from the catalog.

For additional options and help, run:
```
datahub iceberg [command] --help
```

### Migrating from Other Iceberg Catalogs

When migrating from another Iceberg catalog, you can register existing Iceberg tables using the `system.register_table` command. This allows you to start managing existing Iceberg tables through DataHub without moving the underlying data.

Example of registering an existing table:
```
# REGISTER EXISTING ICEBERG TABLE 
call system.register_table('barTable', 's3://my-s3-bucket/my-data-root/fooNs/barTable/metadata/00000-f9dbba67-df4f-4742-9ba5-123aa2bb4076.metadata.json');
select * from barTable;
```

### Security and Permissions

DataHub provides granular access control for Iceberg operations through policies. The following privileges were introduced with Iceberg support:

| Operation | Required Privilege | Resource Type |
|-----------|-------------------|---------------|
| CREATE or DROP namespaces | Manage Namespaces | Data Platform Instance |
| CREATE, ALTER or DROP tables | Manage Tables | Data Platform Instance |
| CREATE, ALTER or DROP views | Manage Views | Data Platform Instance |
| SELECT from tables or views | Read Only data-access | Dataset |
| INSERT, UPDATE, DELETE or ALTER tables | Read-write data-access | Dataset |
| List tables or views | List tables, views and namespaces | Data Platform Instance |

To configure access:
1. Create policies with the appropriate privileges for Iceberg users
<p>
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/cec184aa1e3cb15c087625ffc997b4345a858c8b/imgs/iceberg-policy-type.png"/>
</p>

2. Scope the policies by:
    * Selecting specific warehouse Data Platform Instances for namespace, table management, and listing privileges
    * Selecting specific DataSets for table and view data access privileges
    * Selecting Tags that may be applied to DataSets or Data Platform Instances

<p>
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/cec184aa1e3cb15c087625ffc997b4345a858c8b/imgs/iceberg-policy-privileges.png"/>
</p>

3. Assign the policies to relevant users or groups
<p>
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/cec184aa1e3cb15c087625ffc997b4345a858c8b/imgs/iceberg-policy-users-groups.png"/>
</p>

### Iceberg tables in DataHub
Once you create tables in iceberg, each of those tables show up in DataHub as a DataSet
<p>
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/cec184aa1e3cb15c087625ffc997b4345a858c8b/imgs/iceberg-dataset-schema.png"/>
</p>

## Troubleshooting

### Q: How do I verify my warehouse was created successfully?

A: Check the DataHub UI under Data Platform Instances, or try creating and reading a table using the provided scripts.

### Q: What permissions are needed for S3 access?

A: The role specified in `DH_ICEBERG_AWS_ROLE` should have permissions to read and write to the S3 bucket specified in `DH_ICEBERG_DATA_ROOT`.

### Q: How does my compute engine authenticate with DataHub?

A: A DataHub Personal Access Token can be used via bearer token to authenticate with DataHub.

### Q: What should I do if table creation fails?

A: Check that:
1. All required environment variables are set correctly
2. Your AWS role has the necessary permissions
3. The namespace exists (create it if needed)
4. The table name doesn't already exist

## Known Limitations

- AWS S3 only support
- Concurrency - supports single instance of GMS
- Multi-table transactional `/commit` is not yet supported
- Table operation purge is not yet implemented
- Metric collection and credential refresh mechanisms are still in development

## Future Enhancements

- Improved concurrency with multiple replicas of GMS
- Support for Iceberg multi-table transactional `/commit`
- Additional table APIs (scan, drop table purge)
- Azure and GCP Support
- Namespace permissions
- Enhanced metrics
- Credential refresh 
- Proxy to another REST Catalog to use its capabilities while DataHub has real-time metadata updates.

## Related Documentation

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [DataHub Documentation](https://datahubproject.io/docs/)