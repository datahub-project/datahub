import FeatureAvailability from '@site/src/components/FeatureAvailability';

# DataHub Iceberg Catalog

<FeatureAvailability/>

> Note that this feature is currently in open **Beta**. With any questions or issues, please reach out to your Acryl 
> representative.

> Open Source DataHub: 1.0.0

## Introduction

DataHub Iceberg Catalog provides integration with Apache Iceberg, allowing you to manage Iceberg tables through DataHub. This tutorial walks through setting up and using the DataHub Iceberg Catalog to create, read, and manage Iceberg tables. The catalog supports bi-directional sync of metadata and provides a secure way to manage Iceberg tables through DataHub's permissions model.

## Use Cases

- Create and manage Iceberg tables through DataHub
- Maintain consistent metadata across DataHub and Iceberg
- Support data lineage tracking for Iceberg tables
- Facilitate data discovery by exposing Iceberg table metadata in DataHub
- Enable secure access to Iceberg tables through DataHub's permissions model
- Streamline data operations with automated warehouse provisioning
- Enhance data governance with policy tags and metadata synchronization
- Support compliance efforts through automated tagging and access control

## Prerequisites

Before starting, ensure you have:

1. DataHub installed and running locally
2. AWS credentials and appropriate permissions configured
3. The following environment variables set:
   ```bash
   ICEBREAKER_CLIENT_ID="your_client_id"
   ICEBREAKER_CLIENT_SECRET="your_client_secret" 
   ICEBREAKER_TEST_ROLE="arn:aws:iam::123456789012:role/your-role-name"  # Example format
   ICEBREAKER_DATA_ROOT="s3://your-bucket/path"
   ```

Note: The code snippets in this tutorial are available in the `metadata-ingestion/examples/iceberg` folder of the DataHub repository.

### Required AWS Permissions

The AWS role must have read and write permissions for the S3 location specified in `ICEBREAKER_DATA_ROOT`.

Note: These permissions must be granted for the specific S3 bucket and path prefix where your Iceberg tables will be stored (as specified in `ICEBREAKER_DATA_ROOT`). Additionally, the role must have a trust policy that allows it to be assumed using the AWS credentials provided in `ICEBREAKER_CLIENT_ID` and `ICEBREAKER_CLIENT_SECRET`.

## Setup Instructions

### 1. Provision a Warehouse

First, create an Iceberg warehouse in DataHub using the provided script:

```python
from constants import warehouse

# Verify environment variables are set
assert os.environ.get("ICEBREAKER_CLIENT_ID"), "ICEBREAKER_CLIENT_ID not set"
assert os.environ.get("ICEBREAKER_CLIENT_SECRET"), "ICEBREAKER_CLIENT_SECRET not set"
assert os.environ.get("ICEBREAKER_TEST_ROLE"), "ICEBREAKER_TEST_ROLE not set"
assert os.environ.get("ICEBREAKER_DATA_ROOT"), "ICEBREAKER_DATA_ROOT not set"

# Create the warehouse
os.system(f"datahub iceberg create --warehouse {warehouse} \
    --data_root $ICEBREAKER_DATA_ROOT/{warehouse} \
    --client_id $ICEBREAKER_CLIENT_ID \
    --client_secret $ICEBREAKER_CLIENT_SECRET \
    --region 'us-east-1' \
    --role $ICEBREAKER_TEST_ROLE")
```

After provisioning the warehouse, ensure your DataHub user has the following privileges, which were introduced with Iceberg support:
- `DATA_MANAGE_VIEWS_PRIVILEGE`
- `DATA_MANAGE_TABLES_PRIVILEGE`
- `DATA_MANAGE_NAMESPACES_PRIVILEGE`
- `DATA_LIST_ENTITIES_PRIVILEGE`
- `DATA_READ_WRITE`

You can grant these privileges through the DataHub UI under the Policies section.

### 2. Create a Table

You can create Iceberg tables using PyIceberg with a defined schema. Here's an example creating a ski resort metrics table:

```python
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

catalog = load_catalog("local_datahub", warehouse=warehouse, token=graph.config.token)
table = catalog.load_table(f"{namespace}.{table_name}")
con = table.scan().to_duckdb(table_name=table_name)

# Query the data
print("\nResort Metrics Data:")
print("-" * 50)
for row in con.execute(f"SELECT * FROM {table_name}").fetchall():
    print(row)
```

### 5. Manage S3 Storage

The provided folder operations script helps manage S3 storage with features like listing contents and cleaning up data:

```python
def list_s3_contents(
    s3_path: str,
    client_id: str,
    client_secret: str,
    role_arn: str,
    region: str = "us-east-1",
    delimiter: Optional[str] = None,
    nuke: bool = False,
    dry_run: bool = False
) -> None:
    # List contents
    list_s3_contents(
        "s3://your-bucket/path",
        client_id=client_id,
        client_secret=client_secret,
        role_arn=role_arn
    )

    # Preview deletions (dry run)
    list_s3_contents(
        "s3://your-bucket/path",
        client_id=client_id,
        client_secret=client_secret,
        role_arn=role_arn,
        dry_run=True
    )

    # Delete contents
    list_s3_contents(
        "s3://your-bucket/path",
        client_id=client_id,
        client_secret=client_secret,
        role_arn=role_arn,
        nuke=True
    )
```

### 6. Clean Up Resources

To remove tables and clean up resources:

```python
# Drop a table
catalog.drop_table(f"{namespace}.{table_name}")

# Clean up S3 storage
list_s3_contents(
    f"{ICEBREAKER_DATA_ROOT}/{warehouse}",
    client_id=ICEBREAKER_CLIENT_ID,
    client_secret=ICEBREAKER_CLIENT_SECRET,
    role_arn=ICEBREAKER_TEST_ROLE,
    nuke=True
)
```

## Conceptual Mapping

| Iceberg Concept | DataHub Concept | Notes |
|-----------------|-----------------|--------|
| Overall platform | dataPlatform | `iceberg` platform |
| Warehouse | dataPlatformInstance | Stores info such as storage credentials |
| Namespace | container | Hierarchical containers of subtype "Iceberg Namespace" |
| Tables, Views | dataset | Dataset URN is UUID that persists across renames |
| Table/View Names | platformResource | Points to dataset, changes with rename operations |

## Integration with Compute Engines

### Spark Integration

You can connect to the DataHub Iceberg Catalog using Spark SQL:

```sql
./bin/spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=rest \
    --conf spark.sql.catalog.local.uri=http://<GMS_HOST>:<GMS_PORT>/iceberg/ \
    --conf spark.sql.catalog.local.warehouse=demoWarehouse \
    --conf spark.sql.catalog.local.token=<USER_PAT> \
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
                "AWS": "arn:aws:iam::123456789012:user/icebreaker-user"  // The IAM user or role associated with your credentials
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
     -d $S3_DATA_ROOT \
     -i $ICEBERG_CLIENT_ID \
     --client_secret $ICEBERG_CLIENT_ID \
     --region $S3_REGION \
     --role $ICEBERG_ROLE
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
call system.register_table('barTable', 's3://my-s3-bucket/my-data-root/fooNs/barTable/metadata/00000-f9dbba67-df4f-4742-9ba5-745aa2bb4076.metadata.json');
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
  <img width="70%"  src="https://raw.githubusercontent.com/chakru-r/static-assets/b569b7bc6805dd459640cdf44649eb2c75d8c846/imgs/iceberg-policy-type.png"/>
</p>

2. Scope the policies by:
    - Selecting specific warehouse Data Platform Instances for namespace, table management, and listing privileges
    - Selecting specific Datasets for table and view data access privileges
<p>
  <img width="70%"  src="https://raw.githubusercontent.com/chakru-r/static-assets/b569b7bc6805dd459640cdf44649eb2c75d8c846/imgs/iceberg-policy-privileges.png"/>
</p>

3. Assign the policies to relevant users or groups
<p>
  <img width="70%"  src="https://raw.githubusercontent.com/chakru-r/static-assets/b569b7bc6805dd459640cdf44649eb2c75d8c846/imgs/iceberg-policy-users-groups.png"/>
</p>

## Troubleshooting

### Q: How do I verify my warehouse was created successfully?

A: Check the DataHub UI under Data Platform Instances, or try creating and reading a table using the provided scripts.

### Q: What permissions are needed for S3 access?

A: The role specified in `ICEBREAKER_TEST_ROLE` should have permissions to read and write to the S3 bucket specified in `ICEBREAKER_DATA_ROOT`.

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
- Multi-table transactions are not yet supported by Spark integration
- Table operations like purge are not yet implemented
- Metric collection and credential refresh mechanisms are still in development
- External tables have limited support for Policy Tags

## Future Enhancements

- Improved concurrency with multiple replicas of GMS
- Support for Iceberg multi-table transactional `/commit`
- Additional table APIs (scan, drop table purge)
- Azure and GCP Support
- Namespace permissions
- Enhanced metrics and credential refresh
- Unity Catalog API compatible implementation

## Related Documentation

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [DataHub Documentation](https://datahubproject.io/docs/)