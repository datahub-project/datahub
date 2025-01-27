import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Iceberg REST Catalog

<FeatureAvailability/>

> Note that this feature is currently in open **Beta**. With any questions or issues, please reach out to your Acryl 
> representative.

> Open Source DataHub: 1.0.0

## Introduction

DataHub implements the Apache Iceberg REST Catalog specification. This enables DataHub  to 
function as an Iceberg Catalog when using any compute engine that supports Apache Iceberg (for example Apache Spark, 
DuckDB). 

When using DataHub as the Iceberg Catalog, the metadata for all Iceberg Tables, Views and Namespaces that are created 
using a supported compute engine is collected and kept up-to-date.

This capability also enables Iceberg data administrators to use DataHub as the single point for access control 
administration 
for users for multiple compute engines with Iceberg.

The currently supported compute engines are Spark <version> and DuckDB <version> with the storage being on AWS S3

This guide will describe the steps required to configure a Iceberg Warehouse, setting up users with permissions to use
warehouse, and configuring compute engines to use DataHub as the catalog to read and write to that warehouse.

<!-- This section should provide a plain-language overview of feature. Consider the following:

* What does this feature do? Why is it useful?
* What are the typical use cases?
* Who are the typical users?
* In which DataHub Version did this become available? -->

## Setting up Iceberg Catalog

### Pre-requisites

To setup an Iceberg Warehouse, the following resources need to be provisioned

- An AWS S3 location that is used as the data root to store all Iceberg Tables that will be created when using this warehouse
- AWS Credentials - AWS Access Key and Secret Access Key that is used assume the configured AWS Role and generate
    temporary session credentials. These credentials need to be long term credentials (and specifically, not short term
    credentials).
- An AWS role that has two policies
  - A permissions policy that allows it read and write to that S3 location. 
  - A trust policy that enables this role to be assumed using the AWS configured AWS credentials. DataHub generates temporary credentials for 
  using this role and provides them to compute engines for performing all read/writes to the warehouse location.

### Setting up the warehouse

Datahub provides warehouse configuration capabilities via the `datahub iceberg` CLI. 

To create a warehouse, run
```commandline
datahub iceberg create -w $WAREHOUSE_NAME -d $S3_DATA_ROOT -i $ICEBERG_CLIENT_ID --client_secret $ICEBERG_CLIENT_ID --region $S3_REGION --role $ICEBERG_ROLE
```
When an Iceberg warehouse is created, an Iceberg Data Platform Instance is created using the warehouse name.
This command validates the credentials/role provided and creates the warehouse only if the credentials/role are valid. 

### Create Users
Create one or more users as needed for performing iceberg operations. For each iceberg user, generate a Personal Access Token. 
The personal access token is used when configuring the compute engine to authenticate itself to DataHub to access the warehouse.

### Create Policy for use with Iceberg Users 

Create one or more Policies that will be used for Iceberg Users with suitable privileges. For Iceberg Support, new  privileges
"Read only data-access", "Read-write data-access", "List tables, views and Namespaces", "Manage namespaces", "Manage Tables", "Manage Views" are now supported and can be 
assigned to the policies meant for Iceberg Users. 

The policy can further be restricted to specific warehouse and or tables by selecting the warehouse Data Platform Instance and/or DataSets

| Operations                               | Privilege                              |
|------------------------------------------|----------------------------------------|
| CREATE or DROP namespaces                | Manage Namespaces                      |
| CREATE, ALTER or DROP tables             | Manage Tables                          |
| CREATE, ALTER or DROP views              | Manage Views                           |
| SELECT from tables or views              | Read Only data-access                  |
| INSERT, UPDATE, DELETE or ALTER tables   | Read-write data-access                 |
| List tables or views                     | List tables, views and namespaces      |                                        

Assign the policy to the relevant Iceberg users or groups.


### Running the compute engine

This section will use Apache Spark as the compute engine to show how the DataHub catalog is configured. 

To start spark, the following spark configs must be specified during launch. 

```commandline
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.rest_prod=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.rest_prod.type=rest \
    --conf spark.sql.catalog.rest_prod.header.X-Iceberg-Access-Delegation=vended-credentials \
    --conf spark.sql.catalog.rest_prod.rest-metrics-reporting-enabled=false \
    --conf spark.sql.catalog.rest_prod.uri=http://${DATAHUB_HOST}:{$DATAHUB_PORT}/iceberg/ \
    --conf spark.sql.catalog.rest_prod.warehouse=${WAREHOUSE_NAME} \
    --conf spark.sql.catalog.rest_prod.token=$USER_TOKEN
```
`DATAHUB_HOST` and `DATAHUB_PORT` contain the datahub host name and port respectively. `WAREHOUSE_NAME` contains the warehouse
that was previously configured on which the read/write operations are to be performed
`USER_TOKEN` contains the personal access token of the user used to authenticate with datahub. This spark session will run
with the privileges as configured in the policy assiged with this user. 
The word rest_prod is the name of the catalog being configured here and can be any name to represent the catalog in spark.

As an example, the following shows a few examples to perform some iceberg operations.

Create a namespace
```commandline
create namespace rest_prod.sample_ns;
```

Create a table in this namespace
```
create table rest_prod.sample_ns.demo_table (id integer, name string); 
```

Insert some data into the table
```commandline
insert into rest_prod.sample_ns.demo_table values (1, "foo"), (2, "bar");

```
View the data in this table
```commandline
select * from rest_prod.sample_ns.demo_table;
```

As and when Tables are created, DataSets corresponding to each of these tables are created with that table metadata. No 
explicit ingestion is required to be setup to ingest the iceberg table metadata for tables when using Datahub Iceberg 
REST Catalog.

A few other notes:
1. A namespace must be created before any tables are created.
2. To avoid having to prefix the catalog name and namespace in all SQL statements, it may be helpful to start spark with 
```
--conf spark.sql.defaultCatalog=<default-catalog-name> --conf spark.sql.catalog.local.default-namespace=<default-namespace>
```

3. The datahub CLI provides a few additional options to help with managing warehouses

List configured warehouses
```commandline 
datahub iceberg list
```

Update warehouse config
```
datahub iceberg update -w $WAREHOUSE_NAME -d $S3_DATA_ROOT -i $ICEBERG_CLIENT_ID --client_secret $ICEBERG_CLIENT_ID --region $S3_REGION --role $ICEBERG_ROLE
```

Delete a warehouse (also deletes Containers associated with Namespaces and Datasets associated with Tables and Views in this warehouse.
Note, this does not delete any backing data in the warehouse data_root location. This only deletes entries from the catalog.
```
datahub iceberg delete -w $WAREHOUSE_NAME   
```

For more help on options, run
```commandline
datahub iceberg [command] --help
```

## Additional Resources


### Videos


### DataHub Blog


## FAQ and Troubleshooting


### Related Features

