> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](../wherehows-backend/jobs/README.md) for the latest version.

As metadata ETL jobs highly depend on the source systems' data models, and the difference between various models sometimes is significant, most ETL parts have to be rewritten to use in new source systems. In our design, we try to minimize any work that needs to be done for integrating a new system.

This document provides step-by-step instructions for how you can integrate a new source system into WhereHows. There are several build-in ETL types, which only require that you configure connections and the running environment information, then you can get started. You can also push the data through the API or even create new ETL job types (refer to the [Integration Guide](Integration-Guide.md)) as they needed.

# Built-In Metadata ETL Types

Currently, we support metadata collections over a few widely used systems. Here is an overview of built-in metadata ETL types:

|Type|System|WhereHows ETL Job Name|Comments|
|---|---|---|---|
|Dataset|Hadoop|HADOOP_DATASET_METADATA_ETL|support whitelist of datasets|
| |Teradata|TERADATA_DATASET_METADATA_ETL|all tables|
| |Oracle|ORACLE_DATASET_METADATA_ETL|all tables|
|Execution|Azkaban|AZKABAN_EXECUTION_METADATA_ETL|multi-instance|
| |Oozie|OOZIE_EXECUTION_METADATA_ETL|multi-instance|
|Lineage|Azkaban|AZKABAN_LINEAGE_METADATA_ETL|multi-instance|

<a name="step-by-step">

# Step-by-Step Instructions

#### 1. Add the Application/Database.

Register the application/database using the following APIs:
One application represent either a scheduler system or a execution system, such as Azkaban, Oozie.
One database represent a storage system, such as HDFS, Teradata.
We first need to add these info in dictionary tables.

- Application: [Add a new application](Backend-API.md#application-postput-api)
- Database: [Add a new database](Backend-API.md#database-postput-api)

#### 2. Fill in all the configurations needed for the job type.
- Make sure basic configurations for all wherehows jobs is already there (in wh_property table): 

| property name| description|
|---|---|
|`wherehows.app_folder`| the folder to store temporary files|
|`wherehows.db.driver`| driver class. e.g. `com.mysql.jdbc.Driver`|
|`wherehows.db.jdbc.url`| url to connect to database. e.g. `jdbc:mysql://host_name/wherehows`|
|`wherehows.db.password`| password|
|`wherehows.db.username`| username|
|`wherehows.encrypt.master.key.loc`| the key file to do the encryption of password|
|`wherehows.ui.tree.dataset.file`| dataset tree json file |
|`wherehows.ui.tree.flow.file`| flow tree json file |

- Add the job configurations through the [ETL job property API](Backend-API.md#etl-job-property-update-put-api) as stated in each types pages in `Metadata ETL types` section.
- Some ETL jobs need extra configurations setting. For example, lineage ETL needs adding content related configurations through additional APIs: [Add a filename](Backend-API.md#filename-pattern-post-api), [Add a dataset partition pattern](Backend-API.md#dataset-partition-pattern-post-api), and [Add a log lineage pattern](Backend-API.md#log-lineage-pattern-post-api) as stated in [Lineage page](Lineage.md).
- Some of the properties need encryption. You need to place a file contained your master under a location and configure it through ```wherehows.encrypt.master.key.loc``` in ```wh_property table```. Default location is ```~/.wherehows/master_key```.  This encryption key file is not checked in to GitHub or included in Play distribution zip file. Please maintain this file manually and keep the file system permission as rw------- or 0600.
Then you can use this [etl-job-property API](Backend-API.md#etl-job-property-update-put-api) to insert encrypted properties.

#### 3. Schedule the Metadata ETL Job.
Submit a scheduled metadata collection ETL job using the API:
- [Add a new ETL job](Backend-API.md#etl-job-new-job-post-api)


We try to parameterize most part of the ETL rules, for some other parts, either because their pattern is not general enough, or there is too much and it is too trivial to be customized, we didn’t abstract them out to table configuration. Instead, you need to change Jython scripts to fit their requirements.