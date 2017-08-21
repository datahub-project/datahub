# WhereHows ETL jobs

This directory contains the code for all the ETL jobs that WhereHows can run.

1. [HDFS Dataset](#hdfs-dataaset)
1. [Teradata Dataset](#teradata-dataaset)
1. [Oracle Dataset](#oracle-dataaset)
1. [Hive Dataset](#hive-dataset)
1. [Azkaban Execution](#azkaban-execution)
1. [Oozie Execution](#oozie-execution)
1. [Appworx Execution](#appwox-execution)
1. [Azkaban Lineage](#azkaban-lineage)
1. [Appworx Lineage](#appworx-lineage)
1. [Elastic Search](#elastic-search)
1. [LDAP Directory](#ldap-directory)
1. [Druid Dataset](#druid-dataset)

The ETL jobs are scheduled and configured via job files. Please refer to [this directory](../wherehows-backend/jobs) for more details. 

## HDFS Dataset

Collect dataset metadata from HDFS.

[Job file template](../wherehows-backend/jobs/templates/HDFS_METADATA_ETL.job)

### Extract
Major related file: [wherehows-hadoop](../wherehows-hadoop)

The standalone module 'wherehows-hadoop' is responsible for this process.

In a real production environment, the machine used to run this ETL job is usually different from the machine that is the gateway to Hadoop. So we need to copy the runnable JAR file to a remote machine, execute remotely, and copy back the result.

At compile time, the module `wherehows-hadoop` is packaged into a standalone JAR file.
At runtime, a Jython script copies it to the Hadoop gateway, remotely runs it on the Hadoop gateway, and copies the result back.

Inside the module, we use a whitelist of folders (configured through parameters) as the starting point
to scan through the folders and files. After abstract at the dataset level, we then extract schema, sample data,
and related metadata from them. The final step is to store this into two result files: metadata file and
sample data file.

### Transform
Major related file: [HdfsTransform.py](src/main/resources/jython/HdfsTransform.py)

Transform the JSON output into CSV format for easy loading.

### Load
Major related file: [HdfsLoad.py](src/main/resources/jython/HdfsLoad.py)

Load into MySQL database.
related tables : dict_dataset, dict_dataset_sample, dict_field_detail


## Teradata Dataset

Collect dataset metadata from Teradata.

[Job file template](../wherehows-backend/jobs/templates/TERADATA_METADATA_ETL.job)

### Extract
Major related file: [TeradataExtract.py](src/main/resources/jython/TeradataExtract.py)

Get metadata from Teradata DBC databases and store it in a local JSON file.

Major source tables: DBC.Tables, DBC.Columns, DBC.ColumnStatsV, DBC.TableSize, DBC.Indieces, DBC.IndexConstraints

'Select top 10' to get sample data.

### Transform
Major related file: [TeradataTransform.py](src/main/resources/jython/TeradataTransform.py)

Transform the JSON output into CSV format for easy loading.

### Load
Major related file: [TeradataLoad.py](src/main/resources/jython/TeradataLoad.py)

Load into MySQL database.

Related tables: dict_dataset, dict_field_detail, dict_dataset_sample


## Oracle Dataset

Collect dataset metadata from Oracle DB.

[Job file template](../wherehows-backend/jobs/templates/ORACLE_METADATA_ETL.job)

### Extract
Major related file: [OracleExtract.py](src/main/resources/jython/OracleExtract.py)

Connect to Oracle database to get all the table/column/comments information excluding the databases in the exclude list. Extra table information including indices, constraints and partitions are also fetched. The results are formatted and stored in two CSV files, one for table records and the other for field records.

Major source tables: ALL_TABLES, ALL_TAB_COLUMNS, ALL_COL_COMMENTS, ALL_INDEXES, ALL_IND_COLUMNS, ALL_CONSTRAINTS, ALL_PART_KEY_COLUMNS

### Transform
Not needed.

### Load
Major related file: [OracleLoad.py](src/main/resources/jython/OracleLoad.py)

Load into MySQL database, similar to HiveLoad or HdfsLoad.

Related tables: dict_dataset, dict_field_detail, dict_dataset_sample


## Hive Dataset

Collect dataset metadata from Hive.

[Job file template](../wherehows-backend/jobs/templates/HIVE_METADATA_ETL.job)

### Extract
Major related file: [HiveExtract.py](src/main/resources/jython/HiveExtract.py)

Connect to Hive Metastore to get the Hive table/view information and store it in a local JSON file.

Major source tables: COLUMNS_V2, SERDE_PARAMS

### Transform
Major related file: [HiveTransform.py](src/main/resources/jython/HiveTransform.py)

Transform the JSON output into CSV format for easy loading.

### Load
Major related file: [HiveLoad.py](src/main/resources/jython/HiveLoad.py)

Load into MySQL database.

Related tables: dict_dataset


## Azkaban Execution

Collect Azkaban execution information, including Azkaban flows/jobs definitions, DAGs, executions, owners, and schedules.

[Job file template](../wherehows-backend/jobs/templates/AZKABAN_EXECUTION_METADATA_ETL.job)

### Extract
Major related file: [AzkabanExtract.py](src/main/resources/jython/AzkabanExtract.py)

Connect to Azkaban MySQL database, collect metadata, and store in local file.

Major source tables from Azkaban database: project_flows, execution_flows, triggers, project_permissions

### Transform
Major related file: [AzkabanTransform.py](src/main/resources/jython/AzkabanTransform.py), [SchedulerTransform.py](src/main/resources/jython/SchedulerTransform.py)

Transform the JSON output into CSV format.

### Load
Major related file: [AzkabanLoad.py](src/main/resources/jython/AzkabanLoad.py), [SchedulerLoad.py](src/main/resources/jython/SchedulerLoad.py)

Load into MySQL database.
Major related tables: flow, flow_job, flow_dag, flow_schedule, flow_owner_permission, flow_execution, job_execution


## Oozie Execution

Collect Oozie execution information, including workflow/coordinate jobs/actions dags, executions, owners and schedules.

[Job file template](../wherehows-backend/jobs/templates/OOZIE_EXECUTION_METADATA_ETL.job)

### Extract
Major related file: [OozieExtract.py](src/main/resources/jython/OozieExtract.py)

Connect to Oozie MySQL database, collect metadata, and store in local file.

Major source tables from Oozie database: WF_JOBS, WF_ACTIONS, COORD_JOBS, COORD_ACTIONS

### Transform
Major related file: [OozieTransform.py](src/main/resources/jython/OozieTransform.py), [SchedulerTransform.py](src/main/resources/jython/SchedulerTransform.py)

Transform the JSON output into CSV format.

### Load
Major related file: [OozieLoad.py](src/main/resources/jython/OozieLoad.py), [SchedulerLoad.py](src/main/resources/jython/SchedulerLoad.py)

Load into MySQL database.
Major related tables: flow, flow_job, flow_dag, flow_schedule, flow_owner_permission, flow_execution, job_execution


## Appworx Execution

TODO: add doc

[Job file template](../wherehows-backend/jobs/templates/APPWORX_EXECUTION_METADATA_ETL.job)


## Azkaban Lineage

Get Lineage Information for jobs that schedule through Azkaban See [architecture](../wherehows-docs/architecture.md) for high level view of this process.

Major related file: [lineage](src/main/java/metadata/etl/lineage)

[Job file template](../wherehows-backend/jobs/templates/AZKABAN_LINEAGE_METADATA_ETL.job)

### Security 
In this lineage ETL process, you will need to communicate with hadoop job history nodes. If you have hadoop security setup, you need to make sure you have the right permission. There are templates of [krb5.conf](src/main/resources/krb5.conf) and [gss-jaas.conf](src/main/resources/gss-jaas.conf) files in our repository. You need to put them in the right location (one of the following : running directory, $WH_HOME, $USER_HOME/.kerboros, /var/tmp/.kerboros). 
You may also use other authorization method as you needed.

### Content Related Configuration
ETL jobs not only depend on the source system’s data model, but sometimes also depend on the source system’s data content. For example, LinkedIn heavily uses the hour glass framework: [https://engineering.linkedin.com/datafu/datafus-hourglass-incremental-data-processing-hadoop](https://engineering.linkedin.com/datafu/datafus-hourglass-incremental-data-processing-hadoop) to do incremental processing in Hadoop. Because we are mining some lineage and operation information from logs, we need to specify the log format in our system to capture that information.

In the AZKABAN_LINEAGE_METADATA_ETL job type, we need to use the following APIs to add configurations for the patterns we want to extract.
* Add filename pattern
  - Example: *(.)/part-m-\d+.avro*
  - Usage: filename’s pattern to abstract from file level to directory level
  - API: [Add a filename pattern](https://github.com/linkedin/WhereHows/wiki/Backend-API#filename-add)

* Add dataset partition layout pattern
  - Example: *(.)\/daily\/(\d{4}\/\d{2}\/\d{2}).*
  - Usage: partitions pattern to abstract from partition level to dataset level
  - API: [Add a dataset partition pattern](https://github.com/linkedin/WhereHows/wiki/Backend-API#dataset-partition-pattern-add)

* Add log lineage pattern
  - Example:  *Moving \S to (\/(?!tmp)[\S])*
  - Usage: log lineage pattern to extract lineage from logs
  - API: [Add a lineage log pattern](https://github.com/linkedin/WhereHows/wiki/Backend-API#log-lineage-pattern-add)

* Add log reference job ID pattern
  - Example : *INFO - Starting Job = (job\d+\d+)*
  - Usage: patterns used to discover the Hadoop map-reduce job ID inside the log
  - API: [Add a job ID log pattern](https://github.com/linkedin/WhereHows/wiki/Backend-API#log-job-id-pattern-add)


## Appworx Lineage

TODO: add doc

[Job file template](../wherehows-backend/jobs/templates/APPWORX_EXECUTION_METADATA_ETL.job)


## Elastic Search

TODO: add doc

[Job file template](../wherehows-backend/jobs/templates/ELASTIC_SEARCH_ETL.job)

## LDAP Directory

This is an optional feature.

Almost every enterprise has an LDAP server. This information is essential for any metadata containing an LDAP ID. For example, if we have the owner ID of a dataset, we also would like to know more about the owner: email address, phone number, manager, department, etc. All that information comes from the LDAP server.

[Job file template](../wherehows-backend/jobs/templates/LDAP_USER_ETL.job)

### Extract
Major related files: [LdapExtract.py](src/main/resources/jython/LdapExtract.py)

There are two major parts at this stage: individual information and group/user mapping.

* For an individual user, extract the following standardized attributes from the LDAP server:

    'user_id', 'distinct_name', 'name', 'display_name', 'title', 'employee_number', 'manager', 'mail', 'department_number', 'department', 'start_date', 'mobile'

    You can specify the actual LDAP return attributes in the job properties. 

* For a group account, find all the users in the group. Because a group can contain a subgroup, two mapping files are stored, one raw mapping file, and one flattened mapping file that will find all users in the subgroup.

### Transform
Major related files: [LdapTransform.py](src/main/resources/jython/LdapTransform.py)

Additional derived information is added during the transform stage: hierarchy. The path from the top management to the specified user is generated by recursively looking for manager until it reaches the CEO.

### Load
Major related files: [LdapLoad.py](src/main/resources/jython/LdapLoad.py)

Loading from staging table into final table.

## Druid Dataset

Collect dataset metadata from Druid.

[Job file template](../wherehows-backend/jobs/templates/DRUID_METADATA_ETL.job)

### Extract
Major related file: [DruidMetadataExtractor.java](src/main/java/metadata/etl/dataset/druid/DruidMetadataExtractor.java)

Get metadata of data sources from Druid cluster and store into local JSON files.

### Load
Major related file: [DruidMetadataLoader.java](src/main/java/metadata/etl/dataset/druid/DruidMetadataLoader.java)

Loading from local JSON files to staging tables and finally into final table.

Related tables: dict_dataset, dict_field_detail

