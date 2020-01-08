> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](../wherehows-etl/README.md) for the latest version.

Collect Azkaban execution information, including Azkaban flows/jobs definitions, DAGs, executions, owners, and schedules.

## Configuration
List of properties required for the ETL process:

| configuration key | description   |
|---                |---            |
| az.db.driver      |Azkaban database driver, e.g., com.mysql.jdbc.Driver|
| az.db.jdbc.url    | Azkaban database JDBC URL (not including username and password), e.g., jdbc:mysql://localhost:3306/azkaban|
|az.db.password     | Azkaban database password|
|az.db.username     |Azkaban database username|
|az.exec_etl.lookback_period.in.minutes|lookback period in minutes for executions|


## Extract
Major related file: [AzkabanExtract.py](../wherehows-etl/src/main/resources/jython/AzkabanExtract.py)

Connect to Azkaban MySQL database, collect metadata, and store in local file.

Major source tables from Azkaban database: project_flows, execution_flows, triggers, project_permissions

## Transform
Major related file: [AzkabanTransform.py](../wherehows-etl/src/main/resources/jython/AzkabanTransform.py), [SchedulerTransform.py](../wherehows-etl/src/main/resources/jython/SchedulerTransform.py)

Transform the JSON output into CSV format.

## Load
Major related file: [AzkabanLoad.py](../wherehows-etl/src/main/resources/jython/AzkabanLoad.py), [SchedulerLoad.py](../wherehows-etl/src/main/resources/jython/SchedulerLoad.py)

Load into MySQL database.
Major related tables: flow, flow_job, flow_dag, flow_schedule, flow_owner_permission, flow_execution, job_execution
