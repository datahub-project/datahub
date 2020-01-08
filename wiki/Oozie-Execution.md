> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](../wherehows-etl/README.md) for the latest version.

Collect Oozie execution information, including workflow/coordinate jobs/actions dags, executions, owners and schedules.

## Configuration
List of properties required for the ETL process:

| configuration key | description   |
|---                |---            |
|oz.db.driver       |Oozie database driver, e.g., com.mysql.jdbc.Driver|                          
|oz.db.jdbc.url     |oozie database jdbc url (not including username and password), e.g., jdbc:mysql://localhost:3306/oozie|
|oz.db.password     |Oozie database password|
|oz.db.username     |Oozie database username|
|oz.exec_etl.lookback_period.in.minutes|lookback period in minutes for executions|


## Extract
Major related file: [OozieExtract.py](../wherehows-etl/src/main/resources/jython/OozieExtract.py)

Connect to Oozie MySQL database, collect metadata, and store in local file.

Major source tables from Oozie database: WF_JOBS, WF_ACTIONS, COORD_JOBS, COORD_ACTIONS

## Transform
Major related file: [OozieTransform.py](../wherehows-etl/src/main/resources/jython/OozieTransform.py), [SchedulerTransform.py](../wherehows-etl/src/main/resources/jython/SchedulerTransform.py)

Transform the JSON output into CSV format.

## Load
Major related file: [OozieLoad.py](../wherehows-etl/src/main/resources/jython/OozieLoad.py), [SchedulerLoad.py](../wherehows-etl/src/main/resources/jython/SchedulerLoad.py)

Load into MySQL database.
Major related tables: flow, flow_job, flow_dag, flow_schedule, flow_owner_permission, flow_execution, job_execution
