This directory contains configurations for both ETL and non-ETL jobs. At launch the backend service loads all `.job` files at the root directory level and runs or schedules jobs accordingly. The syntax of job files are identical to Java's properties file, with the added support for resolving environmental variables using the `${var_name}` syntax.

Example job files are placed in the [templates](templates) directory. Make sure to change them to match your environment. Each job is uniquely identified in the database based on its job filename.

For each ETL job, the job file should contain the following common configurations.

Configuration Key | Description
--- | ---
job.class | The fully-qualified-class-name (FQCN) for the ETL job
job.cron.expr | A [cron expression](https://docs.oracle.com/cd/E12058_01/doc/doc.1014/e12030/cron_expressions.htm) for the job 
job.timeout | Seconds to wait before killing the job
job.cmd.params | Optional extra command line arguments to pass to the job 
job.cmd.disabled | If defined, regardless of it value, the job will not be run
job.ref.id | A unique reference ID for the job (To be removed in the future)

For ETL specific job configurations, we can define the same key with multiple values in different lines.
These values will be combined into a single comma-sepearated string.

Configruation Key | multiple values
--- | ---
job.databases | table1
job.databases | table2
job.databases | table3

For pure Jython-based ETL jobs, set `job.class` to [metadata.etl.JythonEtlJob](https://github.com/linkedin/WhereHows/blob/master/wherehows-etl/src/main/java/metadata/etl/JythonEtlJob.java) and simply set the path to your ETL scripts in the [resources](https://github.com/linkedin/WhereHows/tree/master/wherehows-etl/src/main/resources) folder.

Configuration Key | Description
--- | ---
job.jython.extract | Path to your Extract Jython script. No extract is performed if not set.
job.jython.transform | Path to your Transform Jython script. No transform is performed if not set.
job.jython.load | Path to your Load Jython script. No load is performed if not set.
