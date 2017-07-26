This directory contains configurations for both ETL and non-ETL jobs. At launch the backend service loads all `.job` files at the root directory level and runs or schedules jobs accordingly. The syntax of job files are identical to Java's properties file, with the added support for resolving environmental variables using the `${var_name}` syntax.

Example job files are placed in the [templates](templates) directory. Make sure to change them to match your environment. Each job is uniquely identified in the database based on its job filename.

For each ETL job, the job file should contain the following common configurations,

Configuration Key | Description
--- | ---
job.class | The fully-qualified-class-name (FQCN) for the ETL job
job.cron.expr | A [cron expression](https://docs.oracle.com/cd/E12058_01/doc/doc.1014/e12030/cron_expressions.htm) for the job 
job.timeout | Seconds to wait before killing the job
job.cmd.params | Optional extra command line arguments to pass to the job 
job.cmd.disabled | If defined, regardless of it value, the job will not be run
job.ref.id | A unique reference ID for the job (To be removed in the future)