> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](https://github.com/linkedin/WhereHows/blob/master/wherehows-etl/) for the latest version.

Get Lineage Information for jobs that schedule through Azkaban

## Configuration
List of properties required for the ETL process:

| configuration key | description   |
|---                |---            |
|az.db.driver       |Azkaban database driver, e.g., com.mysql.jdbc.Driver|
|az.db.jdbc.url     |Azkaban database JDBC URL (not including username and password), e.g., jdbc:mysql://localhost:3306/azkaban|
|az.db.password     |Azkaban database password|
|az.db.username     |Azkaban database username|
|az.default.hadoop.database.id|default Hadoop cluster corresponding to this Azkaban instance|
|az.hadoop.jobhistory.server.url|job history server used to get the job configuration file., e.g., http://localhost:19888/ws/v1/history/mapreduce/jobs|
|az.lineage.actor.timeout|timeout for each thread, in seconds|
|az.lineage_etl.lookback_period.in.minutes|lookback period in minutes for the jobs that will process for lineage mining|
|az.server.password|optional. Azkaban server password to get Azkaban log. (default get from Azkaban db)|
|az.server.url|optional. Azkaban server url to get Azkaban log. (default get from Azkaban db)|
|az.server.username|optional. Azkaban server username to get Azkaban log. (default get from Azkaban db)|
|krb5.kdc|optional. used to log in to Hadoop namenode service (if you use Kerberos)|
|krb5.realm|optional. used to log in to Hadoop namenode service (if you use Kerberos)|

See Architecture for high level view of this process.

Major related file: [lineage](https://github.com/linkedin/WhereHows/tree/master/metadata-etl/src/main/java/metadata/etl/lineage)

## Security 
In this lineage ETL process, you will need to communicate with hadoop job history nodes. If you have hadoop security setup, you need to make sure you have the right permission. There are templates of [krb5.conf](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/resources/krb5.conf) and [gss-jaas.conf](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/resources/gss-jaas.conf) files in our repository. You need to put them in the right location (one of the following : running directory, $WH_HOME, $USER_HOME/.kerboros, /var/tmp/.kerboros). 
You may also use other authorization method as you needed.

## Content Related Configuration
ETL jobs not only depend on the source system’s data model, but sometimes also depend on the source system’s data content. For example, LinkedIn heavily uses the hour glass framework: [https://engineering.linkedin.com/datafu/datafus-hourglass-incremental-data-processing-hadoop](https://engineering.linkedin.com/datafu/datafus-hourglass-incremental-data-processing-hadoop) to do incremental processing in Hadoop. Because we are mining some lineage and operation information from logs, we need to specify the log format in our system to capture that information.

In the AZKABAN_LINEAGE_METADATA_ETL job type, we need to use the following APIs to add configurations for the patterns we want to extract.
* Add filename pattern
  - Example: *(.)/part-m-\d+.avro*
  - Usage: filename’s pattern to abstract from file level to directory level
  - API: [Add a filename pattern](Backend-API#filename-add)

* Add dataset partition layout pattern
  - Example: *(.)\/daily\/(\d{4}\/\d{2}\/\d{2}).*
  - Usage: partitions pattern to abstract from partition level to dataset level
  - API: [Add a dataset partition pattern](Backend-API#dataset-partition-pattern-add)

* Add log lineage pattern
  - Example:  *Moving \S to (\/(?!tmp)[\S])*
  - Usage: log lineage pattern to extract lineage from logs
  - API: [Add a lineage log pattern](Backend-API#log-lineage-pattern-add)

* Add log reference job ID pattern
  - Example : *INFO - Starting Job = (job\d+\d+)*
  - Usage: patterns used to discover the Hadoop map-reduce job ID inside the log
  - API: [Add a job ID log pattern](Backend-API#log-job-id-pattern-add)
