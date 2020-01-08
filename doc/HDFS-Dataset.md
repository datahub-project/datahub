> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](https://github.com/linkedin/WhereHows/blob/master/wherehows-etl/) for the latest version.

Collect dataset metadata from HDFS.

## Configuration
List of properties in the wh_etl_job_property table that are required for the Hadoop dataset ETL process:

| configuration key | description|
|---|---|
| hdfs.cluster | cluster name|
| hdfs.remote.machine | remote Hadoop gateway machine name (could be localhost) |
|hdfs.private_key_location | private key used to log in to the remote machine |
|hdfs.remote.jar|JAR file location on the remote machine|
|hdfs.remote.user|user login on remote machine|
|hdfs.remote.raw_metadata|metadata JSON file location on remote machine|
|hdfs.remote.sample|sample data CSV file location on remote machine|
|hdfs.local.field_metadata|place to store field metadata file|
|hdfs.local.metadata|place to store metadata CSV file|
|hdfs.local.raw_metadata|place to store metadata JSON file|
|hdfs.local.sample|place to store sample file|
|hdfs.white_list|the whitelist of folder to collect metadata|
|hdfs.num_of_thread|optional. number of threads you want to scrape the HDFS|
|hdfs.file_path_regex_source_map|The map of file path regex and dataset source. e.g. [{"/data/tracking.*":"Kafka"},{"/data/retail.*":"Teradata"}]


## Extract
Major related file: [hadoop-dataset-extractor-standalone](https://github.com/linkedin/WhereHows/tree/master/hadoop-dataset-extractor-standalone)

The standalone module 'hadoop-dataset-extractor-standalone' is responsible for this process.

In a real production environment, the machine used to run this ETL job is usually different from the machine that is the gateway to Hadoop. So we need to copy the runnable JAR file to a remote machine, execute remotely, and copy back the result.

At compile time, this module `hadoop-dataset-extractor-standalone` packages into a standalone JAR file.
At runtime, a Jython script copies it to the Hadoop gateway, remotely runs it on the Hadoop gateway, and copies the result back.

Inside the module, we use a whitelist of folders (configured through parameters) as the starting point
to scan through the folders and files. After abstract at the dataset level, we then extract schema, sample data,
and related metadata from them. The final step is to store this into two result files: metadata file and
sample data file.

## Transform
Major related file: [HdfsTransform.py](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/resources/jython/HdfsTransform.py)

Transform the JSON output into CSV format for easy loading.

## Load
Major related file: [HdfsLoad.py](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/resources/jython/HdfsLoad.py)

Load into MySQL database.
related tables : dict_dataset, dict_dataset_sample, dict_field_detail
