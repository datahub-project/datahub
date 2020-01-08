# Table of Contents
1. <a href="#dataset-get">Dataset GET API</a>
1. <a href="#dataset-post">Dataset POST API</a>
1. <a href="#lineage-job-get">Downstream/Upstream Jobs GET API</a>
1. <a href="#lineage-dataset-get">Downstream/Upstream Datasets GET API</a>
1. <a href="#lineage-post">Job Lineage POST API</a>
1. <a href="#dataset-dependency">Dataset Dependency GET API</a>
1. <a href="#flow-owner">Flow Owner GET API</a>
1. <a href="#flow-schedule">Flow Schedule GET API</a>
1. <a href="#filename-get">Filename Pattern GET API</a>
1. <a href="#filename-add">Filename Pattern POST API</a>
1. <a href="#dataset-partition-pattern-get">Dataset Partition Pattern GET API</a>
1. <a href="#dataset-partition-pattern-add">Dataset Partition Pattern POST API</a>
1. <a href="#log-lineage-pattern-get">Log Lineage Pattern GET API</a>
1. <a href="#log-lineage-pattern-add">Log Lineage Pattern POST API</a>
1. <a href="#log-job-id-pattern-get">Log Job Id Pattern GET API</a>
1. <a href="#log-job-id-pattern-add">Log Job Id Pattern POST API</a>
1. <a href="#cfg-app">Application GET API</a>
1. <a href="#cfg-app-add">Application POST/PUT API</a>
1. <a href="#cfg-db">Database GET API</a>
1. <a href="#cfg-db-add">Database POST/PUT API</a>
1. <a href="#etl-job-get">ETL Job GET API</a>
1. <a href="#etl-job-post">ETL Job New Job POST API</a>
1. <a href="#etl-job-control">ETL Job Control PUT API</a>
1. <a href="#etl-job-schedule">ETL Job Schedule Update PUT API</a>
1. <a href="#etl-job-property">ETL Job Property Update PUT API</a>


<a name="dataset-get" />

## Dataset GET API
Return information about dataset by dataset ID or URN

* **URL**

  /dataset

* **Method:**

  `GET`

* **Data Params**

   **Required:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| datasetId | Dataset ID |  | Y|

   or

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| urn | URN of dataset |  | Y|

* **Success Response:**
```json
{
  "return_code": "200",
  "dataset": {
    "id": 8011,
    "name": "DUMMY4",
    "schema": "{\"name\": \"DUMMY\", \"fields\": [{\"accessCount\": null, \"lastAccessTime\": null, \"nullable\": \"Y\", \"format\": null, \"type\": \"INT\", \"maxByteLength\": 4, \"name\": \"DUMMY\", \"doc\": \"\"}]}\",\"properties\" : \"{\"storage_type\": \"View\", \"accessCount\": 2670, \"lastAccessTime\": null, \"sizeInMbytes\": null, \"referenceTables\": [\"DWH_DIM.DUMMY\"], \"viewSqlText\": \"REPLACE VIEW DWH_STG.DUMMY AS\nLOCKING ROW FOR ACCESS\n SELECT * \n   FROM DWH_DIM.DUMMY;\", \"createTime\": \"2015-03-06 10:43:58\", \"lastAlterTime\": \"2015-03-10 20:57:16\"}",
    "properties": null,
    "fields": "{\"DUMMY\": {\"type\": \"INT\", \"maxByteLength\": 4}}",
    "urn": "teradata:///DWH_TMP/DUMMY4",
    "source": "Teradata",
    "modified": 1426003036000,
    "created": null,
    "location_prefix": "DWH_STG",
    "parent_name": null,
    "storage_type": null,
    "ref_dataset_id": 1000000,
    "status_id": 0,
    "schema_type": "JSON",
    "dataset_type": null,
    "hive_serdes_class": null,
    "is_partitioned": null,
    "partition_layout_pattern_id": 0
  }
}
```


* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "dataset can not find!"
}
```

* **Sample Call:**
```
GET /dataset?datasetId=1
```
```
GET /dataset?urn=teradata:///DWH_TMP/DUMMY4
```

<a name="dataset-post" />

## Dataset POST API

Post one dataset record into WhereHows metadata storage

* **URL**

  /dataset

* **Method:**

  `POST`

* **Data Params**

  **Required:**
```json
{
  "name" : "DUMMY4",
  "urn" : "teradata:///DWH_TMP/DUMMY4",
  "schema" : "{\"name\": \"DUMMY\", \"fields\": [{\"accessCount\": null, \"lastAccessTime\": null, \"nullable\": \"Y\", \"format\": null, \"type\": \"INT\", \"maxByteLength\": 4, \"name\": \"DUMMY\", \"doc\": \"\"}]}",
  "properties" : "{\"storage_type\": \"View\", \"accessCount\": 2670, \"lastAccessTime\": null, \"sizeInMbytes\": null, \"referenceTables\": [\"DWH_DIM.DUMMY\"], \"viewSqlText\": \"REPLACE VIEW DWH_STG.DUMMY AS\nLOCKING ROW FOR ACCESS\n SELECT * \n   FROM DWH_DIM.DUMMY;\", \"createTime\": \"2015-03-06 10:43:58\", \"lastAlterTime\": \"2015-03-10 20:57:16\"}",
  "schema_type" : "JSON",
  "fields" :  "{\"DUMMY\": {\"type\": \"INT\", \"maxByteLength\": 4}}",
  "source" : "Teradata",
  "source_created_time" : null,
  "location_prefix" : "DWH_STG",
  "ref_dataset_urn" : null,
  "is_partitioned" : "Y",
  "sample_partition_full_path": null,
  "parent_name" : null,
  "storage_type" : null,
  "dataset_type" : null,
  "hive_serdes_class" : null
}
```
```
curl -H "Content-Type: application/json" -X POST -d '{"name" :"DUMMY4","urn" : "teradata:///DWH_TMP/DUMMY4","schema" : "{\"name\": \"DUMMY\", \"fields\": [{\"accessCount\": null, \"lastAccessTime\": null, \"nullable\": \"Y\", \"format\": null, \"type\": \"INT\", \"maxByteLength\": 4, \"name\": \"DUMMY\", \"doc\": \"\"}]}","properties" : "{\"storage_type\": \"View\", \"accessCount\": 2670, \"lastAccessTime\": null, \"sizeInMbytes\": null, \"referenceTables\": [\"DWH_DIM.DUMMY\"], \"viewSqlText\": \"REPLACE VIEW DWH_STG.DUMMY AS\nLOCKING ROW FOR ACCESS\n SELECT * \n FROM DWH_DIM.DUMMY;\", \"createTime\": \"2015-03-06 10:43:58\", \"lastAlterTime\": \"2015-03-10 20:57:16\"}","schema_type" : "JSON","fields" :"{\"DUMMY\": {\"type\": \"INT\", \"maxByteLength\": 4}}","source" : "Teradata","source_created_time" : null,"location_prefix" : "DWH_STG","ref_dataset_urn" : null,”is_partitioned" : "Y","sample_partition_full_path": null,"parent_name" : null,"storage_type" : null,"dataset_type" : null,"hive_serdes_class" : null}' http://localhost:19001/dataset
```
* **Success Response:**
```json
{
  "return_code": 200,
  "message": "Dataset inserted!"
}
```


* **Error Response:**
```json
{
  "return_code": 404,
  "error_message": "Column count doesn't match value count at row 1"
}
```

* **Sample Call:**
```
POST /dataset
```

<a name="lineage-job-get">

## Downstream/Upstream Jobs GET API

Return impact (downstream/upstream) jobs within specified days for a specified dataset

* **URL**

  /lineage/dataset

* **Method:**

  `GET`

* **Data Params**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
|urn | URN of dataset | |		Y|
|cluster|	Cluster instance |	all clusters|	N|
| instance | Application instance | all instances | N |
| direction | Downstream or upstream | downstream | N |
| period |	Historic days of the job execution |	30 |	N |


* **Success Response:**
```json
{
  "return_code": 200,
  "jobs": [
    {
      "short_connection_string": "eat1-nertzaz01",
      "flow_group": "acceptance-tests",
      "flow_name": "all-acceptance-tests",
      "job_name": "pig-token-expired"
    },
    {
      "short_connection_string": "eat1-nertzaz01",
      "flow_group": "all-acceptance",
      "flow_name": "all-acceptance-tests",
      "job_name": "pig-token-expired"
    }
  ]
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "exceptions"
}
```

* **Sample Call:**
```
GET /lineage/dataset?urn=hdfs:///data/databases/GEO/COUNTRIES
```
```
GET /lineage/dataset?urn=hdfs:///data/databases/GEO/COUNTRIES&direction=downstream&cluster=cluster01&instance=instance01
```

<a name="lineage-dataset-get">

## Downstream/Upstream Datasets GET API

Return impact (downstream/upstream) datasets for a specified job or job execution

* **URL**

  /lineage/job

* **Method:**

  `GET`

* **Data Params**

   **Required:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
|flowPath|	project:flow(azkaban); app path(oozie)||		Y|
|jobName|	Job name||		Y|

or

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
|flowExecId	|Flow execution ID||		Y|
|jobName|	Job name||		Y|

or

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| jobExecId| Job execution ID |  | Y|

  **Optional:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| instance | Application instance | all instances | N |
| direction | Downstream or upstream | downstream | N |

* **Success Response:**
```json
{
  "return_code": 200,
  "datasets": [
    {
      "abstracted_object_name": "/data/tracking/UnifiedActionEvent",
      "database_id": 14,
      "partition_start": "2015/10/15/15",
      "partition_end": "2015/10/15/15",
      "storage_type": "HDFS",
      "record_count": null,
      "insert_count": null,
      "update_count": null,
      "delete_count": null
    }
  ]
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
GET /lineage/job?flowPath=b2-backend:ProduceActionDedupRecord&jobName=ProduceActionDedupRecord&direction=upstream
```
```
GET /lineage/job/exec/flow?flowExecId=706130&jobName=prod-bs-decorate-bare-applies
```
```
GET /lineage/job/exec/job?jobExecId=706130001
```

<a name="lineage-post">

## Job Lineage POST API

Post one job lineage records into WhereHows metadata storage

* **URL**

  /lineage

* **Method:**

  `POST`

* **Data Params**

  **Required:**
```json
{
  "app_id" : 310,
  "app_name" : "ltx1-tarockaz01",
  "flow_exec_id" : 928242,
  "job_exec_id" : 928242001,
  "job_exec_uuid" : null,
  "job_name" : "test_lineage",
  "job_start_unixtime" : 1440000000,
  "job_end_unixtime" : 1440000000,
  "flow_path" : "test_post:test_lineage",
  "lineages" : [
       {
        "db_id" : 1000,
        "abstracted_object_name" : "/data/test-lineage-source",
        "full_object_name" : "hdfs:///cluster:port/data/test-lineage-source/1444111922033-PT-262022594",
        "storage_type" : "hdfs",
        "partition_start" : "1444111922033-PT-262022594",
        "partition_end" : "1444111922033-PT-262022594",
        "partition_type" : "snapshot",
        "layout_id" :		0,
        "source_target_type" : "source",
        "operation" : "read",
        "record_count" : 500000,
        "insert_count" :  0,
        "delete_count" :  0,
        "update_count" :  0
       },
       {
        "db_id" : 490,
        "abstracted_object_name" : "data/databases/abc/source1",
        "full_object_name" : "hdfs:///cluster:port/data/databases/abc/source1/2015090822",
        "storage_type" : "hdfs",
        "partition_start" : "2015090822",
        "partition_end" : "2015090822",
        "partition_type" : "hourly",
        "layout_id" :		0,
        "source_target_type" : "source",
        "operation" : "read",
        "record_count" : 3000,
        "insert_count" :  0,
        "delete_count" :  0,
        "update_count" :  0
       },
       {
         "db_id" : 1000,
         "abstracted_object_name" : "/data/test-lineage-target",
         "full_object_name" : "hdfs:///cluster/data/test-lineage-target/20150909",
         "storage_type" : "hdfs",
         "partition_start" : "20150909",
         "partition_end" : "20150909",
         "partition_type" : "daily",
         "layout_id" :		0,
         "source_target_type" : "target",
         "ref_source_object_name" : "/data/databases/abc/source1",
         "ref_source_database_id" : 490,
         "operation" : "write",
         "record_count" : 100000,
         "insert_count" :  0,
         "delete_count" :  0,
         "update_count" :  0
       }
  ]
}
```

* **Success Response:**
```json
{
  "return_code": 200,
  "message": "Lineage inserted!"
}
```


* **Error Response:**
```json
{
  "return_code": 404,
  "error_message": "Something is wrong"
}
```

* **Sample Call:**
```
POST /lineage
```

<a name="dataset-dependency">

## Dataset Dependency GET API
API to Resolve Hive View (Dalids) and Teradata View Dependency

* **URL**

  /dependency/dataset?query=$json

* **Method:**

  `GET`

* **Data Params**  
  We support multiple types of input parameter, if cluster name is skipped, default value is ltx1-holdem 
  
**Required:**
```json
{
  "dataset_uri":"db_name/table_name", 
  "cluster_name": "CLUSTER_NAME"
}
or
{
  "dataset_uri":"hive:///db_name/table_name", 
  "cluster_name": "CLUSTER_NAME"
}
or
{
  "dataset_uri":"dalids:///db_name/table_name", 
  "cluster_name": "CLUSTER_NAME"
}
or
{
  "dataset_uri":"cluster_name:db_name.table_name"
}
or
{
  "dataset_uri":"hive://cluster_name/db_name/table_name"
}
or
{
  "dataset_uri":"dalids://cluster_name/db_name.table_name"
}
   
```

* **Success Response:**
```json
{
  "return_code":200,
  "deployment_tier":"grid",
  "data_center":"eat1",
  "cluster":"eat1-nertz",
  "dataset_type":"Dalids",
  "database_name":"feedimpressionevent_mp",
  "table_name":"feedimpressionevent",
  "urn":"dalids:///feedimpressionevent_mp/feedimpressionevent",
  "dataset_id":192429,
  "input_uri":"dalids://eat1-nertz/feedimpressionevent_mp/feedimpressionevent",
  "dependencies": [
  {
    "dataset_id":164903,
    "database_name":"tracking",
    "type":"Table",
    "table_name":"feedimpressionevent",
    "level_from_root":2,
    "next_level_dependency_count":0,
    "topology_sort_id":"100100",
    "ref_obj_type":"hive",
    "ref_obj_location":"/tracking/feedimpressionevent",
    "high_watermark":null
  },
  {
    "dataset_id":192434,
    "database_name":"feedimpressionevent_mp_versioned",
    "type":"View",
    "table_name":"feedimpressionevent_0_1_8",
    "level_from_root":1,
    "next_level_dependency_count":1,
    "topology_sort_id":"100",
    "ref_obj_type":"dalids",
    "ref_obj_location":"/feedimpressionevent_mp_versioned/feedimpressionevent_0_1_8",
    "high_watermark":null
  }
  ],
  "leaf_level_dependency_count":1}
```


* **Error Response:**
```json
{
  "return_code": 404,
  "error_message": "Wrong input format! Missing dataset uri"
}
```

* **Sample Call:**
```
GET /dependency/dataset?query={"dataset_uri":"feedimpressionevent_mp.feedimpressionevent", "cluster_name": "eat1-nertz"}
```

<a name="flow-owner">

## Flow Owner GET API
Return owner information of a flow

* **URL**

  /flow/owner

* **Method:**

  `GET`

* **Data Params**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
|flowPath|	project:flow(azkaban); app path(oozie)||Y|
| instance | Application instance | all instances | N |

* **Success Response:**
```json
{
  "return_code": 200,
  "owners": [
    {
      "instance": "eat1-nertzaz01",
      "owner_id": "pgovinda",
      "permissions": "ADMIN",
      "owner_type": "LDAP"
    },
    {
      "instance": "lva1-waraz01",
      "owner_id": "elephant",
      "permissions": "ADMIN",
      "owner_type": "LDAP"
    },
    {
      "instance": "lva1-waraz01",
      "owner_id": "pgovinda",
      "permissions": "ADMIN",
      "owner_type": "LDAP"
    }
  ]
}
```

* **Error Response:**
```json
{
  "return_code": 404,
  "error_message": "Something is wrong"
}
```

* **Sample Call:**
```
GET /flow/owner?flowPath=job-analytics:prod-bs-push-to-offline-cluster
```


<a name="flow-schedule">


## Flow Schedule GET API
Return schedule information of a flow

* **URL**

  /flow/schedule

* **Method:**

  `GET`

* **Data Params**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
|flowPath|	project:flow(azkaban); app path(oozie)||Y|
| instance | Application instance | all instances | N |


* **Success Response:**
```json
{
  "return_code": 200,
  "schedules": [
    {
      "instance": "lva1-waraz01",
      "frequency": 1,
      "unit": "DAY",
      "effective_start_time": 1438416000,
      "effective_end_time": 4102387200
    }
  ]
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "Something is wrong"
}
```

* **Sample Call:**
```
GET /flow/schedule?flowPath=job-analytics:prod-bs-push-to-offline-cluster
```

<a name="filename-get">

## Filename Pattern GET API
Get all filename patterns

* **URL**

  /pattern/filename

* **Method:**

  `GET`


* **Success Response:**
```json
{
  "return_code": 200,
  "filename_patterns": [
    {
      "filename_pattern_id": 1,
      "regex": "(.*)/part-m-\d+\.avro"
    },
    {
      "filename_pattern_id": 2,
      "regex": "(.*)/part-r-\d+\.avro"
    },
    {
      "filename_pattern_id": 3,
      "regex": "(.*)/part-\d+\.avro"
    },
    {
      "filename_pattern_id": 4,
      "regex": "(.*)/part-\d+"
    },
    {
      "filename_pattern_id": 5,
      "regex": "(.*)/part-m-\d+"
    },
    {
      "filename_pattern_id": 6,
      "regex": "(.*)/part-r-\d+"
    },
    {
      "filename_pattern_id": 7,
      "regex": "(.*)/\d+\.avro"
    }
  ]
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "Something is wrong"
}
```

* **Sample Call:**
```
GET /pattern/filename
```

<a name="filename-add">

## Filename Pattern POST API
Add a new filename pattern

* **URL**

  /pattern/filename

* **Method:**

  `POST`

* **Data Params**


   **Request body in json format:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| regex | regular expression of filename |  | Y|

**Example**
```json
{
    "regex": "(.*)/partition-m-\d+\.avro"
}
```

* **Success Response:**
```json
{
  "return_code": 200,
  "message": "New filename pattern created!"
}
```


* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
POST /pattern/filename
```
with request body:

```json
{
    "regex": "(.*)/partition-m-\d+\.avro"
}
```

<a name="dataset-partition-pattern-get">

## Dataset Partition Pattern GET API
Get all dataset partition patterns

* **URL**

  /pattern/partiton

* **Method:**

  `GET`


* **Success Response:**
```json
{
  "return_code": 200,
  "dataset_partition_patterns": [
    {
      "layout_id": 1,
      "regex": "(.*)\/daily\/(\d{4}\/\d{2}\/\d{2}).*",
      "mask": "daily/yyyy/MM/dd",
      "leading_path_index": 1,
      "partition_index": 2,
      "second_partition_index": null,
      "sort_id": 1,
      "comments": null,
      "partition_pattern_group": "daily"
    },
    {
      "layout_id": 2,
      "regex": "(.*)\/hourly\/(\d{4}-\d{2}-\d{2}-\d{2}).*",
      "mask": "hourly/yyyy-MM-dd-HH",
      "leading_path_index": 1,
      "partition_index": 2,
      "second_partition_index": null,
      "sort_id": 2,
      "comments": null,
      "partition_pattern_group": "hourly"
    },
    {
      "layout_id": 3,
      "regex": "(.*)\/yearly\/(\d{4}).*",
      "mask": "yearly/YYYY",
      "leading_path_index": 1,
      "partition_index": 2,
      "second_partition_index": null,
      "sort_id": 3,
      "comments": null,
      "partition_pattern_group": "yearly"
    },
    {
      "layout_id": 4,
      "regex": "(.*)\/monthly\/(\d{4}\/\d{2}).*",
      "mask": "monthly/yyyy/MM",
      "leading_path_index": 1,
      "partition_index": 2,
      "second_partition_index": null,
      "sort_id": 10,
      "comments": null,
      "partition_pattern_group": "monthly"
    },
    {
      "layout_id": 5,
      "regex": "(.*)\/weekly\/(\d{4}\/\d{2}).*",
      "mask": "weekly/YYYY/MM",
      "leading_path_index": 1,
      "partition_index": 2,
      "second_partition_index": null,
      "sort_id": 12,
      "comments": null,
      "partition_pattern_group": "weekly"
    },
    {
      "layout_id": 6,
      "regex": "(.*)\/(\d+-(PT|scn)-\d+)(\/.*)?",
      "mask": "epoch-PT|SCN-recordcount",
      "leading_path_index": 1,
      "partition_index": 2,
      "second_partition_index": null,
      "sort_id": 20,
      "comments": null,
      "partition_pattern_group": "snapshot"
    }
  ]
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "Something is wrong"
}
```

* **Sample Call:**
```
GET /pattern/partition
```


<a name="dataset-partition-pattern-add">

## Dataset Partition Pattern POST API
Add a new dataset partition patterns

* **URL**

  /pattern/partition

* **Method:**

  `POST`

* **Data Params**


   **Request body in JSON format:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| regex | regular expression of partition |  | Y|
| mask| mask of the partition| | N|
|leading_path_index|group index of abstract path in the regex||Y|
|partition_index|group index of partition in the regex||Y|
|second_partition_index|group index of second partition in the regex|null|N|
|sort_id|order ID that will apply the regex|AUTO incremental|Y|
|partition_pattern_group|pattern type|||
|comments|comments|null|N|


**Example**
```json
{
  "regex": "(.*)/daily/(d{4}/d{2}/d{2}).*",
  "mask": "daily/yyyy/MM/dd",
  "leading_path_index": 1,
  "partition_index": 2,
  "second_partition_index": null,
  "sort_id": 1000000,
  "comments": null,
  "partition_pattern_group": "daily"
}
```

* **Success Response:**
```json
{
  "return_code": 200,
  "message": "New dataset partition pattern created!"
}
```


* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
POST /pattern/partition
```
with request body:

```json
{
  "regex": "(.*)/daily/(d{4}/d{2}/d{2}).*",
  "mask": "daily/yyyy/MM/dd",
  "leading_path_index": 1,
  "partition_index": 2,
  "second_partition_index": null,
  "sort_id": 1000000,
  "comments": null,
  "partition_pattern_group": "daily"
}
```

<a name="log-lineage-pattern-get">

## Log Lineage Pattern GET API
Get all log lineage patterns

* **URL**

  /pattern/lineage

* **Method:**

  `GET`


* **Success Response:**
```json
{
  "return_code": 200,
  "lineage_patterns": [
    {
      "pattern_id": 1,
      "pattern_type": "hourglass",
      "regex": "Moving from staged path\[\S*\] to final resting place\[(\/(?!tmp)[\S]*)\]",
      "database_type": "hdfs",
      "database_name_index": null,
      "dataset_index": 1,
      "operation_type": "write",
      "record_count_index": null,
      "record_byte_index": null,
      "insert_count_index": null,
      "insert_byte_index": null,
      "delete_count_index": null,
      "delete_byte_index": null,
      "update_count_index": null,
      "update_byte_index": null,
      "comments": "hourglass",
      "source_target_type": "target"
    },
    {
      "pattern_id": 2,
      "pattern_type": "hourglass",
      "regex": "Moving \S* to (\/(?!tmp)[\S]*)",
      "database_type": "hdfs",
      "database_name_index": null,
      "dataset_index": 1,
      "operation_type": "write",
      "record_count_index": null,
      "record_byte_index": null,
      "insert_count_index": null,
      "insert_byte_index": null,
      "delete_count_index": null,
      "delete_byte_index": null,
      "update_count_index": null,
      "update_byte_index": null,
      "comments": "hourglass",
      "source_target_type": "target"
    },
    {
      "pattern_id": 3,
      "pattern_type": "build and push",
      "regex": "Pushing to clusterURl(tcp:\S*)[\s\S]*Initiating swap of (\S*) with dataDir",
      "database_type": "voldermont",
      "database_name_index": 1,
      "dataset_index": 2,
      "operation_type": "push",
      "record_count_index": null,
      "record_byte_index": null,
      "insert_count_index": null,
      "insert_byte_index": null,
      "delete_count_index": null,
      "delete_byte_index": null,
      "update_count_index": null,
      "update_byte_index": null,
      "comments": "build and push voldermont",
      "source_target_type": "target"
    },
    {
      "pattern_id": 4,
      "pattern_type": "pig log",
      "regex": "Successfully read (\d+) records( \(\d+ bytes\))? from: \"(.*)\"",
      "database_type": "hdfs",
      "database_name_index": null,
      "dataset_index": 3,
      "operation_type": "read",
      "record_count_index": 1,
      "record_byte_index": 2,
      "insert_count_index": null,
      "insert_byte_index": null,
      "delete_count_index": null,
      "delete_byte_index": null,
      "update_count_index": null,
      "update_byte_index": null,
      "comments": "pig log",
      "source_target_type": "source"
    },
    {
      "pattern_id": 5,
      "pattern_type": "pig log",
      "regex": "Successfully stored (\d+) records( \(\d+ bytes\))? in: \"(.*)\"",
      "database_type": "hdfs",
      "database_name_index": null,
      "dataset_index": 3,
      "operation_type": "write",
      "record_count_index": 1,
      "record_byte_index": 2,
      "insert_count_index": null,
      "insert_byte_index": null,
      "delete_count_index": null,
      "delete_byte_index": null,
      "update_count_index": null,
      "update_byte_index": null,
      "comments": "pig log",
      "source_target_type": "target"
    }
  ]
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "Something is wrong"
}
```

* **Sample Call:**
```
GET /pattern/lineage
```

<a name="log-lineage-pattern-add">

## Log Lineage Pattern POST API
Add a new lineage pattern

* **URL**

  /pattern/lineage

* **Method:**

  `POST`

* **Data Params**


   **Request body in JSON format:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| regex | regular expression of lineage |  | Y|
|pattern_type|type of log||N|
|database_type|e.g., HDFS, Voldemort ||N|
|operation_type|e.g., write, read|||
|source_target_type|||Y|
|dataset_index|dataset group index in the regex|null|N|
|database_name_index|database name group index in the regex|null|N|
|record_count_index|all kinds of record count group index in the regex|null|N|
|record_byte_index|all kinds of record byte group index in the regex|null|N|
|insert_count_index|insert record count group index in the regex|null|N|
|insert_byte_index|insert byte group index in the regex|null|N|
|delete_count_index|delete record count group index in the regex|null|N|
|delete_byte_index|delete byte group index in the regex|null|N|
|update_count_index|update record count group index in the regex|null|N|
|update_byte_index|update byte group index in the regex|null|N|
|comments|comments|null|N|


**Example**
```json
{
  "pattern_type": "pig log",
  "regex": "Successfully read (d+) records( (d+ bytes))? from: \"(.*)\"",
  "database_type": "hdfs",
  "database_name_index": null,
  "dataset_index": 3,
  "operation_type": "read",
  "record_count_index": 1,
  "record_byte_index": 2,
  "insert_count_index": null,
  "insert_byte_index": null,
  "delete_count_index": null,
  "delete_byte_index": null,
  "update_count_index": null,
  "update_byte_index": null,
  "comments": "pig log",
  "source_target_type": "source"
}
```

* **Success Response:**
```json
{
  "return_code": 200,
  "message": "New lineage pattern created!"
}
```


* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
POST /pattern/lineage
```
with request body:

```json
{
  "pattern_type": "pig log",
  "regex": "Successfully read (d+) records( (d+ bytes))? from: \"(.*)\"",
  "database_type": "hdfs",
  "database_name_index": null,
  "dataset_index": 3,
  "operation_type": "read",
  "record_count_index": 1,
  "record_byte_index": 2,
  "insert_count_index": null,
  "insert_byte_index": null,
  "delete_count_index": null,
  "delete_byte_index": null,
  "update_count_index": null,
  "update_byte_index": null,
  "comments": "pig log",
  "source_target_type": "source"
}
```

<a name="log-job-id-pattern-get">

## Log Job Id Pattern GET API
Get all log job ID patterns

* **URL**

  /pattern/jobid

* **Method:**

  `GET`


* **Success Response:**
```json
{
  "return_code": 200,
  "job_id_patterns": [
    {
      "pattern_id": 1,
      "pattern_type": "pig",
      "regex": "HadoopJobId: (job_\d+_\d+)",
      "reference_job_id_index": 1,
      "is_active": true,
      "comments": "pig sample : HadoopJobId: job_1443068642861_495061"
    },
    {
      "pattern_id": 2,
      "pattern_type": "hive",
      "regex": "INFO - Starting Job = (job_\d+_\d+)",
      "reference_job_id_index": 1,
      "is_active": true,
      "comments": "hive sample : Starting Job = job_1443068642861_495047"
    },
    {
      "pattern_id": 3,
      "pattern_type": "mapreduce",
      "regex": "Job (job_\d+_\d+) completed successfully",
      "reference_job_id_index": 1,
      "is_active": true,
      "comments": "Job job_1440264275625_235896 completed successfully"
    }
  ]
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "Something is wrong"
}
```

* **Sample Call:**
```
GET /pattern/jobid
```

<a name="log-job-id-pattern-add">

## Log Job Id Pattern POST API
Add a new log job ID pattern

* **URL**

  /pattern/jobid

* **Method:**

  `POST`

* **Data Params**


   **Request body in json format:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| regex | regular expression of job ID in log |  | Y|
| pattern_type|type||N|
|reference_job_id_index|group index of reference job in regex||Y|
|is_active|active flag|1|N|
|comments|comments|null|N|

**Example**
```json
{
      "pattern_type": "pig",
      "regex": "HadoopJobId: (job_d+_d+)",
      "reference_job_id_index": 1,
      "is_active": true,
      "comments": "pig sample : HadoopJobId: job_1443068642861_495061"
}
```

* **Success Response:**
```json
{
  "return_code": 200,
  "message": "New job id pattern created!"
}
```


* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
POST /pattern/jobid
```
with request body:

```json
{
      "pattern_type": "pig",
      "regex": "HadoopJobId: (job_d+_d+)",
      "reference_job_id_index": 1,
      "is_active": true,
      "comments": "pig sample : HadoopJobId: job_1443068642861_495061"
}
```

<a name="cfg-app">

## Application GET API 1
Get all applications information

* **URL**

  /cfg/apps

* **Method:**

  `GET`

* **Success Response:**
```json
{
  "return_code": 200,
  "applications": [
    {
      "app_id": 102,
      "app_code": "OOZIE VM",
      "description": "VM",
      "tech_matrix_id": 19,
      "doc_url": null,
      "parent_app_id": 100,
      "app_status": "N",
      "last_modified": 1446099261000,
      "is_logical": "N",
      "uri_type": null,
      "uri": "http://172.21.98.60:11000/",
      "lifecycle_layer_id": null,
      "short_connection_string": "oozie-vm"
    }
  ]
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
GET /cfg/apps
```

## Application GET API 2
Get application information by application ID
* **URL**

  /cfg/app/id

* **Method:**

  `GET`

* **Data Params**

   **Required:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| id | application ID |  | Y|

* **Success Response:**
```json
{
  "return_code": 200,
  "application": {
    "app_id": 102,
    "app_code": "OOZIE VM",
    "description": "VM",
    "tech_matrix_id": 19,
    "doc_url": null,
    "parent_app_id": 100,
    "app_status": "N",
    "last_modified": 1446099261000,
    "is_logical": "N",
    "uri_type": null,
    "uri": "http://172.21.98.60:11000/",
    "lifecycle_layer_id": null,
    "short_connection_string": "oozie-vm"
  }
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
GET /cfg/app/id?id=102
```

## Application GET API 3
Get application information by application short connection string name
* **URL**

  /cfg/app/name

* **Method:**

  `GET`

* **Data Params**

   **Required:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| name | short connection string name |  | Y|

* **Success Response:**
```json
{
  "return_code": 200,
  "application": {
    "app_id": 102,
    "app_code": "OOZIE VM",
    "description": "VM",
    "tech_matrix_id": 19,
    "doc_url": null,
    "parent_app_id": 100,
    "app_status": "N",
    "last_modified": 1446099261000,
    "is_logical": "N",
    "uri_type": null,
    "uri": "http://172.21.98.60:11000/",
    "lifecycle_layer_id": null,
    "short_connection_string": "oozie-vm"
  }
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
GET /cfg/app/name?name=oozie-vm
```

<a name="cfg-app-add">

## Application POST/PUT API
Add or update an application

* **URL**

  /cfg/app

* **Method:**

  `POST` for add a new application
or
  `PUT` for update an existing application

* **Data Params**


   **Request body in JSON format:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| app_id | User assigned a unique application ID |  | Y |
| app_code | Application code |  | Y |
| description | Description of this application | | Y |
| tech_matrix_id | matrix id it belong to | | N |
| parent_app_id | the parent application ID, for example the Hadoop cluster application ID  | 0 | N |
| app_status | application status | A | N |
| uri | URI of the application | null | N |
| short_connection_string | unique short connection string |  | Y |

**Example**


```json
    {
      "app_id": 103,
      "app_code": "OOZIE VM",
      "description": "VM",
      "parent_app_id": 100,
      "app_status": "A",
      "uri": "http://172.21.98.60:11000/",
      "short_connection_string": "oozie-vm"
    }
```

* **Success Response:**
```json
{
  "return_code": 200,
  "message": "New application created!"
}
```

or

```json
{
  "return_code": 200,
  "message": "Application updated!"
}
```



* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**

```
POST /cfg/app
```

or

```
PUT /cfg/app
```

with request body:

```json
    {
      "app_id": 103,
      "app_code": "OOZIE VM",
      "description": "VM",
      "parent_app_id": 100,
      "app_status": "A",
      "uri": "http://172.21.98.60:11000/",
      "short_connection_string": "oozie-vm"
    }
```


<a name="cfg-db">

## Database GET API 1
Get all databases information

* **URL**

  /cfg/dbs

* **Method:**

  `GET`

* **Success Response:**
```json
{
  "return_code": 200,
  "databases": [
    {
      "db_id": 10001,
      "db_code": "SAMPLETD",
      "db_type_id": 0,
      "description": "TERADATA VM",
      "is_logical": "N",
      "cluster_size": 23,
      "associated_data_centers": 1,
      "replication_role": "MASTER",
      "jdbc_url": null,
      "uri": "Teradata://sample-td",
      "short_connection_string": "SAMPLE-TD",
      "last_modified": 1446099112000
    }
  ]
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
GET /cfg/dbs
```

## Database GET API 2
Get database information by database ID
* **URL**

  /cfg/db/id

* **Method:**

  `GET`

* **Data Params**

   **Required:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| id | database ID |  | Y|

* **Success Response:**
```json
{
  "return_code": 200,
  "database": {
    "db_id": 10001,
    "db_code": "SAMPLETD",
    "db_type_id": 0,
    "description": "TERADATA VM",
    "is_logical": "N",
    "cluster_size": 23,
    "associated_data_centers": 1,
    "replication_role": "MASTER",
    "jdbc_url": null,
    "uri": "Teradata://sample-td",
    "short_connection_string": "SAMPLE-TD",
    "last_modified": 1446099112000
  }
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
GET /cfg/db/id?id=10001
```

## Application GET API 3
Get application information by application short connection string name
* **URL**

  /cfg/db/name

* **Method:**

  `GET`

* **Data Params**

   **Required:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| name | short connection string name |  | Y|

* **Success Response:**
```json
{
  "return_code": 200,
  "database": {
    "db_id": 10001,
    "db_code": "SAMPLETD",
    "db_type_id": 0,
    "description": "TERADATA VM",
    "is_logical": "N",
    "cluster_size": 23,
    "associated_data_centers": 1,
    "replication_role": "MASTER",
    "jdbc_url": null,
    "uri": "Teradata://sample-td",
    "short_connection_string": "SAMPLE-TD",
    "last_modified": 1446099112000
  }
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
GET /cfg/db/name?name=SAMPLE-TD
```

<a name="cfg-db-add">

## Database POST/PUT API

* **URL**

  /cfg/db

* **Method:**

  `POST` for add a new database
or
  `PUT` for update an existing database

* **Data Params**


   **Request body in JSON format:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| db_id | User assigned a unique application ID |  | Y |
| db_code | Database code |  | Y |
| primary_dataset_type | Database type  | null | N |
| description | Description of this database | | Y |
| cluster_size | Size of the cluster  | 0 | N |
| short_connection_string | unique short connection string |  | Y |
| associated_dc_num | Number of associated data centers | 0 | N |
| replication_role | MASTER or SLAVE or BOTH or Primary | null | N |
| uri | URI of the application | null | N |

**Example**
```json
    {
      "db_id": 10001,
      "db_code": "SAMPLETD",
      "primary_dataset_type": null,
      "description": "TERADATA VM",
      "cluster_size": 22,
      "associated_dc_num": 1,
      "replication_role": "MASTER",
      "uri": "Teradata://sample-td",
      "short_connection_string": "SAMPLE-TD"
    }
```

* **Success Response:**
```json
{
  "return_code": 200,
  "message": "New database created!"
}
```

or

```json
{
  "return_code": 200,
  "message": "Database updated!"
}
```



* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**

```
POST /cfg/db
```

or

```
PUT /cfg/db
```

with request body:

```json
    {
      "db_id": 10001,
      "db_code": "SAMPLETD",
      "description": "TERADATA VM",
      "cluster_size": 22,
      "associated_data_centers": 1,
      "replication_role": "MASTER",
      "uri": "Teradata://sample-td",
      "short_connection_string": "SAMPLE-TD"
    }
```


<a name="etl-job-get">

## ETL Job GET API 1
Get all ETL jobs information

* **URL**

  /etls

* **Method:**

  `GET`

* **Success Response:**
```json
{
  "return_code": 200,
  "etl_jobs": [
    {
      "wh_etl_job_id": 19,
      "wh_etl_job_name": "OOZIE_EXECUTION_METADATA_ETL",
      "wh_etl_type": "OPERATION",
      "cron_expr": "0 12 * * * ?",
      "ref_id": 102,
      "timeout": null,
      "next_run": null,
      "comments": "oozie vm metadata etl",
      "ref_id_type": "APP",
      "is_active": "Y"
    }
  ]
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
GET /etls
```

## ETL Job GET API 2
Return information about dataset by dataset ID or URN

* **URL**

  /etl

* **Method:**

  `GET`

* **Data Params**

   **Required:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| id | ETL job ID |  | Y|

* **Success Response:**
```json
{
  "return_code": 200,
  "etl_job": {
    "wh_etl_job_id": 1,
    "wh_etl_job_name": "AZKABAN_EXECUTION_METADATA_ETL",
    "wh_etl_type": "OPERATION",
    "cron_expr": "* 0 * * * ?",
    "ref_id": 31,
    "timeout": null,
    "next_run": null,
    "comments": null,
    "ref_id_type": "APP",
    "is_active": "Y"
  }
}
```


* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
GET /etl?id=1
```

<a name="etl-job-post">

## ETL Job New Job POST API
Submit a new scheduled ETL job

* **URL**

  /etl

* **Method:**

  `POST`

* **Data Params**


   **Request body in JSON format:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| wh_etl_job_name | WhereHows ETL job name |  | Y|
| ref_id | Application or database ID |  | Y|
| cron_expr | cron expression of the job schedule |  | Y|
| properties | key-value pair properties of the job |  | Y|
| encrypted_property_keys | keys that need encrypted |  | N|
| timeout | timeout for the job |  | N|
| next_run | next runtime in Unix time | now | N|
| comments | user comments |  | Y|

**Example**
```json
{
    "wh_etl_job_name": "OOZIE_EXECUTION_METADATA_ETL",
    "ref_id": 102,
    "cron_expr": "* 11 * * * ?",
    "properties": {"oz.db.username": "root",
                   "oz.db.password": "cloudera",
                   "oz.db.jdbc.url": "jdbc:mysql://host:port/oozie",
                   "oz.db.driver": "com.mysql.jdbc.Driver",
                   "oz.exec_etl.lookback_period.in.minutes": "1000"
                  },
    "encrypted_property_keys": ["oz.db.password"],
    "timeout": null,
    "next_run": null,
    "comments": "oozie vm metadata etl"
}
```

* **Success Response:**
```json
{
  "return_code": 200,
  "message": "Etl job created!"
}
```


* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
POST /etl
```
with request body:

```json
{
    "wh_etl_job_name": "OOZIE_EXECUTION_METADATA_ETL",
    "ref_id": 102,
    "cron_expr": "* 11 * * * ?",
    "properties": {"oz.db.username": "root",
                   "oz.db.password": "cloudera",
                   "oz.db.jdbc.url": "jdbc:mysql://host:port/oozie",
                   "oz.db.driver": "com.mysql.jdbc.Driver",
                   "oz.exec_etl.lookback_period.in.minutes": "1000"
                  },
    "encrypted_property_keys": ["oz.db.password"],
    "timeout": null,
    "next_run": null,
    "comments": "oozie vm metadata etl"
}
```

<a name="etl-job-control">

## ETL Job Control PUT API
Deactivate a previous scheduled ETL job or reactivate a stopped ETL job or delete an ETL job

* **URL**

  /etl/control

* **Method:**

  `PUT`

* **Data Params**

   **Request body in JSON format:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| wh_etl_job_name | WhereHows ETL job name |  | Y|
| ref_id | Application or database ID |  | Y|
| control | deactivate or activate or delete |  | Y|

**Example**
```json
{
    "wh_etl_job_name": "OOZIE_EXECUTION_METADATA_ETL",
    "ref_id": 102,
    "control": "deactivate"
}
```

* **Success Response:**
```json
{
  "return_code": 200,
  "message": "Etl job disabled!"
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
PUT /etl/control
```

with request body:

```json
{
    "wh_etl_job_name": "OOZIE_EXECUTION_METADATA_ETL",
    "ref_id": 102,
    "control": "deactivate"
}
```

<a name="etl-job-schedule">

## ETL Job Schedule Update PUT API
Update an ETL job schedule

* **URL**

  /etl/schedule

* **Method:**

  `PUT`

* **Data Params**

   **Request body in JSON format:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| wh_etl_job_name | WhereHows ETL job name |  | Y|
| ref_id | Application or database ID |  | Y|
| cron_expr | deactivate or activate or delete |  | Y|

   **Example**
```json
{
    "wh_etl_job_name": "OOZIE_EXECUTION_METADATA_ETL",
    "ref_id": 102,
    "cron_expr": "0 12 * * * ?"
}
```

* **Success Response:**
```json
{
  "return_code": 200,
  "message": "Etl job schedule updated!"
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
curl -H "Content-Type: application/json" -d '{"wh_etl_job_name":"OOZIE_EXECUTION_METADATA_ETL","ref_id":102,"cron_expr":"0 12 * * * ?"}' http://localhost:19001/etl/schedule -X PUT
```


<a name="etl-job-property">

## ETL Job Property Update PUT API
Update a job property

* **URL**

  /etl/property

* **Method:**

  `PUT`

* **Data Params**

   **Request body in JSON format:**

| Param Name | Description | Default | Required |
| -----|-----| -----|:----:|
| wh_etl_job_name | WhereHows ETL job name |  | Y|
| ref_id | Application or database ID |  | Y|
| property_name | name of the property |  | Y|
| property_value | value of the property |  | Y|
| need_encrypted | true or false |  | N |

\* If **need_encrypted=true**, an encryption key will be fetched from the location specified in ETL property table by property_name=**wherehows.encrypt.master.key.loc** or the default location at **~/.wherehows/master_key**.

Usually this option is set to true when storing passwords or connection tokens.
This encryption key file is not checked in to GitHub or included in Play distribution zip file.
Please maintain this file manually and keep the file system permission as rw------- or 0600

   **Example:**

```json
{
    "wh_etl_job_name": "OOZIE_EXECUTION_METADATA_ETL",
    "ref_id": 102,
    "property_name": "oz.exec_etl.lookback_period.in.minutes",
    "property_value": "1001",
    "need_encrypted": true
}
```

* **Success Response:**
```json
{
  "return_code": 200,
  "message": "Etl job schedule updated!"
}
```

* **Error Response:**
```json
{
  "return_code": "404",
  "error_message": "something is wrong"
}
```

* **Sample Call:**
```
curl -H "Content-Type: application/json" -X PUT -d '{"wh_etl_job_name": "OOZIE_EXECUTION_METADATA_ETL", "ref_id": 102, "property_name": "oz.exec_etl.lookback_period.in.minutes", "property_value": "1001", "need_encrypted": true}' http://localhost:19001/etl/property
```