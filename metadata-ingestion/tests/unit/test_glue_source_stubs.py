import datetime
import io
from typing import Any, Dict

from botocore.response import StreamingBody

get_databases_response = {
    "DatabaseList": [
        {
            "Name": "flights-database",
            "CreateTime": datetime.datetime(2021, 6, 9, 14, 14, 19),
            "CreateTableDefaultPermissions": [
                {
                    "Principal": {
                        "DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"
                    },
                    "Permissions": ["ALL"],
                }
            ],
            "CatalogId": "123412341234",
        },
        {
            "Name": "test-database",
            "CreateTime": datetime.datetime(2021, 6, 1, 14, 55, 2),
            "CreateTableDefaultPermissions": [
                {
                    "Principal": {
                        "DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"
                    },
                    "Permissions": ["ALL"],
                }
            ],
            "CatalogId": "123412341234",
        },
    ]
}
databases_1 = {"flights-database": {"Name": "flights-database"}}
databases_2 = {"test-database": {"Name": "test-database"}}
tables_1 = [
    {
        "Name": "avro",
        "DatabaseName": "flights-database",
        "Owner": "owner",
        "CreateTime": datetime.datetime(2021, 6, 9, 14, 17, 35),
        "UpdateTime": datetime.datetime(2021, 6, 9, 14, 17, 35),
        "LastAccessTime": datetime.datetime(2021, 6, 9, 14, 17, 35),
        "Retention": 0,
        "StorageDescriptor": {
            "Columns": [
                {"Name": "yr", "Type": "int"},
                {"Name": "flightdate", "Type": "string"},
                {"Name": "uniquecarrier", "Type": "string"},
                {"Name": "airlineid", "Type": "int"},
                {"Name": "carrier", "Type": "string"},
                {"Name": "flightnum", "Type": "string"},
                {"Name": "origin", "Type": "string"},
            ],
            "Location": "s3://crawler-public-us-west-2/flight/avro/",
            "InputFormat": "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
            "Compressed": False,
            "NumberOfBuckets": -1,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
                "Parameters": {
                    "avro.schema.literal": '{"type":"record","name":"flights_avro_subset","namespace":"default","fields":[{"name":"yr","type":["null","int"],"default":null},{"name":"flightdate","type":["null","string"],"default":null},{"name":"uniquecarrier","type":["null","string"],"default":null},{"name":"airlineid","type":["null","int"],"default":null},{"name":"carrier","type":["null","string"],"default":null},{"name":"flightnum","type":["null","string"],"default":null},{"name":"origin","type":["null","string"],"default":null},{"name":"dest","type":["null","string"],"default":null},{"name":"depdelay","type":["null","int"],"default":null},{"name":"carrierdelay","type":["null","int"],"default":null},{"name":"weatherdelay","type":["null","int"],"default":null}]}',
                    "serialization.format": "1",
                },
            },
            "BucketColumns": [],
            "SortColumns": [],
            "Parameters": {
                "CrawlerSchemaDeserializerVersion": "1.0",
                "CrawlerSchemaSerializerVersion": "1.0",
                "UPDATED_BY_CRAWLER": "flights-crawler",
                "averageRecordSize": "55",
                "avro.schema.literal": '{"type":"record","name":"flights_avro_subset","namespace":"default","fields":[{"name":"yr","type":["null","int"],"default":null},{"name":"flightdate","type":["null","string"],"default":null},{"name":"uniquecarrier","type":["null","string"],"default":null},{"name":"airlineid","type":["null","int"],"default":null},{"name":"carrier","type":["null","string"],"default":null},{"name":"flightnum","type":["null","string"],"default":null},{"name":"origin","type":["null","string"],"default":null},{"name":"dest","type":["null","string"],"default":null},{"name":"depdelay","type":["null","int"],"default":null},{"name":"carrierdelay","type":["null","int"],"default":null},{"name":"weatherdelay","type":["null","int"],"default":null}]}',
                "classification": "avro",
                "compressionType": "none",
                "objectCount": "30",
                "recordCount": "169222196",
                "sizeKey": "9503351413",
                "typeOfData": "file",
            },
            "StoredAsSubDirectories": False,
        },
        "PartitionKeys": [{"Name": "year", "Type": "string"}],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "CrawlerSchemaDeserializerVersion": "1.0",
            "CrawlerSchemaSerializerVersion": "1.0",
            "UPDATED_BY_CRAWLER": "flights-crawler",
            "averageRecordSize": "55",
            "avro.schema.literal": '{"type":"record","name":"flights_avro_subset","namespace":"default","fields":[{"name":"yr","type":["null","int"],"default":null},{"name":"flightdate","type":["null","string"],"default":null},{"name":"uniquecarrier","type":["null","string"],"default":null},{"name":"airlineid","type":["null","int"],"default":null},{"name":"carrier","type":["null","string"],"default":null},{"name":"flightnum","type":["null","string"],"default":null},{"name":"origin","type":["null","string"],"default":null},{"name":"dest","type":["null","string"],"default":null},{"name":"depdelay","type":["null","int"],"default":null},{"name":"carrierdelay","type":["null","int"],"default":null},{"name":"weatherdelay","type":["null","int"],"default":null}]}',
            "classification": "avro",
            "compressionType": "none",
            "objectCount": "30",
            "recordCount": "169222196",
            "sizeKey": "9503351413",
            "typeOfData": "file",
        },
        "CreatedBy": "arn:aws:sts::123412341234:assumed-role/AWSGlueServiceRole-flights-crawler/AWS-Crawler",
        "IsRegisteredWithLakeFormation": False,
        "CatalogId": "123412341234",
    }
]
get_tables_response_1 = {"TableList": tables_1}
tables_2 = [
    {
        "Name": "test_jsons_markers",
        "DatabaseName": "test-database",
        "Owner": "owner",
        "CreateTime": datetime.datetime(2021, 6, 2, 12, 6, 59),
        "UpdateTime": datetime.datetime(2021, 6, 2, 12, 6, 59),
        "LastAccessTime": datetime.datetime(2021, 6, 2, 12, 6, 59),
        "Retention": 0,
        "StorageDescriptor": {
            "Columns": [
                {
                    "Name": "markers",
                    "Type": "array<struct<name:string,position:array<double>,location:array<double>>>",
                }
            ],
            "Location": "s3://test-glue-jsons/markers/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "Compressed": False,
            "NumberOfBuckets": -1,
            "SerdeInfo": {
                "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                "Parameters": {"paths": "markers"},
            },
            "BucketColumns": [],
            "SortColumns": [],
            "Parameters": {
                "CrawlerSchemaDeserializerVersion": "1.0",
                "CrawlerSchemaSerializerVersion": "1.0",
                "UPDATED_BY_CRAWLER": "test-jsons",
                "averageRecordSize": "273",
                "classification": "json",
                "compressionType": "none",
                "objectCount": "1",
                "recordCount": "1",
                "sizeKey": "273",
                "typeOfData": "file",
            },
            "StoredAsSubDirectories": False,
        },
        "PartitionKeys": [],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "CrawlerSchemaDeserializerVersion": "1.0",
            "CrawlerSchemaSerializerVersion": "1.0",
            "UPDATED_BY_CRAWLER": "test-jsons",
            "averageRecordSize": "273",
            "classification": "json",
            "compressionType": "none",
            "objectCount": "1",
            "recordCount": "1",
            "sizeKey": "273",
            "typeOfData": "file",
        },
        "CreatedBy": "arn:aws:sts::795586375822:assumed-role/AWSGlueServiceRole-test-crawler/AWS-Crawler",
        "IsRegisteredWithLakeFormation": False,
        "CatalogId": "795586375822",
    },
    {
        "Name": "test_parquet",
        "DatabaseName": "test-database",
        "Owner": "owner",
        "CreateTime": datetime.datetime(2021, 6, 1, 16, 14, 53),
        "UpdateTime": datetime.datetime(2021, 6, 1, 16, 14, 53),
        "LastAccessTime": datetime.datetime(2021, 6, 1, 16, 14, 53),
        "Retention": 0,
        "StorageDescriptor": {
            "Columns": [
                {"Name": "yr", "Type": "int"},
                {"Name": "quarter", "Type": "int"},
                {"Name": "month", "Type": "int"},
                {"Name": "dayofmonth", "Type": "int"},
            ],
            "Location": "s3://crawler-public-us-west-2/flight/parquet/",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": False,
            "NumberOfBuckets": -1,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "BucketColumns": [],
            "SortColumns": [],
            "Parameters": {
                "CrawlerSchemaDeserializerVersion": "1.0",
                "CrawlerSchemaSerializerVersion": "1.0",
                "UPDATED_BY_CRAWLER": "test",
                "averageRecordSize": "19",
                "classification": "parquet",
                "compressionType": "none",
                "objectCount": "60",
                "recordCount": "167497743",
                "sizeKey": "4463574900",
                "typeOfData": "file",
            },
            "StoredAsSubDirectories": False,
        },
        "PartitionKeys": [{"Name": "year", "Type": "string"}],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "CrawlerSchemaDeserializerVersion": "1.0",
            "CrawlerSchemaSerializerVersion": "1.0",
            "UPDATED_BY_CRAWLER": "test",
            "averageRecordSize": "19",
            "classification": "parquet",
            "compressionType": "none",
            "objectCount": "60",
            "recordCount": "167497743",
            "sizeKey": "4463574900",
            "typeOfData": "file",
        },
        "CreatedBy": "arn:aws:sts::795586375822:assumed-role/AWSGlueServiceRole-test-crawler/AWS-Crawler",
        "IsRegisteredWithLakeFormation": False,
        "CatalogId": "795586375822",
    },
]
get_tables_response_2 = {"TableList": tables_2}
get_jobs_response = {
    "Jobs": [
        {
            "Name": "test-job-1",
            "Description": "The first test job",
            "Role": "arn:aws:iam::123412341234:role/service-role/AWSGlueServiceRole-glue-crawler",
            "CreatedOn": datetime.datetime(2021, 6, 10, 16, 51, 25, 690000),
            "LastModifiedOn": datetime.datetime(2021, 6, 10, 16, 55, 35, 307000),
            "ExecutionProperty": {"MaxConcurrentRuns": 1},
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": "s3://aws-glue-assets-123412341234-us-west-2/scripts/job-1.py",
                "PythonVersion": "3",
            },
            "DefaultArguments": {
                "--TempDir": "s3://aws-glue-assets-123412341234-us-west-2/temporary/",
                "--class": "GlueApp",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-glue-datacatalog": "true",
                "--enable-metrics": "true",
                "--enable-spark-ui": "true",
                "--encryption-type": "sse-s3",
                "--job-bookmark-option": "job-bookmark-enable",
                "--job-language": "python",
                "--spark-event-logs-path": "s3://aws-glue-assets-123412341234-us-west-2/sparkHistoryLogs/",
            },
            "MaxRetries": 3,
            "AllocatedCapacity": 10,
            "Timeout": 2880,
            "MaxCapacity": 10.0,
            "WorkerType": "G.1X",
            "NumberOfWorkers": 10,
            "GlueVersion": "2.0",
        },
        {
            "Name": "test-job-2",
            "Description": "The second test job",
            "Role": "arn:aws:iam::123412341234:role/service-role/AWSGlueServiceRole-glue-crawler",
            "CreatedOn": datetime.datetime(2021, 6, 10, 16, 58, 32, 469000),
            "LastModifiedOn": datetime.datetime(2021, 6, 10, 16, 58, 32, 469000),
            "ExecutionProperty": {"MaxConcurrentRuns": 1},
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": "s3://aws-glue-assets-123412341234-us-west-2/scripts/job-2.py",
                "PythonVersion": "3",
            },
            "DefaultArguments": {
                "--TempDir": "s3://aws-glue-assets-123412341234-us-west-2/temporary/",
                "--class": "GlueApp",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-glue-datacatalog": "true",
                "--enable-metrics": "true",
                "--enable-spark-ui": "true",
                "--encryption-type": "sse-s3",
                "--job-bookmark-option": "job-bookmark-enable",
                "--job-language": "python",
                "--spark-event-logs-path": "s3://aws-glue-assets-123412341234-us-west-2/sparkHistoryLogs/",
            },
            "MaxRetries": 3,
            "AllocatedCapacity": 10,
            "Timeout": 2880,
            "MaxCapacity": 10.0,
            "WorkerType": "G.1X",
            "NumberOfWorkers": 10,
            "GlueVersion": "2.0",
        },
    ]
}
# for job 1
get_dataflow_graph_response_1 = {
    "DagNodes": [
        {
            "Id": "Transform0_job1",
            "NodeType": "Filter",
            "Args": [
                {"Name": "f", "Value": "lambda row : ()", "Param": False},
                {
                    "Name": "transformation_ctx",
                    "Value": '"Transform0"',
                    "Param": False,
                },
            ],
            "LineNumber": 32,
        },
        {
            "Id": "Transform1_job1",
            "NodeType": "ApplyMapping",
            "Args": [
                {
                    "Name": "mappings",
                    "Value": '[("yr", "int", "yr", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string"), ("flightnum", "string", "flightnum", "string"), ("origin", "string", "origin", "string"), ("dest", "string", "dest", "string"), ("depdelay", "int", "depdelay", "int"), ("carrierdelay", "int", "carrierdelay", "int"), ("weatherdelay", "int", "weatherdelay", "int"), ("year", "string", "year", "string")]',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"Transform1"',
                    "Param": False,
                },
            ],
            "LineNumber": 37,
        },
        {
            "Id": "Transform2_job1",
            "NodeType": "ApplyMapping",
            "Args": [
                {
                    "Name": "mappings",
                    "Value": '[("yr", "int", "yr", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string"), ("flightnum", "string", "flightnum", "string"), ("origin", "string", "origin", "string"), ("dest", "string", "dest", "string"), ("depdelay", "int", "depdelay", "int"), ("carrierdelay", "int", "carrierdelay", "int"), ("weatherdelay", "int", "weatherdelay", "int"), ("year", "string", "year", "string")]',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"Transform2"',
                    "Param": False,
                },
            ],
            "LineNumber": 22,
        },
        {
            "Id": "Transform3_job1",
            "NodeType": "Join",
            "Args": [
                {
                    "Name": "keys2",
                    "Value": '["(right) flightdate"]',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"Transform3"',
                    "Param": False,
                },
                {"Name": "keys1", "Value": '["yr"]', "Param": False},
            ],
            "LineNumber": 47,
        },
        {
            "Id": "DataSource0_job1",
            "NodeType": "DataSource",
            "Args": [
                {
                    "Name": "database",
                    "Value": '"flights-database"',
                    "Param": False,
                },
                {"Name": "table_name", "Value": '"avro"', "Param": False},
                {
                    "Name": "transformation_ctx",
                    "Value": '"DataSource0"',
                    "Param": False,
                },
            ],
            "LineNumber": 17,
        },
        {
            "Id": "DataSink0_job1",
            "NodeType": "DataSink",
            "Args": [
                {
                    "Name": "database",
                    "Value": '"test-database"',
                    "Param": False,
                },
                {
                    "Name": "table_name",
                    "Value": '"test_jsons_markers"',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"DataSink0"',
                    "Param": False,
                },
            ],
            "LineNumber": 57,
        },
        {
            "Id": "Transform4_job1",
            "NodeType": "ApplyMapping",
            "Args": [
                {
                    "Name": "mappings",
                    "Value": '[("yr", "int", "yr", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string"), ("flightnum", "string", "flightnum", "string"), ("origin", "string", "origin", "string"), ("dest", "string", "dest", "string"), ("depdelay", "int", "depdelay", "int"), ("carrierdelay", "int", "carrierdelay", "int"), ("weatherdelay", "int", "weatherdelay", "int"), ("year", "string", "year", "string")]',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"Transform4"',
                    "Param": False,
                },
            ],
            "LineNumber": 27,
        },
        {
            "Id": "Transform5_job1",
            "NodeType": "ApplyMapping",
            "Args": [
                {
                    "Name": "mappings",
                    "Value": '[("yr", "int", "(right) yr", "int"), ("flightdate", "string", "(right) flightdate", "string"), ("uniquecarrier", "string", "(right) uniquecarrier", "string"), ("airlineid", "int", "(right) airlineid", "int"), ("carrier", "string", "(right) carrier", "string"), ("flightnum", "string", "(right) flightnum", "string"), ("origin", "string", "(right) origin", "string"), ("dest", "string", "(right) dest", "string"), ("depdelay", "int", "(right) depdelay", "int"), ("carrierdelay", "int", "(right) carrierdelay", "int"), ("weatherdelay", "int", "(right) weatherdelay", "int"), ("year", "string", "(right) year", "string")]',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"Transform5"',
                    "Param": False,
                },
            ],
            "LineNumber": 42,
        },
        {
            "Id": "DataSink1_job1",
            "NodeType": "DataSink",
            "Args": [
                {"Name": "connection_type", "Value": '"s3"', "Param": False},
                {"Name": "format", "Value": '"json"', "Param": False},
                {
                    "Name": "connection_options",
                    "Value": '{"path": "s3://test-glue-jsons/", "partitionKeys": []}',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"DataSink1"',
                    "Param": False,
                },
            ],
            "LineNumber": 52,
        },
    ],
    "DagEdges": [
        {
            "Source": "Transform2_job1",
            "Target": "Transform0_job1",
            "TargetParameter": "frame",
        },
        {
            "Source": "Transform0_job1",
            "Target": "Transform1_job1",
            "TargetParameter": "frame",
        },
        {
            "Source": "DataSource0_job1",
            "Target": "Transform2_job1",
            "TargetParameter": "frame",
        },
        {
            "Source": "Transform4_job1",
            "Target": "Transform3_job1",
            "TargetParameter": "frame1",
        },
    ],
}
# for job 2
get_dataflow_graph_response_2 = {
    "DagNodes": [
        {
            "Id": "Transform0_job2",
            "NodeType": "SplitFields",
            "Args": [
                {
                    "Name": "paths",
                    "Value": '["yr", "quarter", "month", "dayofmonth", "dayofweek", "flightdate", "uniquecarrier"]',
                    "Param": False,
                },
                {
                    "Name": "name2",
                    "Value": '"Transform0Output1"',
                    "Param": False,
                },
                {
                    "Name": "name1",
                    "Value": '"Transform0Output0"',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"Transform0"',
                    "Param": False,
                },
            ],
            "LineNumber": 42,
        },
        {
            "Id": "Transform1_job2",
            "NodeType": "ApplyMapping",
            "Args": [
                {
                    "Name": "mappings",
                    "Value": '[("yr", "int", "yr", "int"), ("quarter", "int", "quarter", "int"), ("month", "int", "month", "int"), ("dayofmonth", "int", "dayofmonth", "int"), ("dayofweek", "int", "dayofweek", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string")]',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"Transform1"',
                    "Param": False,
                },
            ],
            "LineNumber": 22,
        },
        {
            "Id": "Transform2_job2",
            "NodeType": "FillMissingValues",
            "Args": [
                {
                    "Name": "missing_values_column",
                    "Value": '"dayofmonth"',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"Transform2"',
                    "Param": False,
                },
            ],
            "LineNumber": 27,
        },
        {
            "Id": "Transform3_job2",
            "NodeType": "SelectFields",
            "Args": [
                {"Name": "paths", "Value": "[]", "Param": False},
                {
                    "Name": "transformation_ctx",
                    "Value": '"Transform3"',
                    "Param": False,
                },
            ],
            "LineNumber": 32,
        },
        {
            "Id": "DataSource0_job2",
            "NodeType": "DataSource",
            "Args": [
                {
                    "Name": "database",
                    "Value": '"test-database"',
                    "Param": False,
                },
                {
                    "Name": "table_name",
                    "Value": '"test_parquet"',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"DataSource0"',
                    "Param": False,
                },
            ],
            "LineNumber": 17,
        },
        {
            "Id": "DataSink0_job2",
            "NodeType": "DataSink",
            "Args": [
                {"Name": "connection_type", "Value": '"s3"', "Param": False},
                {"Name": "format", "Value": '"json"', "Param": False},
                {
                    "Name": "connection_options",
                    "Value": '{"path": "s3://test-glue-jsons/", "partitionKeys": []}',
                    "Param": False,
                },
                {
                    "Name": "transformation_ctx",
                    "Value": '"DataSink0"',
                    "Param": False,
                },
            ],
            "LineNumber": 37,
        },
    ],
    "DagEdges": [
        {
            "Source": "Transform1_job2",
            "Target": "Transform0_job2",
            "TargetParameter": "frame",
        },
        {
            "Source": "DataSource0_job2",
            "Target": "Transform1_job2",
            "TargetParameter": "frame",
        },
        {
            "Source": "Transform1_job2",
            "Target": "Transform2_job2",
            "TargetParameter": "frame",
        },
        {
            "Source": "Transform2_job2",
            "Target": "Transform3_job2",
            "TargetParameter": "frame",
        },
        {
            "Source": "Transform3_job2",
            "Target": "DataSink0_job2",
            "TargetParameter": "frame",
        },
    ],
}

get_object_body_1 = """
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "flights-database", table_name = "avro", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "flights-database", table_name = "avro", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("yr", "int", "yr", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string"), ("flightnum", "string", "flightnum", "string"), ("origin", "string", "origin", "string"), ("dest", "string", "dest", "string"), ("depdelay", "int", "depdelay", "int"), ("carrierdelay", "int", "carrierdelay", "int"), ("weatherdelay", "int", "weatherdelay", "int"), ("year", "string", "year", "string")], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [frame = DataSource0]
Transform2 = ApplyMapping.apply(frame = DataSource0, mappings = [("yr", "int", "yr", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string"), ("flightnum", "string", "flightnum", "string"), ("origin", "string", "origin", "string"), ("dest", "string", "dest", "string"), ("depdelay", "int", "depdelay", "int"), ("carrierdelay", "int", "carrierdelay", "int"), ("weatherdelay", "int", "weatherdelay", "int"), ("year", "string", "year", "string")], transformation_ctx = "Transform2")
## @type: ApplyMapping
## @args: [mappings = [("yr", "int", "yr", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string"), ("flightnum", "string", "flightnum", "string"), ("origin", "string", "origin", "string"), ("dest", "string", "dest", "string"), ("depdelay", "int", "depdelay", "int"), ("carrierdelay", "int", "carrierdelay", "int"), ("weatherdelay", "int", "weatherdelay", "int"), ("year", "string", "year", "string")], transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [frame = Transform2]
Transform4 = ApplyMapping.apply(frame = Transform2, mappings = [("yr", "int", "yr", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string"), ("flightnum", "string", "flightnum", "string"), ("origin", "string", "origin", "string"), ("dest", "string", "dest", "string"), ("depdelay", "int", "depdelay", "int"), ("carrierdelay", "int", "carrierdelay", "int"), ("weatherdelay", "int", "weatherdelay", "int"), ("year", "string", "year", "string")], transformation_ctx = "Transform4")
## @type: Filter
## @args: [f = lambda row : (), transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = Transform2]
Transform0 = Filter.apply(frame = Transform2, f = lambda row : (), transformation_ctx = "Transform0")
## @type: ApplyMapping
## @args: [mappings = [("yr", "int", "yr", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string"), ("flightnum", "string", "flightnum", "string"), ("origin", "string", "origin", "string"), ("dest", "string", "dest", "string"), ("depdelay", "int", "depdelay", "int"), ("carrierdelay", "int", "carrierdelay", "int"), ("weatherdelay", "int", "weatherdelay", "int"), ("year", "string", "year", "string")], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = Transform0]
Transform1 = ApplyMapping.apply(frame = Transform0, mappings = [("yr", "int", "yr", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string"), ("flightnum", "string", "flightnum", "string"), ("origin", "string", "origin", "string"), ("dest", "string", "dest", "string"), ("depdelay", "int", "depdelay", "int"), ("carrierdelay", "int", "carrierdelay", "int"), ("weatherdelay", "int", "weatherdelay", "int"), ("year", "string", "year", "string")], transformation_ctx = "Transform1")
## @type: ApplyMapping
## @args: [mappings = [("yr", "int", "(right) yr", "int"), ("flightdate", "string", "(right) flightdate", "string"), ("uniquecarrier", "string", "(right) uniquecarrier", "string"), ("airlineid", "int", "(right) airlineid", "int"), ("carrier", "string", "(right) carrier", "string"), ("flightnum", "string", "(right) flightnum", "string"), ("origin", "string", "(right) origin", "string"), ("dest", "string", "(right) dest", "string"), ("depdelay", "int", "(right) depdelay", "int"), ("carrierdelay", "int", "(right) carrierdelay", "int"), ("weatherdelay", "int", "(right) weatherdelay", "int"), ("year", "string", "(right) year", "string")], transformation_ctx = "Transform5"]
## @return: Transform5
## @inputs: [frame = Transform1]
Transform5 = ApplyMapping.apply(frame = Transform1, mappings = [("yr", "int", "(right) yr", "int"), ("flightdate", "string", "(right) flightdate", "string"), ("uniquecarrier", "string", "(right) uniquecarrier", "string"), ("airlineid", "int", "(right) airlineid", "int"), ("carrier", "string", "(right) carrier", "string"), ("flightnum", "string", "(right) flightnum", "string"), ("origin", "string", "(right) origin", "string"), ("dest", "string", "(right) dest", "string"), ("depdelay", "int", "(right) depdelay", "int"), ("carrierdelay", "int", "(right) carrierdelay", "int"), ("weatherdelay", "int", "(right) weatherdelay", "int"), ("year", "string", "(right) year", "string")], transformation_ctx = "Transform5")
## @type: Join
## @args: [keys2 = ["(right) flightdate"], keys1 = ["yr"], transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [frame1 = Transform4, frame2 = Transform5]
Transform3 = Join.apply(frame1 = Transform4, frame2 = Transform5, keys2 = ["(right) flightdate"], keys1 = ["yr"], transformation_ctx = "Transform3")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://test-glue-jsons/", "partitionKeys": []}, transformation_ctx = "DataSink1"]
## @return: DataSink1
## @inputs: [frame = Transform3]
DataSink1 = glueContext.write_dynamic_frame.from_options(frame = Transform3, connection_type = "s3", format = "json", connection_options = {"path": "s3://test-glue-jsons/", "partitionKeys": []}, transformation_ctx = "DataSink1")
## @type: DataSink
## @args: [database = "test-database", table_name = "test_jsons_markers", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform3]
DataSink0 = glueContext.write_dynamic_frame.from_catalog(frame = Transform3, database = "test-database", table_name = "test_jsons_markers", transformation_ctx = "DataSink0")
job.commit()
"""

get_object_body_2 = """
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import FillMissingValues

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "test-database", table_name = "test_parquet", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "test-database", table_name = "test_parquet", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("yr", "int", "yr", "int"), ("quarter", "int", "quarter", "int"), ("month", "int", "month", "int"), ("dayofmonth", "int", "dayofmonth", "int"), ("dayofweek", "int", "dayofweek", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string")], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = DataSource0]
Transform1 = ApplyMapping.apply(frame = DataSource0, mappings = [("yr", "int", "yr", "int"), ("quarter", "int", "quarter", "int"), ("month", "int", "month", "int"), ("dayofmonth", "int", "dayofmonth", "int"), ("dayofweek", "int", "dayofweek", "int"), ("flightdate", "string", "flightdate", "string"), ("uniquecarrier", "string", "uniquecarrier", "string"), ("airlineid", "int", "airlineid", "int"), ("carrier", "string", "carrier", "string")], transformation_ctx = "Transform1")
## @type: FillMissingValues
## @args: [missing_values_column = "dayofmonth", transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [frame = Transform1]
Transform2 = FillMissingValues.apply(frame = Transform1, missing_values_column = "dayofmonth", transformation_ctx = "Transform2")
## @type: SelectFields
## @args: [paths = [], transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [frame = Transform2]
Transform3 = SelectFields.apply(frame = Transform2, paths = [], transformation_ctx = "Transform3")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://test-glue-jsons/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform3]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform3, connection_type = "s3", format = "json", connection_options = {"path": "s3://test-glue-jsons/", "partitionKeys": []}, transformation_ctx = "DataSink0")
## @type: SplitFields
## @args: [paths = ["yr", "quarter", "month", "dayofmonth", "dayofweek", "flightdate", "uniquecarrier", "airlineid", "carrier"], name2 = "Transform0Output1", name1 = "Transform0Output0", transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = Transform1]
Transform0 = SplitFields.apply(frame = Transform1, paths = ["yr", "quarter", "month", "dayofmonth", "dayofweek", "flightdate", "uniquecarrier", "airlineid", "carrier"], name2 = "Transform0Output1", name1 = "Transform0Output0", transformation_ctx = "Transform0")
job.commit()
"""


def mock_get_object_response(raw_body: str) -> Dict[str, Any]:
    """
    Mock s3 client get_object() response object.

    See https://gist.github.com/grantcooksey/132ddc85274a50b94b821302649f9d7b

    Parameters
    ----------
        raw_body:
            Content of the 'Body' field to return
    """

    encoded_message = raw_body.encode("utf-8")
    raw_stream = StreamingBody(io.BytesIO(encoded_message), len(encoded_message))

    return {"Body": raw_stream}


def get_object_response_1() -> Dict[str, Any]:
    return mock_get_object_response(get_object_body_1)


def get_object_response_2() -> Dict[str, Any]:
    return mock_get_object_response(get_object_body_2)


def get_bucket_tagging() -> Dict[str, Any]:
    return {"TagSet": [{"Key": "foo", "Value": "bar"}]}


def get_object_tagging() -> Dict[str, Any]:
    return {"TagSet": [{"Key": "baz", "Value": "bob"}]}
