# How to onboard to Strongly Consistent Secondary Index (SCSI)?

## 1. Create urn path extractor for your entity
This is to provide the parts of urn that need to be indexed as well as the logic to obtain the same from the urn. Refer to [DatasetUrnPathExtractor](https://github.com/linkedin/datahub/tree/master/gms/impl/src/main/java/com/linkedin/metadata/urn/dataset/DatasetUrnPathExtractor.java) as an example.

## 2. Add appropriate docker environment variable to enable SCSI for your entity
Enable SCSI by adding your variable in docker environment [file](https://github.com/linkedin/datahub/tree/master/docker/datahub-gms/env/docker.env) of datahub-gms. Each entity has it's own environment variable. If corresponding variable of your entity is already defined in the docker environment file, then make sure it is set (in order to enable SCSI).

## 3. Enable SCSI in local DAO
Import the docker environment variable in your local DAO factory to enable SCSI. Refer to [DatasetDaoFactory](https://github.com/linkedin/datahub/tree/master/gms/factories/src/main/java/com/linkedin/gms/factory/dataset/DatasetDaoFactory.java) as an example.

## 4. Define Storage Config and use while instantiating your DAO
Other than the urn parts, you may want to index certain fields of an aspect. The indexable fields of aspects of a given entity are configured in a file in JSON format which must be provided during your local DAO instantiation. Refer to the storage config for [dataset](https://github.com/linkedin/datahub/tree/master/gms/factories/src/main/resources/datasetStorageConfig.json).

## 5. Bootstrap index table for existing urns
If you have already enabled SCSI then the write path will ensure that every new urn inserted into the primary document store (i.e. `metadata_aspect` table), also gets inserted into the index table. However for urns that already exist in the `metadata_aspect` table, you will need to bootstrap the index table. Refer to the bootstrap [script](https://github.com/linkedin/datahub/tree/master/datahub/gms/database/scripts/index/dataset-bootstrap.sql) for datasets as an example.

## 6. Add finder method at the resource level
[BaseEntityResource](https://github.com/linkedin/datahub-gma/blob/master/restli-resources/src/main/java/com/linkedin/metadata/restli/BaseEntityResource.java) currently exposes Finder resource method called filter that returns a list of entities that satisfy the filter conditions specified in query parameters. Please refer to [Datasets](https://github.com/linkedin/datahub/blob/master/gms/impl/src/main/java/com/linkedin/metadata/resources/dataset/Datasets.java) resource to understand how to override the filter method.
Once you have the resource method defined, you could as well expose client methods that take different input arguments. Please refer to listUrnsFromIndex and filter methods in [Datasets](https://github.com/linkedin/datahub/blob/master/gms/client/src/main/java/com/linkedin/dataset/client/Datasets.java) client for reference.

Once you have onboarded to SCSI for your entity, you can test the changes as described below

## Testing your changes with some sample API calls

For the steps below, we assume you have already enabled SCSI by following the steps mentioned above.

### Ingestion
Run the ingestion script if you haven't already using
    ```
    ./docker/ingestion/ingestion.sh
    ```
Connect to the MySQL server and you should be able to see the records.
```
mysql> select * from metadata_index;
+----+--------------------------------------------------------------------+------------------------------------+------------------------+---------+---------------------------+-----------+
| id | urn                                                                | aspect                             | path                   | longVal | stringVal                 | doubleVal |
+----+--------------------------------------------------------------------+------------------------------------+------------------------+---------+---------------------------+-----------+
|  1 | urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD) | com.linkedin.common.urn.DatasetUrn | /platform/platformName |    NULL | kafka                     |      NULL |
|  2 | urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD) | com.linkedin.common.urn.DatasetUrn | /origin                |    NULL | PROD                      |      NULL |
|  3 | urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD) | com.linkedin.common.urn.DatasetUrn | /datasetName           |    NULL | SampleKafkaDataset        |      NULL |
|  4 | urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD) | com.linkedin.common.urn.DatasetUrn | /platform              |    NULL | urn:li:dataPlatform:kafka |      NULL |
|  5 | urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)   | com.linkedin.common.urn.DatasetUrn | /platform/platformName |    NULL | hdfs                      |      NULL |
|  6 | urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)   | com.linkedin.common.urn.DatasetUrn | /origin                |    NULL | PROD                      |      NULL |
|  7 | urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)   | com.linkedin.common.urn.DatasetUrn | /datasetName           |    NULL | SampleHdfsDataset         |      NULL |
|  8 | urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)   | com.linkedin.common.urn.DatasetUrn | /platform              |    NULL | urn:li:dataPlatform:hdfs  |      NULL |
|  9 | urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)   | com.linkedin.common.urn.DatasetUrn | /platform/platformName |    NULL | hive                      |      NULL |
| 10 | urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)   | com.linkedin.common.urn.DatasetUrn | /origin                |    NULL | PROD                      |      NULL |
| 11 | urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)   | com.linkedin.common.urn.DatasetUrn | /datasetName           |    NULL | SampleHiveDataset         |      NULL |
| 12 | urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)   | com.linkedin.common.urn.DatasetUrn | /platform              |    NULL | urn:li:dataPlatform:hive  |      NULL |
+----+--------------------------------------------------------------------+------------------------------------+------------------------+---------+---------------------------+-----------+
12 rows in set (0.01 sec)
```

In the following section we will try some API calls, now that the urn parts are ingested

### Get list of dataset urns
Note that the results are paginated

```
curl "http://localhost:8080/datasets?q=filter&aspects=List()" -X GET -H 'X-RestLi-Protocol-Version: 2.0.0' -H 'X-RestLi-Method: finder' | jq

{
  "elements": [
    {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
      "origin": "PROD",
      "name": "SampleHdfsDataset",
      "platform": "urn:li:dataPlatform:hdfs"
    },
    {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
      "origin": "PROD",
      "name": "SampleHiveDataset",
      "platform": "urn:li:dataPlatform:hive"
    },
    {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
      "origin": "PROD",
      "name": "SampleKafkaDataset",
      "platform": "urn:li:dataPlatform:kafka"
    }
  ],
  "paging": {
    "count": 10,
    "start": 0,
    "links": []
  }
}
```

### Get list of dataset urns after a given urn

```
curl "http://localhost:8080/datasets?q=filter&aspects=List()&urn=urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahdfs%2CSampleHdfsDataset%2CPROD%29" -X GET -H 'X-RestLi-Protocol-Version: 2.0.0' -H 'X-RestLi-Method: finder' | jq

{
  "elements": [
    {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
      "origin": "PROD",
      "name": "SampleHiveDataset",
      "platform": "urn:li:dataPlatform:hive"
    },
    {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
      "origin": "PROD",
      "name": "SampleKafkaDataset",
      "platform": "urn:li:dataPlatform:kafka"
    }
  ],
  "paging": {
    "count": 10,
    "start": 0,
    "links": []
  }
}
```