# Adding a custom Dataset Data Platform

A Data Platform represents a 3rd party system from which [Metadata Entities](https://datahubproject.io/docs/metadata-modeling/metadata-model/) are ingested from. Each Dataset that is ingested is associated with a single platform, for example MySQL, Snowflake, Redshift, or BigQuery.

There are some cases in which you may want to add a custom Data Platform identifier for a Dataset. For example,
you have an internal data system that is not widely available, or you're using a Data Platform that is not natively supported by DataHub.

To do so, you can either change the default Data Platforms that are ingested into DataHub *prior to deployment time*, or ingest
a new Data Platform at runtime. You can use the first option if you're able to periodically merge new Data Platforms from the OSS
repository into your own. It will cause the custom Data Platform to be re-ingested each time you deploy DataHub, meaning that
your custom Data Platform will persist even between full cleans (nukes) of DataHub. 

## Changing Default Data Platforms

Simply make a change to the [data_platforms.json](https://github.com/datahub-project/datahub/blob/master/metadata-service/war/src/main/resources/boot/data_platforms.json) 
file to add a custom Data Platform:

```
[ 
  .....
  {
    "urn": "urn:li:dataPlatform:MyCustomDataPlatform",
    "aspect": {
      "name": "My Custom Data Platform",
      "type": "OTHERS",
      "logoUrl": "https://<your-logo-url>"
    }
  }
]
```

## Ingesting Data Platform at runtime

You can also ingest a Data Platform at runtime using either a file-based ingestion source, or using a normal curl to the
[GMS Rest.li APIs](https://datahubproject.io/docs/metadata-service#restli-api). 

### Using File-Based Ingestion Recipe

**Step 1** Define a JSON file containing your custom Data Platform

```
// my-custom-data-platform.json 
[
  {
    "auditHeader": null,
    "proposedSnapshot": {
      "com.linkedin.pegasus2avro.metadata.snapshot.DataPlatformSnapshot": {
        "urn": "urn:li:dataPlatform:MyCustomDataPlatform",
        "aspects": [
          {
            "com.linkedin.pegasus2avro.dataplatform.DataPlatformInfo": {
              "datasetNameDelimiter": "/",
              "name": "My Custom Data Platform",
              "type": "OTHERS",
              "logoUrl": "https://<your-logo-url>"
            }
          }
        ]
      }
    },
    "proposedDelta": null
  }
]
```

**Step 2**: Define an [ingestion recipe](https://datahubproject.io/docs/metadata-ingestion/#recipes) 

```
---
# see https://datahubproject.io/docs/metadata-ingestion/source_docs/file for complete documentation
source:
  type: "file"
  config:
    filename: "./my-custom-data-platform.json"

# see https://datahubproject.io/docs/metadata-ingestion/sink_docs/datahub for complete documentation
sink:
  ... 
```

### Using Rest.li API

You can also issue a normal curl request to the Rest.li `/entities` API to add a custom Data Platform.

```
curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
   "entity":{
      "value":{
         "com.linkedin.metadata.snapshot.DataPlatformSnapshot":{
            "aspects":[
               {
                  "com.linkedin.dataplatform.DataPlatformInfo":{
                      "datasetNameDelimiter": "/",
                      "name": "My Custom Data Platform",
                      "type": "OTHERS",
                      "logoUrl": "https://<your-logo-url>"
                  }
               }
            ],
            "urn":"urn:li:dataPlatform:MyCustomDataPlatform"
         }
      }
   }
}'
```