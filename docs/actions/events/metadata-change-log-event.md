# Metadata Change Log Event V1 

## Event Type

`MetadataChangeLog_v1`

## Overview

This event is emitted when any aspect on DataHub Metadata Graph is changed. This includes creates, updates, and removals of both "versioned" aspects and "time-series" aspects.

> Disclaimer: This event is quite powerful, but also quite low-level. Because it exposes the underlying metadata model directly, it is subject to more frequent structural and semantic changes than the higher level [Entity Change Event](entity-change-event.md). We recommend using that event instead to achieve your use case when possible. 

## Event Structure

The fields include

| Name                            | Type   | Description                                                                                                                                                                                                                                            | Optional |
|---------------------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| entityUrn                       | String | The unique identifier for the Entity being changed. For example, a Dataset's urn.                                                                                                                                                                      | False    |
| entityType                      | String | The type of the entity being changed. Supported values include dataset, chart, dashboard, dataFlow (Pipeline), dataJob (Task), domain, tag, glossaryTerm, corpGroup, & corpUser.                                                                       | False    |
| entityKeyAspect                 | Object | The key struct of the entity that was changed. Only present if the Metadata Change Proposal contained the raw key struct.                                                                                                                              | True     |
| changeType                      | String | The change type. UPSERT or DELETE are currently supported.                                                                                                                                                                                             | False    |
| aspectName                      | String | The entity aspect which was changed.                                                                                                                                                                                                                   | False    |
| aspect                          | Object | The new aspect value. Null if the aspect was deleted.                                                                                                                                                                                                  | True     |
| aspect.contentType              | String | The serialization type of the aspect itself. The only supported value is `application/json`.                                                                                                                                                           | False    |
| aspect.value                    | String | The serialized aspect. This is a JSON-serialized representing the aspect document originally defined in PDL. See https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin for more.                        | False    |
| previousAspectValue             | Object | The previous aspect value. Null if the aspect did not exist previously.                                                                                                                                                                                | True     |
| previousAspectValue.contentType | String | The serialization type of the aspect itself. The only supported value is  `application/json`                                                                                                                                                           | False    |
| previousAspectValue.value       | String | The serialized aspect. This is a JSON-serialized representing the aspect document originally defined in PDL. See https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin for more.                        | False    |
| systemMetadata                  | Object | The new system metadata. This includes the the ingestion run-id, model registry and more. For the full structure, see https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/SystemMetadata.pdl      | True     |
| previousSystemMetadata          | Object | The previous system metadata. This includes the the ingestion run-id, model registry and more. For the full structure, see https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/SystemMetadata.pdl | True     |
| created                         | Object | Audit stamp about who triggered the Metadata Change and when.                                                                                                                                                                                          | False    |
| created.time                    | Number | The timestamp in milliseconds when the aspect change occurred.                                                                                                                                                                                         | False    |
| created.actor                   | String | The URN of the actor (e.g. corpuser) that triggered the change.    


### Sample Events

#### Tag Change Event

```json
{
    "entityType": "container",
    "entityUrn": "urn:li:container:DATABASE",
    "entityKeyAspect": null,
    "changeType": "UPSERT",
    "aspectName": "globalTags",
    "aspect": {
        "value": "{\"tags\":[{\"tag\":\"urn:li:tag:pii\"}]}",
        "contentType": "application/json"
    },
    "systemMetadata": {
        "lastObserved": 1651516475595,
        "runId": "no-run-id-provided",
        "registryName": "unknownRegistry",
        "registryVersion": "0.0.0.0-dev",
        "properties": null
    },
    "previousAspectValue": null,
    "previousSystemMetadata": null,
    "created": {
        "time": 1651516475594,
        "actor": "urn:li:corpuser:datahub",
        "impersonator": null
    }
}
```

#### Glossary Term Change Event

```json
{
    "entityType": "dataset",
    "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
    "entityKeyAspect": null,
    "changeType": "UPSERT",
    "aspectName": "glossaryTerms",
    "aspect": {
        "value": "{\"auditStamp\":{\"actor\":\"urn:li:corpuser:datahub\",\"time\":1651516599479},\"terms\":[{\"urn\":\"urn:li:glossaryTerm:CustomerAccount\"}]}",
        "contentType": "application/json"
    },
    "systemMetadata": {
        "lastObserved": 1651516599486,
        "runId": "no-run-id-provided",
        "registryName": "unknownRegistry",
        "registryVersion": "0.0.0.0-dev",
        "properties": null
    },
    "previousAspectValue": null,
    "previousSystemMetadata": null,
    "created": {
        "time": 1651516599480,
        "actor": "urn:li:corpuser:datahub",
        "impersonator": null
    }
}
```

#### Owner Change Event

```json
{
    "auditHeader": null,
    "entityType": "dataset",
    "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
    "entityKeyAspect": null,
    "changeType": "UPSERT",
    "aspectName": "ownership",
    "aspect": {
        "value": "{\"owners\":[{\"type\":\"DATAOWNER\",\"owner\":\"urn:li:corpuser:datahub\"}],\"lastModified\":{\"actor\":\"urn:li:corpuser:datahub\",\"time\":1651516640488}}",
        "contentType": "application/json"
    },
    "systemMetadata": {
        "lastObserved": 1651516640493,
        "runId": "no-run-id-provided",
        "registryName": "unknownRegistry",
        "registryVersion": "0.0.0.0-dev",
        "properties": null
    },
    "previousAspectValue": {
        "value": "{\"owners\":[{\"owner\":\"urn:li:corpuser:jdoe\",\"type\":\"DATAOWNER\"},{\"owner\":\"urn:li:corpuser:datahub\",\"type\":\"DATAOWNER\"}],\"lastModified\":{\"actor\":\"urn:li:corpuser:jdoe\",\"time\":1581407189000}}",
        "contentType": "application/json"
    },
    "previousSystemMetadata": {
        "lastObserved": 1651516415088,
        "runId": "file-2022_05_02-11_33_35",
        "registryName": null,
        "registryVersion": null,
        "properties": null
    },
    "created": {
        "time": 1651516640490,
        "actor": "urn:li:corpuser:datahub",
        "impersonator": null
    }
}
```
## FAQ

### Where can I find all the aspects and their schemas?

Great Question! All MetadataChangeLog events are based on the Metadata Model which is comprised of Entities,
Aspects, and Relationships which make up an enterprise Metadata Graph. We recommend checking out the following
resources to learn more about this:

- [Intro to Metadata Model](https://datahubproject.io/docs/metadata-modeling/metadata-model)

You can also find a comprehensive list of Entities + Aspects of the Metadata Model under the **Metadata Modeling > Entities** section of the [official DataHub docs](https://datahubproject.io/docs/). 



