# Metadata Events

DataHub makes use a few important Kafka events for operation. The most notable of these include

1. Metadata Change Proposal
2. Metadata Change Log (Versioned + Timeseries) 
3. Platform Event

Each event is originally authored using [PDL]( https://linkedin.github.io/rest.li/DATA-Data-Schema-and-Templates), a modeling language developed by LinkedIn, and 
then converted into their Avro equivalents, which are used when writing and reading the events to Kafka. 

In the document, we'll describe each of these events in detail - including notes about their structure & semantics. 

## Metadata Change Proposal (MCP)

A Metadata Change Proposal represents a request to change to a specific [aspect](aspect.md) on an enterprise's Metadata
Graph. Each MCP provides a new value for a given aspect. For example, a single MCP can 
be emitted to change ownership or documentation or domains or deprecation status for a data asset.

### Emission

MCPs may be emitted by clients of DataHub's low-level ingestion APIs (e.g. ingestion sources)
during the process of metadata ingestion. The DataHub Python API exposes an interface for 
easily sending MCPs into DataHub. 

The default Kafka topic name for MCPs is `MetadataChangeProposal_v1`.

### Consumption

DataHub's storage layer actively listens for new Metadata Change Proposals, attempts
to apply the requested change to the Metadata Graph. 

### Schema

| Name                            | Type   | Description                                                                                                                                                                                                                                            | Optional |
|---------------------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| entityUrn                       | String | The unique identifier for the Entity being changed. For example, a Dataset's urn.                                                                                                                                                                      | False    |
| entityType                      | String | The type of the entity the new aspect is associated with. This corresponds to the entity name in the DataHub Entity Registry, for example 'dataset'.                                                                                                   | False    |
| entityKeyAspect                 | Object | The key struct of the entity that was changed. Only present if the Metadata Change Proposal contained the raw key struct.                                                                                                                              | True     |
| changeType                      | String | The change type. CREATE, UPSERT and DELETE are currently supported.                                                                                                                                                                                    | False    |
| aspectName                      | String | The entity aspect which was changed.                                                                                                                                                                                                                   | False    |
| aspect                          | Object | The new aspect value. Null if the aspect was deleted.                                                                                                                                                                                                  | True     |
| aspect.contentType              | String | The serialization type of the aspect itself. The only supported value is `application/json`.                                                                                                                                                           | False    |
| aspect.value                    | String | The serialized aspect. This is a JSON-serialized representing the aspect document originally defined in PDL. See https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin for more.                        | False    |
| systemMetadata                  | Object | The new system metadata. This includes the the ingestion run-id, model registry and more. For the full structure, see https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/SystemMetadata.pdl      | True     |

The PDL schema can be found [here](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/MetadataChangeProposal.pdl). 

### Examples

An MCP representing a request to update the 'ownership' aspect for a particular Dataset:

```json
{
  "entityType": "dataset",
  "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
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
  }
}
```

Note how the aspect payload is serialized as JSON inside the "value" field. The exact structure
of the aspect is determined by its PDL schema. (For example, the [ownership](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Ownership.pdl) schema)

## Metadata Change Log (MCL)

A Metadata Change Log represents *any* change which has been made to the Metadata Graph.
Metadata Change Log events are emitted to Kafka immediately after writing the change
the durable storage. 

There are 2 flavors of Metadata Change Log: *versioned* and *timeseries*. These correspond to the type
of aspects which were updated for a given change. **Versioned** aspects are those
which represent the "latest" state of some attributes, for example the most recent owners of an asset
or its documentation. **Timeseries** aspects are those which represent events related to an asset
that occurred at a particular time, for example profiling of a Dataset. 

### Emission

MCLs are emitted when *any* change is made to an entity on the DataHub Metadata Graph, this includes
writing to any aspect of an entity. 

Two distinct topics are maintained for Metadata Change Log. The default Kafka topic name for **versioned** aspects is `MetadataChangeLog_Versioned_v1` and for
**timeseries** aspects is `MetadataChangeLog_Timeseries_v1`.

### Consumption

DataHub ships with a Kafka Consumer Job (mae-consumer-job) which listens for MCLs and uses them to update DataHub's search and graph indices,
as well as to generate derived Platform Events (described below). 

In addition, the [Actions Framework](../actions/README.md) consumes Metadata Change Logs to power its [Metadata Change Log](../actions/events/metadata-change-log-event.md) event API.

### Schema

| Name                            | Type   | Description                                                                                                                                                                                                                                            | Optional |
|---------------------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| entityUrn                       | String | The unique identifier for the Entity being changed. For example, a Dataset's urn.                                                                                                                                                                      | False    |
| entityType                      | String | The type of the entity the new aspect is associated with. This corresponds to the entity name in the DataHub Entity Registry, for example 'dataset'.                                                                                                   | False    |
| entityKeyAspect                 | Object | The key struct of the entity that was changed. Only present if the Metadata Change Proposal contained the raw key struct.                                                                                                                              | True     |
| changeType                      | String | The change type. CREATE, UPSERT and DELETE are currently supported.                                                                                                                                                                                    | False    |
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

The PDL schema for can be found [here](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/MetadataChangeLog.pdl).

### Examples

An MCL corresponding to a change in the 'ownership' aspect for a particular Dataset:

```json
{
    "entityType": "dataset",
    "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
    "changeType": "UPSERT",
    "aspectName": "ownership",
    "aspect": {
        "value": "{\"owners\":[{\"type\":\"DATAOWNER\",\"owner\":\"urn:li:corpuser:datahub\"}],\"lastModified\":{\"actor\":\"urn:li:corpuser:datahub\",\"time\":1651516640488}}",
        "contentType": "application/json"
    },
    "previousAspectValue": {
        "value": "{\"owners\":[{\"owner\":\"urn:li:corpuser:jdoe\",\"type\":\"DATAOWNER\"},{\"owner\":\"urn:li:corpuser:datahub\",\"type\":\"DATAOWNER\"}],\"lastModified\":{\"actor\":\"urn:li:corpuser:jdoe\",\"time\":1581407189000}}",
        "contentType": "application/json"
    },
    "systemMetadata": {
        "lastObserved": 1651516640493,
        "runId": "no-run-id-provided",
        "registryName": "unknownRegistry",
        "registryVersion": "0.0.0.0-dev",
        "properties": null
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

Note how the aspect payload is serialized as JSON inside the "value" field. The exact structure
of the aspect is determined by its PDL schema. (For example, the [ownership](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Ownership.pdl) schema)


## Platform Event (PE)

A Platform Event represents an arbitrary business-logic event emitted by DataHub. Each
Platform Event has a `name` which determines its contents.

### Types

- **Entity Change Event** (entityChangeEvent): The most important Platform Event is named **Entity Change Event**, and represents a log of semantic changes
(tag addition, removal, deprecation change, etc) that have occurred on DataHub. It is used an important
  component of the DataHub Actions Framework. 

All registered Platform Event types are declared inside the DataHub Entity Registry (`entity-registry.yml`). 

### Emission

All Platform Events are generated by DataHub itself during normal operation. 

PEs are extremely dynamic - they can contain arbitrary payloads depending on the `name`. Thus,
can be emitted in a variety of circumstances. 

The default Kafka topic name for all Platform Events is `PlatformEvent_v1`.

### Consumption

The [Actions Framework](../actions/README.md) consumes Platform Events to power its [Entity Change Event](../actions/events/entity-change-event.md) API.

### Schema

| Name                            | Type   | Description                                                                                                                                                                                                                                            | Optional |
|---------------------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| header                       | Object | Header fields                                                                                                                                                                 | False    |
| header.timestampMillis                      | Long | The time at which the event was generated.                                                                                                      | False    |
| name                      | String | The name / type of the event.                                                                                                                                                                                                                 | False    |
| payload                          | Object | The event itself.                                                                                                                                                                                                  | False     |
| payload.contentType              | String | The serialization type of the event payload. The only supported value is `application/json`.                                                                                                                                                           | False    |
| payload.value                    | String | The serialized payload. This is a JSON-serialized representing the payload document originally defined in PDL. See https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin for more.                        | False    |


The full PDL schema can be found [here](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/PlatformEvent.pdl).

### Examples

An example of an 'Entity Change Event' Platform Event that is emitted when a new owner is added to a Dataset:

```json
{
  "header": {
    "timestampMillis": 1655390732551
  },
  "name": "entityChangeEvent",
  "payload": {
    "value": "{\"entityUrn\":\"urn:li:dataset:abc\",\"entityType\":\"dataset\",\"category\":\"OWNER\",\"operation\":\"ADD\",\"modifier\":\"urn:li:corpuser:jdoe\",\"parameters\":{\"ownerUrn\":\"urn:li:corpuser:jdoe\",\"ownerType\":\"BUSINESS_OWNER\"},\"auditStamp\":{\"actor\":\"urn:li:corpuser:jdoe\",\"time\":1649953100653}}",
    "contentType": "application/json"
}
```

Note how the actual payload for the event is serialized as JSON inside the 'payload' field. The exact
structure of the Platform Event is determined by its PDL schema. (For example, the [Entity Change Event](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/platform/event/v1/EntityChangeEvent.pdl) schema)

## Failed Metadata Change Proposal (FMCP)

When a Metadata Change Proposal cannot be processed successfully, the event is written to a [dead letter queue](https://en.wikipedia.org/wiki/Dead_letter_queue)
in an event called Failed Metadata Change Proposal (FMCP).

The event simply wraps the original Metadata Change Proposal and an error message, which contains the reason for rejection.
This event can be used for debugging any potential ingestion issues, as well as for re-playing any previous rejected proposal if necessary.

### Emission

FMCEs are emitted when MCEs cannot be successfully committed to DataHub's storage layer.

The default Kafka topic name for FMCPs is `FailedMetadataChangeProposal_v1`.

### Consumption

No active consumers.

### Schema

The PDL schema can be found [here](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/FailedMetadataChangeProposal.pdl).


# Deprecated Events

DataHub ships with a set of deprecated events, which were historically used for proposing and logging
changes to the Metadata Graph. 

Each event in this category was deprecated due to its inflexibility - namely the fact that
the schemas had to be updated when a new aspect was introduced. These events have since been replaced
by the more flexible events described above (Metadata Change Proposal, Metadata Change Log). 

It is not recommended to build dependencies on deprecated events.

## Metadata Change Event (MCE)

A Metadata Change Event represents a request to change multiple aspects for the same entity.
It leverages a deprecated concept of `Snapshot`, which is a strongly-typed list of aspects for the same
entity. 

A MCE is a "proposal" for a set of metadata changes, as opposed to [MAE](#metadata-audit-event), which is conveying a committed change.
Consequently, only successfully accepted and processed MCEs will lead to the emission of a corresponding MAE / MCLs.

### Emission

MCEs may be emitted by clients of DataHub's low-level ingestion APIs (e.g. ingestion sources)
during the process of metadata ingestion.

The default Kafka topic name for MCEs is `MetadataChangeEvent_v4`.

### Consumption

DataHub's storage layer actively listens for new Metadata Change Events, attempts
to apply the requested changes to the Metadata Graph.

### Schema

The PDL schema can be found [here](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/MetadataChangeEvent.pdl).

### Examples

An example of an MCE emitted to change the 'ownership' aspect for an Entity:

```json
{
  "proposedSnapshot": {
    "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
      "aspects": [
        {
          "com.linkedin.pegasus2avro.common.Ownership": {
            "owners": [
              {
                "owner": "urn:li:corpuser:jdoe",
                "type": "DATAOWNER",
                "source": null
              },
              {
                "owner": "urn:li:corpuser:datahub",
                "type": "DATAOWNER",
                "source": null
              }
            ],
            "lastModified": {
              "time": 1581407189000,
              "actor": "urn:li:corpuser:jdoe",
              "impersonator": null
            }
          }
        }
      ]
    }
  }
}
```

## Metadata Audit Event (MAE)

A Metadata Audit Event captures changes made to one or multiple metadata [aspects](aspect.md) associated with a particular [entity](entity.md), in the form of a metadata [snapshot](snapshot.md) (deprecated) before the change, and a metadata snapshot after the change.

Every source-of-truth for a particular metadata aspect is expected to emit a MAE whenever a change is committed to that aspect. By ensuring that, any listener of MAE will be able to construct a complete view of the latest state for all aspects. 
Furthermore, because each MAE contains the "after image", any mistake made in emitting the MAE can be easily mitigated by emitting a follow-up MAE with the correction. By the same token, the initial bootstrap problem for any newly added entity can also be solved by emitting a MAE containing all the latest metadata aspects associated with that entity.

### Emission

> Note: In recent versions of DataHub (mid 2022), MAEs are no longer actively emitted, and will soon be completely removed from DataHub.
> Use Metadata Change Log instead. 

MAEs are emitted once any metadata change has been successfully committed into DataHub's storage
layer. 

The default Kafka topic name for MAEs is `MetadataAuditEvent_v4`.

### Consumption

No active consumers.

### Schema

The PDL schema can be found [here](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/MetadataAuditEvent.pdl). 

### Examples

An example of an MAE emitted representing a change made to the 'ownership' aspect for an Entity (owner removed):

```json
{
  "oldSnapshot": {
    "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
      "aspects": [
        {
          "com.linkedin.pegasus2avro.common.Ownership": {
            "owners": [
              {
                "owner": "urn:li:corpuser:jdoe",
                "type": "DATAOWNER",
                "source": null
              },
              {
                "owner": "urn:li:corpuser:datahub",
                "type": "DATAOWNER",
                "source": null
              }
            ],
            "lastModified": {
              "time": 1581407189000,
              "actor": "urn:li:corpuser:jdoe",
              "impersonator": null
            }
          }
        }
      ]
    }
  },
  "newSnapshot": {
    "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
      "aspects": [
        {
          "com.linkedin.pegasus2avro.common.Ownership": {
            "owners": [
              {
                "owner": "urn:li:corpuser:datahub",
                "type": "DATAOWNER",
                "source": null
              }
            ],
            "lastModified": {
              "time": 1581407189000,
              "actor": "urn:li:corpuser:jdoe",
              "impersonator": null
            }
          }
        }
      ]
    }
  }
}
```


## Failed Metadata Change Event (FMCE)

When a Metadata Change Event cannot be processed successfully, the event is written to a [dead letter queue](https://en.wikipedia.org/wiki/Dead_letter_queue) in an event called Failed Metadata Change Event (FMCE).

The event simply wraps the original Metadata Change Event and an error message, which contains the reason for rejection.
This event can be used for debugging any potential ingestion issues, as well as for re-playing any previous rejected proposal if necessary.

### Emission

FMCEs are emitted when MCEs cannot be successfully committed to DataHub's storage layer.

### Consumption

No active consumers.

### Schema

The PDL schema can be found [here](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/FailedMetadataChangeEvent.pdl).

The default Kafka topic name for FMCEs is `FailedMetadataChangeEvent_v4`.
