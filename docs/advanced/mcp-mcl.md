# MetadataChangeProposal & MetadataChangeLog Events

## Overview & Vision

As of release v0.8.7, two new important event streams have been introduced: MetadataChangeProposal & MetadataChangeLog. These topics serve as a more generic (and more appropriately named) versions of the classic MetadataChangeEvent and MetadataAuditEvent events, used for a) proposing and b) logging changes to the DataHub Metadata Graph.

With these events, we move towards a more generic world, in which Metadata models are not strongly-typed parts of the event schemas themselves. This provides flexibility, allowing for the core models comprising the Metadata Graph to be added and changed dynamically, without requiring structural updates to Kafka or REST API schemas used for ingesting and serving Metadata.

Moreover, we've focused in on the "aspect" as the atomic unit of write in DataHub. MetadataChangeProposal & MetadataChangeLog with carry only a single aspect in their payload, as opposed to the list of aspects carried by today's MCE & MAE. This more accurately reflects the atomicity contract of the metadata model, hopefully lessening confusion about transactional guarantees for multi-aspect writes in addition to making it simpler to tune into the metadata changes a consumer cares about.

Making these events more generic does not come for free; we give up some in the form of Restli and Kafka-native schema validation and defer this responsibility to DataHub itself, who is the sole enforcer of the graph model contracts. Additionally, we add an extra step to unbundling the actual metadata by requiring a double-deserialization: that of the event / response body itself and another of the nested Metadata aspect.

To mitigate these downsides, we are committed to providing cross-language client libraries capable of doing the hard work for you. We intend to publish these as strongly-typed artifacts generated from the "default" model set DataHub ships with. This stands in addition to an initiative to introduce an OpenAPI layer in DataHub's backend (gms) which would provide a strongly typed model.

Ultimately, we intend to realize a state in which the Entities and Aspect schemas can be altered without requiring generated code and without maintaining a single mega-model schema (looking at you, Snapshot.pdl). The intention is that changes to the metadata model become even easier than they are today.

### Synchronous Ingestion Architecture

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/advanced/mcp-mcl/sync-ingestion.svg"/>
</p>

### Asynchronous Ingestion Architecture

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/advanced/mcp-mcl/async-ingestion.svg"/>
</p>

## Modeling

A Metadata Change Proposal is defined (in PDL) as follows

```protobuf
record MetadataChangeProposal {

  /**
   * Kafka audit header. See go/kafkaauditheader for more info.
   */
  auditHeader: optional KafkaAuditHeader

  /**
   * Type of the entity being written to
   */
  entityType: string

  /**
   * Urn of the entity being written
   **/
  entityUrn: optional Urn,

  /**
   * Key aspect of the entity being written
   */
  entityKeyAspect: optional GenericAspect
	
  /**
   * Type of change being proposed
   */
  changeType: ChangeType

  /**
   * Aspect of the entity being written to
   * Not filling this out implies that the writer wants to affect the entire entity
   * Note: This is only valid for CREATE and DELETE operations.
   **/
  aspectName: optional string

  aspect: optional GenericAspect

  /**
   * A string->string map of custom properties that one might want to attach to an event
   **/
  systemMetadata: optional SystemMetadata

  /**
  * Headers - intended to mimic http headers
  */
  headers: optional map[string, string]
}
```

Each proposal is comprised of the following:

1. entityType

   Refers to the type of the entity e.g. dataset, chart

2. entityUrn

   Urn of the entity being updated. Note, **exactly one** of entityUrn or entityKeyAspect must be filled out to correctly identify an entity.

3. entityKeyAspect

   Key aspect of the entity. Instead of having a string URN, we will support identifying entities by their key aspect structs. Note, this is not supported as of now.

4. changeType

   Type of change you are proposing: one of

    - UPSERT: Insert if not exists, update otherwise
    - CREATE: Insert aspect if not exists, fail otherwise
    - CREATE_ENTITY: Insert if entity does not exist, fail otherwise
    - UPDATE: Update if exists, fail otherwise
    - DELETE: Delete
    - PATCH: Patch the aspect instead of doing a full replace

   Only UPSERT, CREATE, CREATE_ENTITY, DELETE, PATCH are supported as of now.

5. aspectName

   Name of the aspect. Must match the name in the "@Aspect" annotation.

6. aspect

   To support strongly typed aspects, without having to keep track of a union of all existing aspects, we introduced a new object called GenericAspect.

    ```xml
    record GenericAspect {
        value: bytes
        contentType: string
    }
    ```

   It contains the type of serialization and the serialized value. Note, currently we only support "application/json" as contentType but will be adding more forms of serialization in the future. Validation of the serialized object happens in GMS against the schema matching the aspectName.

7. systemMetadata

   Extra metadata about the proposal like run_id or updated timestamp.

8. headers

   Optional headers which are meant to mimic http headers. These are currently used for implementing conditional write logic.

GMS processes the proposal and produces the Metadata Change Log, which looks like this.

```protobuf
record MetadataChangeLog includes MetadataChangeProposal {

  previousAspectValue: optional GenericAspect

  previousSystemMetadata: optional SystemMetadata

}
```

It includes all fields in the proposal, but also has the previous version of the aspect value and system metadata. This allows the MCL processor to know the previous value before deciding to update all indices.

## Topics

Following the change in our event models, we introduced 4 new topics. The old topics will get deprecated as we fully migrate to this model.

1. **MetadataChangeProposal_v1, FailedMetadataChangeProposal_v1**

   Analogous to the MCE topic, proposals that get produced into the MetadataChangeProposal_v1 topic, will get ingested to GMS asynchronously, and any failed ingestion will produce a failed MCP in the FailedMetadataChangeProposal_v1 topic.


2. **MetadataChangeLog_Versioned_v1**

   Analogous to the MAE topic, MCLs for versioned aspects will get produced into this topic. Since versioned aspects have a source of truth that can be separately backed up, the retention of this topic is short (by default 7 days). Note both this and the next topic are consumed by the same MCL processor.


3. **MetadataChangeLog_Timeseries_v1**

   Analogous to the MAE topics, MCLs for timeseries aspects will get produced into this topic. Since timeseries aspects do not have a source of truth, but rather gets ingested straight to elasticsearch, we set the retention of this topic to be longer (90 days). You can backup timeseries aspect by replaying this topic.

## Configuration

With MetadataChangeProposal and MetadataChangeLog, we will introduce a new mechanism for configuring the association between Metadata Entities & Aspects. Specifically, the Snapshot.pdl model will no longer encode this information by way of [Rest.li](http://rest.li) union. Instead, a more explicit yaml file will provide these links. This file will be leveraged at runtime to construct the in-memory Entity Registry which contains the global Metadata schema along with some additional metadata.

An example of the configuration file that will be used for MCP & MCL, which defines a "dataset" entity that is associated with to two aspects: "datasetKey" and "datasetProfile".

```
# entity-registry.yml

entities:
  - name: dataset
    keyAspect: datasetKey
    aspects:
      - datasetProfile
```

## Features

### Conditional Writes

Conditional write semantics use extra information contained in the MCP `headers` field to possibly avoid writing new aspects
if the conditions are not met.

#### If-Version-Match

Each time an aspect is updated a `version` is incremented to represent the change to the aspect. This `version` is stored and returned
in `SystemMetadata`.

A writer can provide a header with the expected `version` when initiating the request. If the expected `version` does not
match the actual `version` stored in the database, the write will fail. This prevents overwriting an aspect that has
been modified by another process.

Note: If the aspect doesn't exist yet, then the `version` is `-1`. A writer can use this `version` to only create
an aspect if it doesn't. Also see _Change Types: [`CREATE`, `CREATE_ENTITY`]_ section below.

#### If-Modified-Since / If-Unmodified-Since

A writer may also specify time-based conditions using http header semantics. Similar to version based conditional writes
this method can be used to prevent the write if the target aspect was modified after a reading the aspect. Per the
http specification dates must comply with ISO-8601 standard.

`If-Unmodified-Since`:
A writer can specify that the aspect must NOT have been modified after a specific time, following [If-Unmodified-Since](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-Unmodified-Since) http headers.

`If-Modified-Since`
A writer can specify that the aspect must have been modified after a specific time, following [If-Modified-Since](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-Modified-Since) http headers.


#### Change Types: [`CREATE`, `CREATE_ENTITY`]

Another form of conditional writes which considers the existence of an aspect or entity uses the following Change Types.

`CREATE` - Create the aspect if it doesn't already exist.

`CREATE_ENTITY` - Create the aspect if no aspects exist for the entity.

By default, a validation exception is thrown if the `CREATE`/`CREATE_ENTITY` constraint is violated. If the write operation
should be dropped without considering it an exception, then add the following header: `If-None-Match: *` to the MCP.

### Synchronous ElasticSearch Updates

The writes to the elasticsearch are asynchronous by default. A writer can add a custom header
`X-DataHub-Sync-Index-Update` to the MCP `headers` with value set to `true` to enable a synchronous update of
elasticsearch for specific MCPs that may benefit from it.
