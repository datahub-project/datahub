# Metadata Ingestion Architecture

## MCE Consumer Job

Metadata providers communicate changes in metadata by emitting [MCE]s, which are consumed by a Kafka Streams job, [mce-consumer-job].
The job converts the AVRO-based MCE into the equivalent [Pegasus Data Template] and saves it into the database by calling a special GMS ingest API.

## MAE Consumer Job

All the emitted [MAE] will be consumed by a Kafka Streams job, [mae-consumer-job], which updates the [graph] and [search index] accordingly. 
The job itself is entity-agnostic and will execute corresponding graph & search index builders, which will be invoked by the job when a specific metadata aspect is changed. 
The builder should instruct the job how to update the graph and search index based on the metadata change. 
The builder can optionally use [Remote DAO] to fetch additional metadata from other sources to help compute the final update.

To ensure that metadata changes are processed in the correct chronological order, 
MAEs are keyed by the entity [URN] — meaning all MAEs for a particular entity will be processed sequentially by a single Kafka streams thread. 

## Search and Graph Index Builders

As described in [Metadata Modelling] section, [Entity], [Relationship], and [Search Document] models do not directly encode the logic of how each field should be derived from metadata. 
Instead, this logic should be provided in the form of a graph or search index builder.

The builders register the metadata [aspect]s of their interest against [MAE Consumer Job](#mae-consumer-job) and will be invoked whenever a MAE involving the corresponding aspect is received. 
If the MAE itself doesn’t contain all the metadata needed, builders can use Remote DAO to fetch from GMS directly.

```java
public abstract class BaseIndexBuilder<DOCUMENT extends RecordTemplate> {

 BaseIndexBuilder(@Nonnull List<Class<? extends RecordTemplate>> snapshotsInterested);

 @Nullable
 public abstract List<DOCUMENT> getDocumentsToUpdate(@Nonnull RecordTemplate snapshot);

 @Nonnull
 public abstract Class<DOCUMENT> getDocumentType();
}
```

```java
public interface GraphBuilder<SNAPSHOT extends RecordTemplate> {
 GraphUpdates build(SNAPSHOT snapshot);

 @Value
 class GraphUpdates {
   List<? extends RecordTemplate> entities;
   List<RelationshipUpdates> relationshipUpdates;
 }

 @Value
 class RelationshipUpdates {
   List<? extends RecordTemplate> relationships;
   BaseGraphWriterDAO.RemovalOption preUpdateOperation;
 }
}
```

[MCE]: ../what/mxe.md#metadata-change-event-mce
[MAE]: ../what/mxe.md#metadata-audit-event-mae
[Pegasus Data Template]: https://linkedin.github.io/rest.li/how_data_is_represented_in_memory#the-data-template-layer
[graph]: ../what/graph.md
[search index]: ../what/search-index.md
[mce-consumer-job]: ../../metadata-jobs/mce-consumer-job
[mae-consumer-job]: ../../metadata-jobs/mae-consumer-job
[Remote DAO]: ../architecture/metadata-serving.md#remote-dao
[URN]: ../what/urn.md
[Metadata Modelling]: ../how/metadata-modelling.md
[Entity]: ../what/entity.md
[Relationship]: ../what/relationship.md
[Search Document]: ../what/search-document.md
[Aspect]: ../what/aspect.md
