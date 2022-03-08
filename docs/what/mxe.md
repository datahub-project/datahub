# What is MXE (Metadata Events)?

The models defined in [snapshot](snapshot.md) and [delta](delta.md) are used to build the schema for several metadata Kafka events. As these events have the prefix `Metadata` and suffix `Event`, they’re collectively referred to as MXE.

We also model MXEs using [PDL](https://linkedin.github.io/rest.li/pdl_schema) and rely on the [pegasus gradle plugin](https://linkedin.github.io/rest.li/setup/gradle#generateavroschema) to convert them into [AVSC](https://avro.apache.org/docs/current/spec.html). However, we also need to rename all the namespaces of the generated AVSC to avoid namespace clashes for projects that depend on both the PDL models and MXEs. 

As the AVSC and PDL model schemas are 100% compatible, it’d be very easy to convert the in-memory representation from one to another using [Pegasus’ DataTranslator](https://linkedin.github.io/rest.li/avro_translation).

## Metadata Change Event (MCE)

MCE is a "proposal" for a metadata change, as opposed to [MAE](#metadata-audit-event), which is conveying a committed change. 
Consequently, only successfully accepted and processed MCEs will lead to the emission of a corresponding MAE. 
A single MCE can contain both snapshot-oriented and delta-oriented metadata change proposal. The use case of this event is explained in [Metadata Ingestion](../architecture/metadata-ingestion.md).

```
namespace com.linkedin.mxe

import com.linkedin.avro2pegasus.events.KafkaAuditHeader
import com.linkedin.metadata.delta.Delta
import com.linkedin.metadata.snapshot.Snapshot

/**
 * Kafka event for proposing a metadata change for an entity
 */
record MetadataChangeEvent {

  /** Kafka audit header */
  auditHeader: optional KafkaAuditHeader

  /** Snapshot of the proposed metadata change. Include only the aspects affected by the change in the snapshot. */
  proposedSnapshot: Snapshot

  /** Delta of the proposed metadata partial update */
  proposedDelta: optional Delta
}
```

We’ll also generate a [dead letter queue](https://en.wikipedia.org/wiki/Dead_letter_queue) event, Failed Metadata Change Event (FMCE), for any rejected MCE. The event simply wraps the original MCE and an error message, which contains the reason for rejection. This event can be used for debugging any potential ingestion issues, as well as for re-playing any previous rejected proposal if ever needed.

```
namespace com.linkedin.mxe

import com.linkedin.avro2pegasus.events.KafkaAuditHeader

/**
 * Kafka event for capturing a failure to process a specific MCE
 */
record FailedMetadataChangeEvent {

  /** Kafka audit header */
  auditHeader: optional KafkaAuditHeader

  /** The event that failed to be processed */
  metadataChangeEvent: MetadataChangeEvent

  /** The error message or the stacktrace for the failure */
  error: string
}
```

## Metadata Audit Event (MAE)

A Metadata Audit Event captures the change made to one or multiple metadata [aspects](aspect.md) associated with a particular [entity](entity.md), in the form of a metadata [snapshot](snapshot.md) before the change, and a metadata snapshot after the change.

Every source-of-truth for a particular metadata aspect is expected to emit a MAE whenever a change is committed to that aspect. By ensuring that, any listener of MAE will be able to construct a complete view of the latest state for all aspects. 
Furthermore, because each MAE contains the "after image", any mistake made in emitting the MAE can be easily mitigated by emitting a follow-up MAE with the correction. By the same token, the initial bootstrap problem for any newly added entity can also be solved by emitting a MAE containing all the latest metadata aspects associated with that entity.

```
namespace com.linkedin.mxe

import com.linkedin.avro2pegasus.events.KafkaAuditHeader
import com.linkedin.metadata.snapshot.Snapshot

/**
 * Kafka event for capturing update made to an entity's metadata.
 */
record MetadataAuditEvent {

  /** Kafka audit header */
  auditHeader: optional KafkaAuditHeader

  /**
   * Snapshot of the metadata before the update. Set to null for newly created metadata. 
   * Only the metadata aspects affected by the update are included in the snapshot.
   */
  oldSnapshot: optional Snapshot

  /**
   * Snapshot of the metadata after the update. Only the metadata aspects affected by the 
   * update are included in the snapshot.
   */
  newSnapshot: Snapshot
}
```
