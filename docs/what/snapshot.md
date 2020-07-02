# What is a snapshot?

A metadata snapshot models the current state of one or multiple metadata [aspects](aspect.md) associated with a particular [entity](entity.md). 
Each entity type is expected to have:
1. An entity-specific aspect (e.g. `CorpGroupAspect` from below), which is a `typeref` containing a union of all possible metadata aspects for the entity.
2. An entity-specific snapshot (e.g. `CorpGroupSnapshot` from below), which contains an array (aspects) of entity-specific aspects.

```
namespace com.linkedin.metadata.aspect

import com.linkedin.group.Membership
import com.linkedin.group.SomeOtherMetadata

/**
 * A union of all supported metadata aspects for a group
 */
typeref CorpGroupAspect = union[Membership, SomeOtherMetadata]
```

```
namespace com.linkedin.metadata.snapshot

import com.linkedin.common.CorpGroupUrn
import com.linkedin.metadata.aspect.CorpGroupAspect

/**
 * A metadata snapshot for a specific Group entity.
 */
record CorpGroupSnapshot {

  /** URN for the entity the metadata snapshot is associated with */
  urn: CorpGroupUrn

  /** The list of metadata aspects associated with the group */
  aspects: array[CorpGroupAspect]
}
```

The generic `Snapshot` typeref contains a union of all entity-specific snapshots and can therefore be used to represent the state of any metadata aspect for all supported entity types.

```
namespace com.linkedin.metadata.snapshot

/**
 * A union of all supported metadata snapshot types.
 */
typeref Snapshot = union[DatasetSnapshot, CorpGroupSnapshot, CorpUserSnapshot]
```
