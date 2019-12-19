# What is a snapshot in GMA?

A metadata snapshot models the current state of one or multiple metadata [aspects](aspect.md) associated with a particular [entity](entity.md). 
Each entity type is expected to have:
1. An entity-specific aspect (e.g. `GroupAspect` from below), which is a `typeref` containing a union of all possible metadata aspects for the entity.
2. An entity-specific snapshot (e.g. `GroupSnapshot` from below), which contains an array (aspects) of entity-specific aspects.

```json
{
 "type": "typeref",
 "name": "GroupAspect",
 "namespace": "com.linkedin.metadata.aspect",
 "doc": "A specific metadata aspect for a group",
 "ref": [
   "com.linkedin.group.Membership",
   "com.linkedin.group.SomeOtherMetadata"
 ]
}
```

```json
{
 "type": "record",
 "name": "GroupSnapshot",
 "namespace": "com.linkedin.metadata.snapshot",
 "doc": "A metadata snapshot for a specific group entity.",
 "fields": [
   {
     "name": "urn",
     "type": "com.linkedin.common.CorpGroupUrn",
     "doc": "URN for the entity the metadata snapshot is associated with."
   },
   {
     "name": "aspects",
     "doc": "The list of metadata aspects associated with the group.",
     "type": {
       "type": "array",
       "items": "com.linkedin.metadata.aspect.GroupAspect"
     }
   }
 ]
}
```

The generic `Snapshot` typeref contains a union of all entity-specific snapshots and can therefore be used to represent the state of any metadata aspect for all supported entity types.

```json
{
 "type": "typeref",
 "name": "Snapshot",
 "namespace": "com.linkedin.metadata.snapshot",
 "doc": "A union of all supported metadata snapshot types.",
 "ref": [
   "DatasetSnapshot",
   "GroupSnapshot",
   "UserSnapshot"
 ]
}
```
