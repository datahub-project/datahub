# What is Delta in GMA?

Rest.li supports [partial update](https://linkedin.github.io/rest.li/user_guide/restli_server#partial_update) natively without needing explicitly defined models. 
However, the granularity of update is always limited to each field in a PDSC model. 
There are cases where the update need to happen at an even finer grain, e.g. adding or removing items from an array.

To this end, we’re proposing the following entity-specific metadata delta model that allows atomic partial updates at any desired granularity. 
Note that:
1. Just like metadata [aspects](aspect.md), we’re not imposing any limit on the partial update model, as long as it’s a valid PDSC record. 
This is because the rest.li endpoint will have the logic that performs the corresponding partial update based on the information in the model. 
That said, it’s common to have fields that denote the list of items to be added or removed (e.g. `membersToAdd` & `membersToRemove` from below)
2. Similar to metadata [snapshots](snapshot.md), entity that supports metadata delta will add an entity-specific metadata delta 
(e.g. `GroupDelta` from below) that unions all supported partial update models.
3. The entity-specific metadata delta is then added to the global `Delta` typeref, which is added as part of [Metadata Change Event](mxe.md#metadata-change-event-mce) and used during [Metadata Ingestion](../architecture/metadata-ingestion.md).

```json
{
 "type": "record",
 "name": "MembershipPartialUpdate",
 "namespace": "com.linkedin.group",
 "doc": "A metadata delta for a specific group entity.",
 "fields": [
   {
     "name": "membersToAdd",
     "doc": "List of members to be added to the group.",
     "type": {
       "type": "array",
       "items": "com.linkedin.common.CorpuserUrn"
     }
   },
   {
     "name": "membersToRemove",
     "doc": "List of members to be removed from the group.",
     "type": {
       "type": "array",
       "items": "com.linkedin.common.CorpuserUrn"
     }
   }
 ]
}
```

```json
{
 "type": "record",
 "name": "GroupDelta",
 "namespace": "com.linkedin.metadata.delta",
 "doc": "A metadata delta for a specific group entity.",
 "fields": [
   {
     "name": "urn",
     "type": "com.linkedin.common.CorpGroupUrn",
     "doc": "URN for the entity the metadata delta is associated with."
   },
   {
     "name": "delta",
     "doc": "The specific type of metadata delta to apply.",
     "type": [
       "com.linkedin.group.MembershipPartialUpdate"
     ]
   }
 ]
}
```

```json
{
 "type": "typeref",
 "name": "Delta",
 "namespace": "com.linkedin.metadata.delta",
 "doc": "A union of all supported metadata delta types.",
 "ref": [
   "DatasetDelta",
   "GroupDelta"
 ]
}
```