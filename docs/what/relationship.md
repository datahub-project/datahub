# What is a relationship?

A relationship is a named associate between exactly two entities, a source and a destination. 

![metadata-modeling](../imgs/metadata-modeling.png)

From the above graph, a `Group` entity can be linked to a `User` entity via a `HasMember` relationship. 
Note that the name of the relationship reflects the direction, i.e pointing from `Group` to `User`. 
This is due to the fact that the actual metadata aspect holding this information is associated with `Group`, rather than User. 
Had the direction been reversed, the relationship would have been named `IsMemberOf` instead. 
See [Direction of Relationships](#direction-of-relationships) for more discussions on relationship directionality. 
A specific instance of a relationship, e.g. `urn:li:corpgroup:group1` has a member `urn:li:corpuser:user1`, 
corresponds to an edge in the metadata graph.

Similar to an entity, a relationship can also be associated with optional attributes that are derived from metadata. 
For example, from the `Membership` metadata aspect shown below, we’re able to derive the `HasMember` relationship that links a specific `Group` to a specific `User`. 
We can also include additional attribute to the relationship, e.g. importance, which corresponds to the position of the specific member in the original membership array. 
This allows complex graph query that travel only relationships that match certain criteria, e.g. `returns only the top-5 most important members of this group.` 
Once again, attributes should only be added based on query patterns.

```json
{
  "type": "record",
  "name": "Membership",
  "namespace": "com.linkedin.group",
  "doc": "The membership metadata for a group",
  "fields": [
    {
      "name": "auditStamp",
      "type": "com.linkedin.common.AuditStamp",
      "doc": "Audit stamp for the last change"
    },
    {
      "name": "admin",
      "type": "com.linkedin.common.CorpuserUrn",
      "doc": "Admin of the group"
    },
    {
      "name": "members",
      "type": {
        "type": "array",
        "items": "com.linkedin.common.CorpuserUrn"
      },
      "doc": "Members of the group, ordered in descending importance"
    }
  ]
}
```

Relationships are meant to be `entity-neutral`. In other words, one would expect to use the same `OwnedBy` relationship to link a `Dataset` to a `User` and to link a `Dashboard` to a `User`. 
As Pegasus doesn’t allow typing a field using multiple URNs (because they’re all essentially strings), we resort to using generic URN type for the source and destination. 
We also introduce a non-standard property pairings to limit the allowed source and destination URN types.

While it’s possible to model relationships in rest.li as [association resources](https://linkedin.github.io/rest.li/modeling/modeling#association), 
which often get stored as mapping tables, it is far more common to model them as "foreign keys" field in a metadata aspect. 
For instance, the `Ownership` aspect is likely to contain an array of owner’s corpuser URNs.

Below is an example of how a relationship is modeled in PDSC. Note that:
1. As the `source` and `destination` are of generic URN type, we’re able to factor them out to a common `BaseRelationship` model.
2. Each model is expected to have a pairings property that is an array of all allowed source-destination URNs.
3. Unlike entities, there’s no requirement on making all attributes optional since relationships do not support partial updates.

```json
{
  "type": "record",
  "name": "BaseRelationship",
  "namespace": "com.linkedin.metadata.relationship",
  "doc": "Common fields that apply to all relationships",
  "fields": [
    {
      "name": "source",
      "type": "com.linkedin.common.Urn",
      "doc": "Urn for the source of the relationship"
    },
    {
      "name": "destination",
      "type": "com.linkedin.common.Urn",
      "doc": "Urn for the destination of the relationship"
    }
  ]
}
```

```json
{
  "type": "record",
  "name": "HasMember",
  "namespace": "com.linkedin.metadata.relationship",
  "doc": "Data model for a has-member relationship",
  "include": [
    "BaseRelationship"
  ],
  "pairings": [
    {
      "source": "com.linkedin.common.urn.CorpGroupUrn",
      "destination": "com.linkedin.common.urn.CorpUserUrn"
    }
  ],
  "fields": [
    {
      "name": "importance",
      "type": "int",
      "doc": "The importance of the membership"
    }
  ]
}
```

## Direction of Relationships

As relationships are modeled as directed edges between nodes, it’s natural to ask which way should it be pointing, 
or should there be edges going both ways? The answer is, "it kind of doesn’t matter." It’s rather an aesthetic choice than technical one. 

For one, the actual direction doesn’t really matter when it comes to constructing graph queries. Most graph DBs are fully capable of traversing edges in reverse direction efficiently.

That being said, generally there’s a more "natural way" to specify the direction of a relationship, which is closely related to how metadata is stored. For example, the membership information for an LDAP group is generally stored as a list in group’s metadata. As a result, it’s more natural to model a `HasAMember` relationship that points from a group to a member, instead of a `IsMemberOf` relationship pointing from member to group.

Since all relationships are explicitly declared, it’s fairly easy for a user to discover what relationships are available and their directionality by inspecting 
the [relationships directory](../../metadata-models/src/main/pegasus/com/linkedin/metadata/relationship). It’s also possible to provide a UI for the catalog of entities and relationships for analysts who are interested in building complex graph queries to gain insights into metadata.
