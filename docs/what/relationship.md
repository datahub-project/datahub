# What is a relationship?

A relationship is a named associate between exactly two [entities](entity.md), a source and a destination. 

![metadata-modeling](../imgs/metadata-modeling.png)

From the above graph, a `Group` entity can be linked to a `User` entity via a `HasMember` relationship. 
Note that the name of the relationship reflects the direction, i.e. pointing from `Group` to `User`. 
This is due to the fact that the actual metadata aspect holding this information is associated with `Group`, rather than User. 
Had the direction been reversed, the relationship would have been named `IsMemberOf` instead. 
See [Direction of Relationships](#direction-of-relationships) for more discussions on relationship directionality. 
A specific instance of a relationship, e.g. `urn:li:corpGroup:group1` has a member `urn:li:corpuser:user1`, 
corresponds to an edge in the metadata graph.

Similar to an entity, a relationship can also be associated with optional attributes that are derived from the metadata. 
For example, from the `Membership` metadata aspect shown below, we’re able to derive the `HasMember` relationship that links a specific `Group` to a specific `User`. We can also include additional attribute to the relationship, e.g. importance, which corresponds to the position of the specific member in the original membership array. This allows complex graph query that travel only relationships that match certain criteria, e.g. "returns only the top-5 most important members of this group." 
Similar to the entity attributes, relationship attributes should only be added based on the expected query patterns to reduce the indexing cost.

```
namespace: com.linkedin.group

import com.linkedin.common.AuditStamp
import com.linkedin.common.CorpuserUrn

/**
 * The membership metadata for a group
 */
record Membership {

  /** Audit stamp for the last change */
  modified: AuditStamp

  /** Admin of the group */
  admin: CorpuserUrn

  /** Members of the group, ordered in descending importance */
  members: array[CorpuserUrn]
}
```

Relationships are meant to be "entity-neutral". In other words, one would expect to use the same `OwnedBy` relationship to link a `Dataset` to a `User` and to link a `Dashboard` to a `User`. As Pegasus doesn’t allow typing a field using multiple URNs (because they’re all essentially strings), we resort to using generic URN type for the source and destination. 
We also introduce a `@pairings` [annotation](https://linkedin.github.io/rest.li/pdl_migration#shorthand-for-custom-properties) to limit the allowed source and destination URN types.

While it’s possible to model relationships in rest.li as [association resources](https://linkedin.github.io/rest.li/modeling/modeling#association), which often get stored as mapping tables, it is far more common to model them as "foreign keys" field in a metadata aspect. For instance, the `Ownership` aspect is likely to contain an array of owner’s corpuser URNs.

Below is an example of how a relationship is modeled in PDL. Note that:
1. As the `source` and `destination` are of generic URN type, we’re able to factor them out to a common `BaseRelationship` model.
2. Each model is expected to have a `@pairings` annotation that is an array of all allowed source-destination URN pairs.
3. Unlike entity attributes, there’s no requirement on making all relationship attributes optional since relationships do not support partial updates.

```
namespace com.linkedin.metadata.relationship

import com.linkedin.common.Urn

/**
 * Common fields that apply to all relationships
 */
record BaseRelationship {

  /**
   * Urn for the source of the relationship
   */
  source: Urn

  /**
   * Urn for the destination of the relationship
   */
  destination: Urn
}
```

```
namespace com.linkedin.metadata.relationship

/**
 * Data model for a has-member relationship
 */
@pairings = [ {
  "destination" : "com.linkedin.common.urn.CorpGroupUrn",
  "source" : "com.linkedin.common.urn.CorpUserUrn"
} ]
record HasMembership includes BaseRelationship
{
  /**
   * The importance of the membership
   */
  importance: int 
}
```

## Direction of Relationships

As relationships are modeled as directed edges between nodes, it’s natural to ask which way should it be pointing, 
or should there be edges going both ways? The answer is, "doesn’t really matter." It’s rather an aesthetic choice than technical one. 

For one, the actual direction doesn’t really impact the execution of graph queries. Most graph DBs are fully capable of traversing edges in reverse direction efficiently.

That being said, generally there’s a more "natural way" to specify the direction of a relationship, which closely relate to how the metadata is stored. For example, the membership information for an LDAP group is generally stored as a list in group’s metadata. As a result, it’s more natural to model a `HasMember` relationship that points from a group to a member, instead of a `IsMemberOf` relationship pointing from member to group.

Since all relationships are explicitly declared, it’s fairly easy for a user to discover what relationships are available and their directionality by inspecting 
the [relationships directory](../../metadata-models/src/main/pegasus/com/linkedin/metadata/relationship). It’s also possible to provide a UI for the catalog of entities and relationships for analysts who are interested in building complex graph queries to gain insights into the metadata.

## High Cardinality Relationships

See [this doc](../advanced/high-cardinality.md) for suggestions on how to best model relationships with high cardinality.
