# What is a relationship?

A relationship is a named associate between exactly two [entities](entity.md), a source and a destination.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-modeling.png"/>
</p>


From the above graph, a `Group` entity can be linked to a `User` entity via a `HasMember` relationship.
Note that the name of the relationship reflects the direction, i.e. pointing from `Group` to `User`.
This is due to the fact that the actual metadata aspect holding this information is associated with `Group`, rather than
User.
Had the direction been reversed, the relationship would have been named `IsMemberOf` instead.
See [Direction of Relationships](#direction-of-relationships) for more discussions on relationship directionality.
A specific instance of a relationship, e.g. `urn:li:corpGroup:group1` has a member `urn:li:corpuser:user1`,
corresponds to an edge in the metadata graph.

Relationships are meant to be "entity-neutral". In other words, one would expect to use the same `OwnedBy` relationship
to link a `Dataset` to a `User` and to link a `Dashboard` to a `User`.
As Pegasus doesn’t allow typing a field using multiple URNs (because they’re all essentially strings), we resort to
using generic URN type for the source and destination.
We also introduce a `@Relationship` [annotation](../modeling/extending-the-metadata-model.md/#relationship) to
limit the allowed source and destination URN types.

While it’s possible to model relationships in rest.li
as [association resources](https://linkedin.github.io/rest.li/modeling/modeling#association), which often get stored as
mapping tables, it is far more common to model them as "foreign keys" field in a metadata aspect. For instance,
the `Ownership` aspect is likely to contain an array of owner’s corpUser URNs.

Below is an example of how a relationship is modeled in PDL. Note that:

1. This aspect, `nativeGroupMembership` would be associated with a `corpUser`
2. The `corpUser`'s aspect points to one or more parent entities of type `corpGroup`

```
namespace com.linkedin.identity

import com.linkedin.common.Urn

/**
 * Carries information about the native CorpGroups a user is in.
 */
@Aspect = {
  "name": "nativeGroupMembership"
}
record NativeGroupMembership {
  @Relationship = {
    "/*": {
      "name": "IsMemberOfNativeGroup",
      "entityTypes": [ "corpGroup" ]
    }
  }
  nativeGroups: array[Urn]
}
```

## Direction of Relationships

As relationships are modeled as directed edges between nodes, it’s natural to ask which way should it be pointing,
or should there be edges going both ways? The answer is, "doesn’t really matter." It’s rather an aesthetic choice than
technical one.

For one, the actual direction doesn’t really impact the execution of graph queries. Most graph DBs are fully capable of
traversing edges in reverse direction efficiently.

That being said, generally there’s a more "natural way" to specify the direction of a relationship, which closely relate
to how the metadata is stored. For example, the membership information for an LDAP group is generally stored as a list
in group’s metadata. As a result, it’s more natural to model a `HasMember` relationship that points from a group to a
member, instead of a `IsMemberOf` relationship pointing from member to group.

## High Cardinality Relationships

See [this doc](../advanced/high-cardinality.md) for suggestions on how to best model relationships with high
cardinality.
