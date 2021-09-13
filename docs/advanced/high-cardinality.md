# High Cardinality Relationships

As explained in [What is a Relationship](../what/relationship.md), the raw metadata for forming relationships is captured directly inside of a [Metadata Aspect](../what/aspect.md). The most natural way to model this is using an array, e.g. a group membership aspect contains an array of user [URNs](../what/urn.md). However, this poses some challenges when the cardinality of the relationship is expected to be large (say, greater than 10,000). The aspect becomes large in size, which leads to slow update and retrieval. It may even exceed the underlying limit of the document store, which is often in the range of a few MBs. Furthermore, sending large messages (> 1MB) over Kafka requires special tuning and is generally discouraged.

Depending on the type of relationships, there are different strategies for dealing with high cardinality. 

### 1:N Relationships

When `N` is large, simply store the relationship as a reverse pointer on the `N` side, instead of an `N`-element array on the `1` side. In other words, instead of doing this

```
record MemberList {
  members: array[UserUrn]
}
```

do this

```
record Membership {
  group: GroupUrn
}
```

One drawback with this approach is that batch updating the member list becomes multiple DB operations and non-atomic. If the list is provided by an external metadata provider via [MCEs](../what/mxe.md), this also means that multiple MCEs will be required to update the list, instead of having one giant array in a single MCE.

### M:N Relationships

When one side of the relation (`M` or `N`) has low cardinality, you can apply the same trick in [1:N Relationship] by creating the array on the side with low-cardinality. For example, assuming a user can only be part of a small number of groups but each group can have a large number of users, the following model will be more efficient than the reverse.

```
record Membership {
  groups: array[GroupUrn]
}
```

When both `M` and `N` are of high cardinality (e.g. millions of users, each belongs to million of groups), the only way to store such relationships efficiently is by creating a new "Mapping Entity" with a single aspect like this

```
record UserGroupMap {
  user: UserUrn
  group: GroupUrn
}
```

This means that the relationship now can only be created & updated at a single source-destination pair granularity.  
