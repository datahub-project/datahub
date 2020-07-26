# Onboarding to GMA Graph - Adding a new relationship type

Steps for this already detailed in [How to onboard to GMA graph?](../how/graph-onboarding.md)

For this exercise, we'll add a new relationship type `FollowedBy` which is extracted out of `Follow` aspect. For that, we first need to add `Follow` aspect.

## 1. Onboard `Follow` aspect
Referring to [How to add a new metadata aspect?](../how/add-new-aspect.md)

### 1.1 Model new aspect
* Follow.pdl
```
namespace com.linkedin.common

/**
 * Follow information of an entity.
 */
record Follow {

  /**
   * List of followers of an entity.
   */
  followers: array[FollowAction]
}
```

* FollowAction.pdl
```
namespace com.linkedin.common

/**
 * Follow Action of an entity.
 */
record FollowAction {

  /**
   * Follower (User or a Group) of an entity
   */
  follower: FollowerType

  /**
   * Audit stamp containing who last modified the record and when.
   */
  lastModified: optional AuditStamp
}
```

* FollowerType.pdl
```
namespace com.linkedin.common

/**
 * A union of all supported follower types
 */
typeref FollowerType = union[
  corpUser: CorpuserUrn,
  corpGroup: CorpGroupUrn
]
```

### 1.2 Update aspect union for dataset
```
namespace com.linkedin.metadata.aspect

import com.linkedin.common.Follow
import com.linkedin.common.InstitutionalMemory
import com.linkedin.common.Ownership
import com.linkedin.common.Status
import com.linkedin.dataset.DatasetDeprecation
import com.linkedin.dataset.DatasetProperties
import com.linkedin.dataset.UpstreamLineage
import com.linkedin.schema.SchemaMetadata

/**
 * A union of all supported metadata aspects for a Dataset
 */
typeref DatasetAspect = union[
  DatasetProperties,
  DatasetDeprecation,
  Follow,
  UpstreamLineage,
  InstitutionalMemory,
  Ownership,
  Status,
  SchemaMetadata
]
```

## 2. Create `FollowedBy` relationship
```
namespace com.linkedin.metadata.relationship

/**
 * A generic model for the Followed-By relationship
 */
@pairings = [ {
  "destination" : "com.linkedin.common.urn.CorpuserUrn",
  "source" : "com.linkedin.common.urn.DatasetUrn"
}, {
  "destination" : "com.linkedin.common.urn.CorpGroupUrn",
  "source" : "com.linkedin.common.urn.DatasetUrn"
} ]
record FollowedBy includes BaseRelationship {
}
```

## 3. Build the repo to generate Java classes for newly added models
```
./gradlew build -Prest.model.compatibility=ignore
```

You can verify that API definitions for /dataset endpoint of GMS as well as MXE schemas are automatically updated to include new model changes.

## 4. Create `FollowedBy` relationship builder from `Follow` aspect
```java
package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.Follow;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.FollowedBy;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;


public class FollowedByBuilderFromFollow extends BaseRelationshipBuilder<Follow> {

  public FollowedByBuilderFromFollow() {
    super(Follow.class);
  }

  @Nonnull
  @Override
  public <URN extends Urn> List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull URN urn,
      @Nonnull Follow follow) {
    final List<FollowedBy> followedByList = follow.getFollowers().stream().map(followAction -> {
      if (followAction.getFollower().isCorpUser()) {
        return new FollowedBy().setSource(urn).setDestination(followAction.getFollower().getCorpUser());
      }
      if (followAction.getFollower().isCorpGroup()) {
        return new FollowedBy().setSource(urn).setDestination(followAction.getFollower().getCorpGroup());
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());

    return Collections.singletonList(new GraphBuilder.RelationshipUpdates(followedByList, REMOVE_ALL_EDGES_FROM_SOURCE));
  }
}
```

## 5. Update set of relationship builders for dataset by adding `FollowedByBuilderFromFollow`

```java
package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.graph.relationship.BaseRelationshipBuilder;
import com.linkedin.metadata.builders.graph.relationship.DownstreamOfBuilderFromUpstreamLineage;
import com.linkedin.metadata.builders.graph.relationship.FollowedByBuilderFromFollow;
import com.linkedin.metadata.builders.graph.relationship.OwnedByBuilderFromOwnership;
import com.linkedin.metadata.entity.DatasetEntity;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

public class DatasetGraphBuilder extends BaseGraphBuilder<DatasetSnapshot> {
  private static final Set<BaseRelationshipBuilder> RELATIONSHIP_BUILDERS =
      Collections.unmodifiableSet(new HashSet<BaseRelationshipBuilder>() {
        {
          add(new DownstreamOfBuilderFromUpstreamLineage());
          add(new FollowedByBuilderFromFollow());
          add(new OwnedByBuilderFromOwnership());
        }
      });

  public DatasetGraphBuilder() {
    super(DatasetSnapshot.class, RELATIONSHIP_BUILDERS);
  }

  @Nonnull
  @Override
  protected List<? extends RecordTemplate> buildEntities(@Nonnull DatasetSnapshot snapshot) {
    final DatasetUrn urn = snapshot.getUrn();
    final DatasetEntity entity = new DatasetEntity().setUrn(urn)
        .setName(urn.getDatasetNameEntity())
        .setPlatform(urn.getPlatformEntity())
        .setOrigin(urn.getOriginEntity());

    setRemovedProperty(snapshot, entity);

    return Collections.singletonList(entity);
  }
}
```

## 6. Rebuild & restart all containers with new changes

This is all the code change we need to do to enable linking datasets and corp users (groups as well).
Now we can re-build & start all Docker images.

```
./docker/rebuild-all/rebuild-all.sh
```

## 7. That's it. Let's test our new feature!

Let's ingest a user first
```
curl 'http://localhost:8080/corpUsers?action=ingest' -X POST -H 'X-RestLi-Protocol-Version:2.0.0' --data '
{
  "snapshot": {
    "aspects": [{
      "com.linkedin.identity.CorpUserInfo": {
        "active": true,
        "displayName": "Foo Bar",
        "fullName": "Foo Bar",
        "email": "fbar@linkedin.com"
      }
    }],
    "urn": "urn:li:corpuser:fbar"
  }
}'
```

And now let's ingest a dataset with two aspects: Ownership & Follow
```
curl 'http://localhost:8080/datasets?action=ingest' -X POST -H 'X-RestLi-Protocol-Version:2.0.0' --data '
{
  "snapshot": {
    "aspects": [{
      "com.linkedin.common.Ownership": {
        "owners": [{
            "owner": "urn:li:corpuser:fbar",
            "type": "DATAOWNER"
        }],
        "lastModified": {
            "time": 0,
            "actor": "urn:li:corpuser:fbar"
        }
      }
    },
    {
      "com.linkedin.common.Follow": {
        "followers": [{
          "follower": {
            "corpUser": "urn:li:corpuser:fbar"
          }
        }]
      }
    }],
    "urn": "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)"
  }
}'
```

Ownership aspect will help create an `OwnedBy` edge between user & dataset nodes. That existed already.
Now that we added follow aspect, we'll also be able to see a `FollowedBy` edge between same user & dataset nodes.

You can confirm this by connecting to Neo4j browser on http://localhost:7474/browser