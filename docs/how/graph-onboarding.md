# How to onboard to GMA graph?

## 1. Define relationship models
If you need to define a [relationship] which is not available in the set of [relationship models] provided,
that relationship model should be implemented as a first step for graph onboarding. 
Below is an example model for `OwnedBy` relationship:

```
namespace com.linkedin.metadata.relationship

import com.linkedin.common.OwnershipType

/**
 * A generic model for the Owned-By relationship
 */
@pairings = [ {
  "destination" : "com.linkedin.common.urn.CorpuserUrn",
  "source" : "com.linkedin.common.urn.DatasetUrn"
} ]
record OwnedBy includes BaseRelationship {

  /** The type of the ownership */
  type: OwnershipType
}
```

## 2. Implement relationship builders
You need to implement relationship builders for your specific [aspect]s and [relationship]s if they are not already defined.
Relationship builders build list of relationships after processing aspects and any relationship builder should implement `BaseRelationshipBuilder` abstract class.
Relationship builders are per aspect and per relationship type.

```java
public abstract class BaseRelationshipBuilder<ASPECT extends RecordTemplate> {

  private Class<ASPECT> _aspectClass;

  public BaseRelationshipBuilder(Class<ASPECT> aspectClass) {
    _aspectClass = aspectClass;
  }

  /**
   * Returns the aspect class this {@link BaseRelationshipBuilder} supports
   */
  public Class<ASPECT> supportedAspectClass() {
    return _aspectClass;
  }

  /**
   * Returns a list of corresponding relationship updates for the given metadata aspect
   */
  public abstract <URN extends Urn> List<GraphBuilder.RelationshipUpdates> buildRelationships(URN urn, ASPECT aspect);
}
```

## 3. Implement graph builders
Graph builders build graph updates by processing [snapshot]s. 
They internally use relationship builders to generate edges and nodes of the graph.
All relationship builders for an [entity] should be registered through graph builder.

```java
public abstract class BaseGraphBuilder<SNAPSHOT extends RecordTemplate> implements GraphBuilder<SNAPSHOT> {

  private final Class<SNAPSHOT> _snapshotClass;
  private final Map<Class<? extends RecordTemplate>, BaseRelationshipBuilder> _relationshipBuildersMap;

  public BaseGraphBuilder(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Collection<BaseRelationshipBuilder> relationshipBuilders) {
    _snapshotClass = snapshotClass;
    _relationshipBuildersMap = relationshipBuilders.stream()
        .collect(Collectors.toMap(builder -> builder.supportedAspectClass(), Function.identity()));
  }

  @Nonnull
  Class<SNAPSHOT> supportedSnapshotClass() {
    return _snapshotClass;
  }

  @Nonnull
  @Override
  public GraphUpdates build(@Nonnull SNAPSHOT snapshot) {
    final Urn urn = RecordUtils.getRecordTemplateField(snapshot, "urn", Urn.class);

    final List<? extends RecordTemplate> entities = buildEntities(snapshot);

    final List<RelationshipUpdates> relationshipUpdates = new ArrayList<>();

    final List<RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshot(snapshot);
    for (RecordTemplate aspect : aspects) {
      BaseRelationshipBuilder relationshipBuilder = _relationshipBuildersMap.get(aspect.getClass());
      if (relationshipBuilder != null) {
        relationshipUpdates.addAll(relationshipBuilder.buildRelationships(urn, aspect));
      }
    }

    return new GraphUpdates(Collections.unmodifiableList(entities), Collections.unmodifiableList(relationshipUpdates));
  }

  @Nonnull
  protected abstract List<? extends RecordTemplate> buildEntities(@Nonnull SNAPSHOT snapshot);
}
```

```java
public class DatasetGraphBuilder extends BaseGraphBuilder<DatasetSnapshot> {
  private static final Set<BaseRelationshipBuilder> RELATIONSHIP_BUILDERS =
      Collections.unmodifiableSet(new HashSet<BaseRelationshipBuilder>() {
        {
          add(new DownstreamOfBuilderFromUpstreamLineage());
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

## 4. Ingestion into graph
The ingestion process for each [entity] is done by graph builders. 
The builders will be invoked whenever an [MAE] is received by [MAE Consumer Job]. 
Graph builders should be extended from BaseGraphBuilder. Check DatasetGraphBuilder as an example above. 
For the consumer job to consume those MAEs, you should add your graph builder to the [graph builder registry].

## 5. Graph queries
You can onboard the graph queries which fit to your specific use cases using [Query DAO]. 
You also need to create [rest.li](https://rest.li) APIs to serve your graph queries.
[BaseQueryDAO] provides an abstract implementation of several graph query APIs.
Refer to [DownstreamLineageResource] rest.li resource implementation to see a use case of graph queries.

[relationship]: ../what/relationship.md
[relationship models]: ../../metadata-models/build/mainSchemas/com/linkedin/metadata/relationship
[aspect]: ../what/aspect.md
[snapshot]: ../what/snapshot.md
[entity]: ../what/entity.md
[mae]: ../what/mxe.md#metadata-audit-event-mae
[mae consumer job]: ../architecture/metadata-ingestion.md#mae-consumer-job
[graph builder registry]: ../../metadata-builders/src/main/java/com/linkedin/metadata/builders/graph/RegisteredGraphBuilders.java
[query dao]: ../architecture/metadata-serving.md#query-dao
[BaseQueryDAO]: ../../metadata-dao/src/main/java/com/linkedin/metadata/dao/BaseQueryDAO.java
[DownstreamLineageResource]: ../../gms/impl/src/main/java/com/linkedin/metadata/resources/dataset/DownstreamLineageResource.java
