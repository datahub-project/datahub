package com.linkedin.metadata.builders.graph;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.graph.relationship.BaseRelationshipBuilder;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.validator.EntityValidator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


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

  protected <ENTITY extends RecordTemplate> void setRemovedProperty(@Nonnull SNAPSHOT snapshot, @Nonnull ENTITY entity) {
    EntityValidator.validateEntitySchema(entity.getClass());
    final Optional<Status> statusAspect = ModelUtils.getAspectFromSnapshot(snapshot, Status.class);
    if (statusAspect.isPresent()) {
      RecordUtils.setRecordTemplatePrimitiveField(entity, "removed", (statusAspect.get()).isRemoved());
    }
  }
}
