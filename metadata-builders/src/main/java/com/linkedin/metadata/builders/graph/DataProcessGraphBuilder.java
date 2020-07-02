package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.graph.relationship.BaseRelationshipBuilder;
import com.linkedin.metadata.builders.graph.relationship.OwnedByBuilderFromOwnership;
import com.linkedin.metadata.entity.DataProcessEntity;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;


public class DataProcessGraphBuilder extends BaseGraphBuilder<DataProcessSnapshot> {

  private static final Set<BaseRelationshipBuilder> RELATIONSHIP_BUILDERS =
    Collections.unmodifiableSet(new HashSet<BaseRelationshipBuilder>() {
      {
        add(new OwnedByBuilderFromOwnership());
      }
    });

  public DataProcessGraphBuilder() {
    super(DataProcessSnapshot.class, RELATIONSHIP_BUILDERS);
  }

  @Nonnull
  @Override
  protected List<? extends RecordTemplate> buildEntities(@Nonnull DataProcessSnapshot snapshot) {
    final DataProcessUrn urn = snapshot.getUrn();
    final DataProcessEntity entity = new DataProcessEntity().setUrn(urn)
        .setName(urn.getNameEntity())
        .setOrchestrator(urn.getOrchestrator())
        .setOrigin(urn.getOriginEntity());

    return Collections.singletonList(entity);
  }
}
