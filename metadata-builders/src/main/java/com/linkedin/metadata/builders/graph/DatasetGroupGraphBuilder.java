package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.DatasetGroupUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.graph.relationship.BaseRelationshipBuilder;
import com.linkedin.metadata.builders.graph.relationship.IsPartOfBuilderFromDatasetGroupMembership;
import com.linkedin.metadata.entity.DatasetGroupEntity;
import com.linkedin.metadata.snapshot.DatasetGroupSnapshot;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;


public class DatasetGroupGraphBuilder extends BaseGraphBuilder<DatasetGroupSnapshot> {

  private static final Set<BaseRelationshipBuilder> RELATIONSHIP_BUILDERS =
      Collections.unmodifiableSet(new HashSet<BaseRelationshipBuilder>() {
        {
          add(new IsPartOfBuilderFromDatasetGroupMembership());
        }
      });

  public DatasetGroupGraphBuilder() {
    super(DatasetGroupSnapshot.class, RELATIONSHIP_BUILDERS);
  }

  @Nonnull
  @Override
  protected List<? extends RecordTemplate> buildEntities(@Nonnull DatasetGroupSnapshot snapshot) {
    final DatasetGroupUrn urn = snapshot.getUrn();
    final DatasetGroupEntity entity = new DatasetGroupEntity().setUrn(urn)
        .setRemoved(false)
        .setNamespace(urn.getNamespaceEntity())
        .setName(urn.getNameEntity());

    return Collections.singletonList(entity);
  }
}
