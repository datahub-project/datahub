package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.DataJobEntity;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.IsPartOf;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE;


public class IsPartOfBuilderFromDataFlow extends BaseRelationshipBuilder<DataJobEntity> {
  public IsPartOfBuilderFromDataFlow() {
    super(DataJobEntity.class);
  }

  @Nonnull
  @Override
  public List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull Urn urn, @Nonnull DataJobEntity dataJob) {
    if (dataJob.getFlow() == null) {
      return Collections.emptyList();
    }
    final List<IsPartOf> dataJobEdges = new ArrayList<>();
    dataJobEdges.add(new IsPartOf().setSource(dataJob.getUrn()).setDestination(dataJob.getFlow()));
    return Collections.singletonList(
        new GraphBuilder.RelationshipUpdates(dataJobEdges, REMOVE_ALL_EDGES_FROM_SOURCE)
    );
  }
}
