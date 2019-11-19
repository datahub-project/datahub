package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.Urn;
import com.linkedin.datasetGroup.DatasetGroupMembership;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.IsPartOf;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;


public class IsPartOfBuilderFromDatasetGroupMembership extends BaseRelationshipBuilder<DatasetGroupMembership> {

  public IsPartOfBuilderFromDatasetGroupMembership() {
    super(DatasetGroupMembership.class);
  }

  @Nonnull
  @Override
  public List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull Urn datasetGroupUrn,
      @Nonnull DatasetGroupMembership datasetGroupMembership) {

    final List<IsPartOf> membership = datasetGroupMembership.getDatasets()
        .stream()
        .map(datasetUrn -> new IsPartOf().setSource(datasetUrn).setDestination(datasetGroupUrn))
        .collect(Collectors.toList());

    return Collections.singletonList(
        new GraphBuilder.RelationshipUpdates(membership, REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION));
  }
}
