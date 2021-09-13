package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.IsPartOf;

import javax.annotation.Nonnull;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE;


public class IsPartOfBuilderFromDataFlow extends BaseRelationshipBuilder<DataJobInfo> {
  public IsPartOfBuilderFromDataFlow() {
    super(DataJobInfo.class);
  }

  @Nonnull
  @Override
  public List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull Urn sourceUrn, @Nonnull DataJobInfo dataJobInfo) {
    final List<IsPartOf> dataJobEdges = new ArrayList<>();
    try {
      dataJobEdges.add(new IsPartOf().setSource(sourceUrn).setDestination(DataJobUrn.createFromUrn(sourceUrn).getFlowEntity()));
      return Collections.singletonList(
              new GraphBuilder.RelationshipUpdates(dataJobEdges, REMOVE_ALL_EDGES_FROM_SOURCE)
      );
    } catch (URISyntaxException e) {
      return Collections.emptyList();
    }
  }
}
