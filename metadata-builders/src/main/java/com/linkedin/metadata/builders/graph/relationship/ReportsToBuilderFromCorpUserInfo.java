package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.Urn;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.ReportsTo;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;


public class ReportsToBuilderFromCorpUserInfo extends BaseRelationshipBuilder<CorpUserInfo> {

  public ReportsToBuilderFromCorpUserInfo() {
    super(CorpUserInfo.class);
  }

  @Nonnull
  @Override
  public List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull Urn urn, @Nonnull CorpUserInfo userInfo) {
    if (userInfo.getManagerUrn() == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(new GraphBuilder.RelationshipUpdates(
        Collections.singletonList(new ReportsTo().setSource(urn).setDestination(userInfo.getManagerUrn())),
        REMOVE_ALL_EDGES_FROM_SOURCE));
  }
}
