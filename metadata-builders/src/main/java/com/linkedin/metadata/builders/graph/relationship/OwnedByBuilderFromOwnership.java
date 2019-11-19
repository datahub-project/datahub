package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.OwnedBy;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;


public class OwnedByBuilderFromOwnership extends BaseRelationshipBuilder<Ownership> {

  private static final String CORPUSER_URN_TYPE = "corpuser";

  public OwnedByBuilderFromOwnership() {
    super(Ownership.class);
  }

  @Nonnull
  @Override
  public List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull Urn urn, @Nonnull Ownership ownership) {
    // currently only support OwnedBy Corpuser in models
    final List<OwnedBy> ownerList = ownership.getOwners()
        .stream()
        .filter(owner -> CORPUSER_URN_TYPE.equals(owner.getOwner().getEntityType()))
        .map(owner -> new OwnedBy().setSource(urn).setDestination(owner.getOwner()).setType(owner.getType()))
        .collect(Collectors.toList());

    return Collections.singletonList(new GraphBuilder.RelationshipUpdates(ownerList, REMOVE_ALL_EDGES_FROM_SOURCE));
  }
}
