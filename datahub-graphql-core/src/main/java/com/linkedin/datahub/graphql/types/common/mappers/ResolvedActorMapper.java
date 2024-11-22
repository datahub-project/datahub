package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ResolvedActor;
import com.linkedin.metadata.Constants;
import javax.annotation.Nonnull;

public class ResolvedActorMapper {

  public static final ResolvedActorMapper INSTANCE = new ResolvedActorMapper();

  public static ResolvedActor map(@Nonnull final Urn actorUrn) {
    return INSTANCE.apply(actorUrn);
  }

  public ResolvedActor apply(@Nonnull final Urn actorUrn) {
    if (actorUrn.getEntityType().equals(Constants.CORP_GROUP_ENTITY_NAME)) {
      CorpGroup partialGroup = new CorpGroup();
      partialGroup.setUrn(actorUrn.toString());
      partialGroup.setType(EntityType.CORP_GROUP);
      return partialGroup;
    }
    CorpUser partialUser = new CorpUser();
    partialUser.setUrn(actorUrn.toString());
    partialUser.setType(EntityType.CORP_USER);
    return (ResolvedActor) partialUser;
  }
}
