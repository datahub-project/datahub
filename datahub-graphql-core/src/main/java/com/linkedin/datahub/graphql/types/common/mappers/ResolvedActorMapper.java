/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
