package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmbedUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  private EmbedUtils() {}

  public static boolean isAuthorizedToUpdateEmbedForEntity(
      @Nonnull final Urn entityUrn, @Nonnull final QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_EMBED_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, entityUrn.getEntityType(), entityUrn.toString(), orPrivilegeGroups);
  }
}
