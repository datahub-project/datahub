package com.linkedin.datahub.graphql.resolvers.ingest;

import static com.linkedin.datahub.graphql.resolvers.AuthUtils.*;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import javax.annotation.Nonnull;

public class IngestionAuthUtils {

  public static boolean canManageIngestion(@Nonnull QueryContext context) {
    final Authorizer authorizer = context.getAuthorizer();
    final String principal = context.getActorUrn();
    return isAuthorized(
        principal,
        ImmutableList.of(PoliciesConfig.MANAGE_INGESTION_PRIVILEGE.getType()),
        authorizer);
  }

  public static boolean canManageSecrets(@Nonnull QueryContext context) {
    final Authorizer authorizer = context.getAuthorizer();
    final String principal = context.getActorUrn();
    return isAuthorized(
        principal, ImmutableList.of(PoliciesConfig.MANAGE_SECRETS_PRIVILEGE.getType()), authorizer);
  }

  private IngestionAuthUtils() {}
}
