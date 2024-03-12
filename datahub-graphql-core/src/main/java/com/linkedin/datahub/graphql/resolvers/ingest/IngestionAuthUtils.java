package com.linkedin.datahub.graphql.resolvers.ingest;

import static com.datahub.authorization.AuthUtil.isAuthorized;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import javax.annotation.Nonnull;

public class IngestionAuthUtils {

  public static boolean canManageIngestion(@Nonnull QueryContext context) {
    final Authorizer authorizer = context.getAuthorizer();
    final String principal = context.getActorUrn();
    return isAuthorized(authorizer, principal, PoliciesConfig.MANAGE_INGESTION_PRIVILEGE);
  }

  public static boolean canManageSecrets(@Nonnull QueryContext context) {
    final Authorizer authorizer = context.getAuthorizer();
    final String principal = context.getActorUrn();
    return isAuthorized(authorizer, principal, PoliciesConfig.MANAGE_SECRETS_PRIVILEGE);
  }

  private IngestionAuthUtils() {}
}
