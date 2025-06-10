package com.linkedin.datahub.graphql.resolvers.ingest;

import static com.datahub.authorization.AuthUtil.isAuthorizedEntityType;
import static com.datahub.authorization.AuthUtil.isAuthorizedEntityUrns;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SECRETS_ENTITY_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.*;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import java.util.List;
import javax.annotation.Nonnull;

public class IngestionAuthUtils {

  public static boolean canManageIngestion(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(INGESTION_SOURCE_ENTITY_NAME));
  }

  public static boolean canViewIngestion(@Nonnull QueryContext context, @Nonnull Urn urn) {
    return isAuthorizedEntityUrns(context.getOperationContext(), READ, List.of(urn));
  }

  public static boolean canEditIngestion(@Nonnull QueryContext context, @Nonnull Urn urn) {
    return isAuthorizedEntityUrns(context.getOperationContext(), UPDATE, List.of(urn));
  }

  public static boolean canDeleteIngestion(@Nonnull QueryContext context, @Nonnull Urn urn) {
    return isAuthorizedEntityUrns(context.getOperationContext(), DELETE, List.of(urn));
  }

  public static boolean canExecuteIngestion(@Nonnull QueryContext context, @Nonnull Urn urn) {
    return isAuthorizedEntityUrns(context.getOperationContext(), EXECUTE, List.of(urn));
  }

  public static boolean canManageSecrets(@Nonnull QueryContext context) {
    return isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(SECRETS_ENTITY_NAME));
  }

  private IngestionAuthUtils() {}
}
