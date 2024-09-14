package com.linkedin.datahub.graphql.resolvers.ingest;

import static com.datahub.authorization.AuthUtil.isAuthorizedEntityType;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SECRETS_ENTITY_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;

import com.datahub.authorization.AuthUtil;
import com.linkedin.datahub.graphql.QueryContext;
import java.util.List;
import javax.annotation.Nonnull;

public class IngestionAuthUtils {

  public static boolean canManageIngestion(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(INGESTION_SOURCE_ENTITY_NAME));
  }

  public static boolean canManageSecrets(@Nonnull QueryContext context) {
    return isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(SECRETS_ENTITY_NAME));
  }

  private IngestionAuthUtils() {}
}
