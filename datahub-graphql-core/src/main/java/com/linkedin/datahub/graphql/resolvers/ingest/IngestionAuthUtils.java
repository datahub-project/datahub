package com.linkedin.datahub.graphql.resolvers.ingest;

import static com.datahub.authorization.AuthUtil.isAuthorizedEntityType;
import static com.datahub.authorization.AuthUtil.isAuthorizedEntityUrns;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SECRETS_ENTITY_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.EXECUTE;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;

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

  /**
   * Whether the actor may execute (run/cancel/test) the given ingestion source. Uses the same
   * {@link com.linkedin.metadata.authorization.ApiOperation#EXECUTE} check as OpenAPI/Rest.li /
   * ExecuteIngestionAuthValidator ({@code EXECUTE_ENTITY} or {@code MANAGE_INGESTION}).
   */
  public static boolean canExecuteIngestion(
      @Nonnull QueryContext context, @Nonnull Urn ingestionSourceUrn) {
    return isAuthorizedEntityUrns(
        context.getOperationContext(), EXECUTE, List.of(ingestionSourceUrn));
  }

  /**
   * Type-level execute check when no specific ingestion source URN is available (e.g. rollback by
   * run id). Test connection remains gated on {@link #canManageIngestion} because it accepts a
   * client-supplied recipe.
   */
  public static boolean canExecuteIngestion(@Nonnull QueryContext context) {
    return isAuthorizedEntityType(
        context.getOperationContext(), EXECUTE, List.of(INGESTION_SOURCE_ENTITY_NAME));
  }

  public static boolean canManageSecrets(@Nonnull QueryContext context) {
    return isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(SECRETS_ENTITY_NAME));
  }

  private IngestionAuthUtils() {}
}
