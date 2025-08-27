package com.linkedin.datahub.graphql.resolvers.role;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.policy.DataHubRoleInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver for deleting a DataHub Role */
@Slf4j
@RequiredArgsConstructor
public class DeleteRoleResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final String roleUrn = environment.getArgument("urn");

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // Check if the user has permission to delete roles
            if (!AuthorizationUtils.canManagePolicies(context)) {
              throw new AuthorizationException(
                  "Unauthorized to delete DataHub Roles. Please contact your DataHub administrator.");
            }

            final Urn urn = UrnUtils.getUrn(roleUrn);

            // Fetch the existing role to validate it exists and is editable
            final EntityResponse entityResponse =
                _entityClient.getV2(
                    context.getOperationContext(), Constants.DATAHUB_ROLE_ENTITY_NAME, urn, null);

            if (entityResponse == null) {
              throw new IllegalArgumentException("Role not found: " + roleUrn);
            }

            // Get the existing role info
            final DataHubRoleInfo existingRoleInfo =
                new DataHubRoleInfo(
                    entityResponse
                        .getAspects()
                        .get(Constants.DATAHUB_ROLE_INFO_ASPECT_NAME)
                        .getValue()
                        .data());

            // Check if the role is editable (system roles cannot be deleted)
            if (existingRoleInfo.hasEditable() && !existingRoleInfo.isEditable()) {
              throw new IllegalArgumentException(
                  "Cannot delete system role: " + existingRoleInfo.getName());
            }

            // Delete the role entity
            _entityClient.deleteEntity(context.getOperationContext(), urn);

            log.info("Successfully deleted DataHub Role with URN: {}", urn);
            return true;

          } catch (Exception e) {
            log.error("Failed to delete DataHub Role", e);
            throw new RuntimeException("Failed to delete DataHub Role: " + e.getMessage(), e);
          }
        });
  }
}
