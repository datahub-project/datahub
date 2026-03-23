package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.policy.PolicyAuthUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.policy.DataHubRoleInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeleteRoleResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    if (!PolicyAuthUtils.canManagePolicies(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    final String roleUrnStr = environment.getArgument("urn");

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn roleUrn = Urn.createFromString(roleUrnStr);

            // Verify the role exists and is editable before deleting.
            final EntityResponse existingRole =
                _entityClient
                    .batchGetV2(
                        context.getOperationContext(),
                        DATAHUB_ROLE_ENTITY_NAME,
                        Collections.singleton(roleUrn),
                        Collections.singleton(DATAHUB_ROLE_INFO_ASPECT_NAME))
                    .get(roleUrn);

            if (existingRole == null) {
              throw new RuntimeException(
                  String.format("Role with urn %s does not exist", roleUrnStr));
            }

            final DataHubRoleInfo roleInfo =
                new DataHubRoleInfo(
                    existingRole.getAspects().get(DATAHUB_ROLE_INFO_ASPECT_NAME).getValue().data());
            if (!roleInfo.isEditable()) {
              throw new RuntimeException(
                  String.format(
                      "Role with urn %s is a built-in role and cannot be deleted", roleUrnStr));
            }

            _entityClient.deleteEntity(context.getOperationContext(), roleUrn);
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to delete role with urn %s", roleUrnStr), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
