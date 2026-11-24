package com.linkedin.datahub.graphql.resolvers.customfabrictype;

import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.ApiOperation;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/**
 * Deletes a custom environment. CustomFabricTypeDeleteValidator rejects the delete while any entity
 * still uses it, so nothing is left pointing at a deleted environment.
 */
@RequiredArgsConstructor
public class DeleteCustomFabricTypeResolver implements DataFetcher<CompletableFuture<Boolean>> {

  @Nonnull private final EntityClient entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn urn = UrnUtils.getUrn(environment.getArgument("urn"));

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!CUSTOM_FABRIC_TYPE_ENTITY_NAME.equals(urn.getEntityType())) {
            throw new DataHubGraphQLException(
                String.format("%s is not an environment.", urn),
                DataHubGraphQLErrorCode.BAD_REQUEST);
          }
          if (!AuthorizationUtils.canManageCustomFabricType(context, ApiOperation.DELETE, urn)) {
            throw new AuthorizationException(
                "Unauthorized to delete environments. Please contact your DataHub"
                    + " administrator.");
          }
          try {
            entityClient.deleteEntity(context.getOperationContext(), urn);
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to delete environment %s: %s", urn, e.getMessage()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
