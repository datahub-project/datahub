package com.linkedin.datahub.graphql.resolvers.businessattribute;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for hard deleting a particular Business Attribute */
@Slf4j
@RequiredArgsConstructor
public class DeleteBusinessAttributeResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn businessAttributeUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    if (!AuthorizationUtils.isEntityType(
            businessAttributeUrn, Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME)
        || !BusinessAttributeAuthorizationUtils.canManageBusinessAttribute(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    if (!_entityClient.exists(context.getOperationContext(), businessAttributeUrn)) {
      throw new RuntimeException(
          String.format("This urn does not exist: %s", businessAttributeUrn));
    }
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final var references =
                _entityClient.deleteReferencesThenEntity(
                    context.getOperationContext(), businessAttributeUrn);
            // The entity is already gone. This removes the remaining references.
            CompletableFuture.runAsync(
                () -> {
                  try {
                    references.run();
                  } catch (Exception e) {
                    log.error(
                        String.format(
                            "Exception while attempting to clear all entity references for Business Attribute with urn %s",
                            businessAttributeUrn),
                        e);
                  }
                });
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to delete Business Attribute with urn %s", businessAttributeUrn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
