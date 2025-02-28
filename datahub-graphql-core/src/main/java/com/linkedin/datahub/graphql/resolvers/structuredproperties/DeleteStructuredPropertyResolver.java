package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DeleteStructuredPropertyInput;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteStructuredPropertyResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  public DeleteStructuredPropertyResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final DeleteStructuredPropertyInput input =
        bindArgument(environment.getArgument("input"), DeleteStructuredPropertyInput.class);
    final Urn propertyUrn = UrnUtils.getUrn(input.getUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!AuthorizationUtils.canManageStructuredProperties(context)) {
              throw new AuthorizationException(
                  "Unable to delete structured property. Please contact your admin.");
            }
            _entityClient.deleteEntity(context.getOperationContext(), propertyUrn);
            // Asynchronously Delete all references to the entity (to return quickly)
            GraphQLConcurrencyUtils.supplyAsync(
                () -> {
                  try {
                    _entityClient.deleteEntityReferences(
                        context.getOperationContext(), propertyUrn);
                  } catch (Exception e) {
                    log.error(
                        String.format(
                            "Caught exception while attempting to clear all entity references for Structured Property with urn %s",
                            propertyUrn),
                        e);
                  }
                  return null;
                },
                this.getClass().getSimpleName(),
                "get");
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        });
  }
}
