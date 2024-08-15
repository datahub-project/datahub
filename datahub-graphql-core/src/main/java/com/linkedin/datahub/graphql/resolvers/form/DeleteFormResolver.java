package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DeleteFormInput;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteFormResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  public DeleteFormResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final DeleteFormInput input =
        bindArgument(environment.getArgument("input"), DeleteFormInput.class);
    final Urn formUrn = UrnUtils.getUrn(input.getUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!AuthorizationUtils.canManageForms(context)) {
              throw new AuthorizationException("Unable to delete form. Please contact your admin.");
            }
            _entityClient.deleteEntity(context.getOperationContext(), formUrn);
            // Asynchronously Delete all references to the entity (to return quickly)
            CompletableFuture.runAsync(
                () -> {
                  try {
                    _entityClient.deleteEntityReferences(context.getOperationContext(), formUrn);
                  } catch (Exception e) {
                    log.error(
                        String.format(
                            "Caught exception while attempting to clear all entity references for Form with urn %s",
                            formUrn),
                        e);
                  }
                });

            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        });
  }
}
