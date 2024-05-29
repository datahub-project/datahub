package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchUpdateSoftDeletedInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DeleteUtils;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchUpdateSoftDeletedResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService<?> _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final BatchUpdateSoftDeletedInput input =
        bindArgument(environment.getArgument("input"), BatchUpdateSoftDeletedInput.class);
    final List<String> urns = input.getUrns();
    final boolean deleted = input.getDeleted();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {

          // First, validate the entities exist
          validateInputUrns(urns, context);

          try {
            // Then execute the bulk soft delete
            batchUpdateSoftDeleted(deleted, urns, context);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform batch soft delete against input {}, {}",
                input.toString(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to perform batch soft delete against input %s", input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateInputUrns(List<String> urnStrs, QueryContext context) {
    for (String urnStr : urnStrs) {
      validateInputUrn(urnStr, context);
    }
  }

  private void validateInputUrn(String urnStr, QueryContext context) {
    final Urn urn = UrnUtils.getUrn(urnStr);
    if (!DeleteUtils.isAuthorizedToDeleteEntity(context, urn)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    if (!_entityService.exists(context.getOperationContext(), urn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to soft delete entity with urn %s. Entity does not exist.", urn));
    }
  }

  private void batchUpdateSoftDeleted(boolean removed, List<String> urnStrs, QueryContext context) {
    log.debug("Batch soft deleting assets. urns: {}", urnStrs);
    try {
      DeleteUtils.updateStatusForResources(
          context.getOperationContext(),
          removed,
          urnStrs,
          UrnUtils.getUrn(context.getActorUrn()),
          _entityService);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch update soft deleted status entities with urns %s!", urnStrs),
          e);
    }
  }
}
