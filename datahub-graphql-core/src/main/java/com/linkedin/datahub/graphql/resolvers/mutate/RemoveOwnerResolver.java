package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.RemoveOwnerInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RemoveOwnerResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final RemoveOwnerInput input =
        bindArgument(environment.getArgument("input"), RemoveOwnerInput.class);

    Urn ownerUrn = Urn.createFromString(input.getOwnerUrn());
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());
    Urn ownershipTypeUrn =
        input.getOwnershipTypeUrn() == null
            ? null
            : Urn.createFromString(input.getOwnershipTypeUrn());

    OwnerUtils.validateAuthorizedToUpdateOwners(context, targetUrn, _entityClient);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          OwnerUtils.validateRemoveInput(context.getOperationContext(), targetUrn, _entityService);
          try {
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            OwnerUtils.removeOwnersFromResources(
                context.getOperationContext(),
                ImmutableList.of(ownerUrn),
                ownershipTypeUrn,
                ImmutableList.of(new ResourceRefInput(input.getResourceUrn(), null, null)),
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error("Failed to remove owner from resource with input {}", input);
            throw new RuntimeException(
                String.format(
                    "Failed to remove owner from resource with input  %s", input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
