package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.RemoveOwnerInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RemoveOwnerResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final RemoveOwnerInput input =
        bindArgument(environment.getArgument("input"), RemoveOwnerInput.class);

    Urn ownerUrn = Urn.createFromString(input.getOwnerUrn());
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());
    Optional<Urn> maybeOwnershipTypeUrn =
        input.getOwnershipTypeUrn() == null
            ? Optional.empty()
            : Optional.of(Urn.createFromString(input.getOwnershipTypeUrn()));

    if (!OwnerUtils.isAuthorizedToUpdateOwners(environment.getContext(), targetUrn)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return CompletableFuture.supplyAsync(
        () -> {
          OwnerUtils.validateRemoveInput(targetUrn, _entityService);
          try {
            Urn actor =
                CorpuserUrn.createFromString(
                    ((QueryContext) environment.getContext()).getActorUrn());
            OwnerUtils.removeOwnersFromResources(
                ImmutableList.of(ownerUrn),
                maybeOwnershipTypeUrn,
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
        });
  }
}
