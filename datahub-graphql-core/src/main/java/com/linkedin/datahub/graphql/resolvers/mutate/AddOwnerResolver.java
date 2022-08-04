package com.linkedin.datahub.graphql.resolvers.mutate;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.CorpuserUrn;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AddOwnerInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
@RequiredArgsConstructor
public class AddOwnerResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final AddOwnerInput input = bindArgument(environment.getArgument("input"), AddOwnerInput.class);

    Urn ownerUrn = Urn.createFromString(input.getOwnerUrn());
    OwnerEntityType ownerEntityType = input.getOwnerEntityType();
    OwnershipType type = input.getType() == null ? OwnershipType.NONE : input.getType();
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());

    if (!OwnerUtils.isAuthorizedToUpdateOwners(environment.getContext(), targetUrn)) {
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return CompletableFuture.supplyAsync(() -> {
      OwnerUtils.validateAddInput(
          ownerUrn,
          ownerEntityType,
          targetUrn,
          _entityService
      );
      try {

        log.debug("Adding Owner. input: {}", input.toString());

        Urn actor = CorpuserUrn.createFromString(((QueryContext) environment.getContext()).getActorUrn());
        OwnerUtils.addOwnersToResources(
            ImmutableList.of(new OwnerInput(input.getOwnerUrn(), ownerEntityType, type)),
            ImmutableList.of(new ResourceRefInput(input.getResourceUrn(), null, null)),
            actor,
            _entityService
        );
        return true;
      } catch (Exception e) {
        log.error("Failed to add owner to resource with input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to add owner to resource with input %s", input.toString()), e);
      }
    });
  }
}
