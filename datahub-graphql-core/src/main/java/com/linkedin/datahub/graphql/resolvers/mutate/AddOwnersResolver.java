package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.urn.CorpuserUrn;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AddOwnersInput;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
@RequiredArgsConstructor
public class AddOwnersResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final AddOwnersInput input = bindArgument(environment.getArgument("input"), AddOwnersInput.class);
    List<OwnerInput> owners = input.getOwners();
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());

    return CompletableFuture.supplyAsync(() -> {

      if (!OwnerUtils.isAuthorizedToUpdateOwners(environment.getContext(), targetUrn)) {
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
      }

      OwnerUtils.validateAddInput(
          owners,
          targetUrn,
          _entityService
      );
      try {

        log.debug("Adding Owners. input: {}", input.toString());

        Urn actor = CorpuserUrn.createFromString(((QueryContext) environment.getContext()).getActorUrn());
        OwnerUtils.addOwners(
            owners,
            targetUrn,
            actor,
            _entityService
        );
        return true;
      } catch (Exception e) {
        log.error("Failed to add owners to resource with input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to add owners to resource with input %s", input.toString()), e);
      }
    });
  }
}