package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AddOwnersInput;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AddOwnersResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final AddOwnersInput input =
        bindArgument(environment.getArgument("input"), AddOwnersInput.class);
    List<OwnerInput> owners = input.getOwners();
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          if (!OwnerUtils.isAuthorizedToUpdateOwners(environment.getContext(), targetUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          OwnerUtils.validateAddOwnerInput(owners, targetUrn, _entityService);
          try {

            log.debug("Adding Owners. input: {}", input);

            Urn actor =
                CorpuserUrn.createFromString(
                    ((QueryContext) environment.getContext()).getActorUrn());
            OwnerUtils.addOwnersToResources(
                owners,
                ImmutableList.of(new ResourceRefInput(input.getResourceUrn(), null, null)),
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error("Failed to add owners to resource with input {}, {}", input, e.getMessage());
            throw new RuntimeException(
                String.format("Failed to add owners to resource with input %s", input), e);
          }
        });
  }
}
