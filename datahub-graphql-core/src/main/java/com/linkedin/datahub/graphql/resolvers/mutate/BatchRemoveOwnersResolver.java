package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BatchRemoveOwnersInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchRemoveOwnersResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final BatchRemoveOwnersInput input =
        bindArgument(environment.getArgument("input"), BatchRemoveOwnersInput.class);
    final List<String> owners = input.getOwnerUrns();
    final List<ResourceRefInput> resources = input.getResources();
    final Urn ownershipTypeUrn =
        input.getOwnershipTypeUrn() == null
            ? null
            : Urn.createFromString(input.getOwnershipTypeUrn());
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {

          // First, validate the batch
          validateInputResources(resources, context);

          try {
            // Then execute the bulk remove
            batchRemoveOwners(owners, ownershipTypeUrn, resources, context);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateInputResources(List<ResourceRefInput> resources, QueryContext context) {
    for (ResourceRefInput resource : resources) {
      validateInputResource(resource, context);
    }
  }

  private void validateInputResource(ResourceRefInput resource, QueryContext context) {
    final Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());

    if (resource.getSubResource() != null) {
      throw new IllegalArgumentException(
          "Malformed input provided: owners cannot be removed from subresources.");
    }

    OwnerUtils.validateAuthorizedToUpdateOwners(context, resourceUrn, _entityClient);
    LabelUtils.validateResource(
        context.getOperationContext(),
        resourceUrn,
        resource.getSubResource(),
        resource.getSubResourceType(),
        _entityService);
  }

  private void batchRemoveOwners(
      List<String> ownerUrns,
      Urn ownershipTypeUrn,
      List<ResourceRefInput> resources,
      QueryContext context) {
    log.debug("Batch removing owners. owners: {}, resources: {}", ownerUrns, resources);
    try {
      OwnerUtils.removeOwnersFromResources(
          context.getOperationContext(),
          ownerUrns.stream().map(UrnUtils::getUrn).collect(Collectors.toList()),
          ownershipTypeUrn,
          resources,
          UrnUtils.getUrn(context.getActorUrn()),
          _entityService);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch remove Owners %s to resources with urns %s!",
              ownerUrns,
              resources.stream()
                  .map(ResourceRefInput::getResourceUrn)
                  .collect(Collectors.toList())),
          e);
    }
  }
}
