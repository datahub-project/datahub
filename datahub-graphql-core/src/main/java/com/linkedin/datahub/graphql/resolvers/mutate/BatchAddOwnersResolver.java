package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BatchAddOwnersInput;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchAddOwnersResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final BatchAddOwnersInput input =
        bindArgument(environment.getArgument("input"), BatchAddOwnersInput.class);
    final List<OwnerInput> owners = input.getOwners();
    final List<ResourceRefInput> resources = input.getResources();
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {

          // First, validate the batch
          validateOwners(context.getOperationContext(), owners);
          validateInputResources(context.getOperationContext(), resources, context);

          try {
            // Then execute the bulk add
            batchAddOwners(context.getOperationContext(), owners, resources, context);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateOwners(@Nonnull OperationContext opContext, List<OwnerInput> owners) {
    for (OwnerInput ownerInput : owners) {
      OwnerUtils.validateOwner(opContext, ownerInput, _entityService);
    }
  }

  private void validateInputResources(
      @Nonnull OperationContext opContext, List<ResourceRefInput> resources, QueryContext context) {
    for (ResourceRefInput resource : resources) {
      validateInputResource(opContext, resource, context);
    }
  }

  private void validateInputResource(
      @Nonnull OperationContext opContext, ResourceRefInput resource, QueryContext context) {
    final Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());

    if (resource.getSubResource() != null) {
      throw new IllegalArgumentException(
          "Malformed input provided: owners cannot be applied to subresources.");
    }

    OwnerUtils.validateAuthorizedToUpdateOwners(context, resourceUrn, _entityClient);
    LabelUtils.validateResource(
        opContext,
        resourceUrn,
        resource.getSubResource(),
        resource.getSubResourceType(),
        _entityService);
  }

  private void batchAddOwners(
      @Nonnull OperationContext opContext,
      List<OwnerInput> owners,
      List<ResourceRefInput> resources,
      QueryContext context) {
    log.debug("Batch adding owners. owners: {}, resources: {}", owners, resources);
    try {
      OwnerUtils.addOwnersToResources(
          opContext, owners, resources, UrnUtils.getUrn(context.getActorUrn()), _entityService);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add Owners %s to resources with urns %s!",
              owners,
              resources.stream()
                  .map(ResourceRefInput::getResourceUrn)
                  .collect(Collectors.toList())),
          e);
    }
  }
}
