package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchUpdateDeprecationInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DeprecationUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchUpdateDeprecationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final BatchUpdateDeprecationInput input =
        bindArgument(environment.getArgument("input"), BatchUpdateDeprecationInput.class);
    final List<ResourceRefInput> resources = input.getResources();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {

          // First, validate the resources
          validateInputResources(resources, context);

          try {
            // Then execute the bulk update
            batchUpdateDeprecation(
                input.getDeprecated(),
                input.getNote(),
                input.getDecommissionTime(),
                input.getReplacement(),
                resources,
                context);
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
    if (!DeprecationUtils.isAuthorizedToUpdateDeprecationForEntity(context, resourceUrn)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    LabelUtils.validateResource(
        context.getOperationContext(),
        resourceUrn,
        resource.getSubResource(),
        resource.getSubResourceType(),
        _entityService);
  }

  private void batchUpdateDeprecation(
      boolean deprecated,
      @Nullable String note,
      @Nullable Long decommissionTime,
      @Nullable String replacementUrn,
      List<ResourceRefInput> resources,
      QueryContext context) {
    log.debug(
        "Batch updating deprecation. deprecated: {}, note: {}, decommissionTime: {}, resources: {}"
            + "replacementUrn: {}, notificationConfig: {}",
        deprecated,
        note,
        decommissionTime,
        resources,
        replacementUrn);
    try {
      DeprecationUtils.updateDeprecationForResources(
          context.getOperationContext(),
          deprecated,
          note,
          decommissionTime,
          replacementUrn,
          resources,
          UrnUtils.getUrn(context.getActorUrn()),
          _entityService);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch update deprecated to %s for resources with urns %s!",
              deprecated,
              resources.stream()
                  .map(ResourceRefInput::getResourceUrn)
                  .collect(Collectors.toList())),
          e);
    }
  }
}
