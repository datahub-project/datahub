package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchRemoveTermsInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
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
public class BatchRemoveTermsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final BatchRemoveTermsInput input =
        bindArgument(environment.getArgument("input"), BatchRemoveTermsInput.class);
    final List<Urn> termUrns =
        input.getTermUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    final List<ResourceRefInput> resources = input.getResources();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {

          // First, validate the batch
          validateInputResources(context.getOperationContext(), resources, context);

          try {
            // Then execute the bulk add
            batchRemoveTerms(context.getOperationContext(), termUrns, resources, context);
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

  private void validateInputResources(
      @Nonnull OperationContext opContext, List<ResourceRefInput> resources, QueryContext context) {
    for (ResourceRefInput resource : resources) {
      validateInputResource(opContext, resource, context);
    }
  }

  private void validateInputResource(
      @Nonnull OperationContext opContext, ResourceRefInput resource, QueryContext context) {
    final Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());
    if (!LabelUtils.isAuthorizedToUpdateTerms(context, resourceUrn, resource.getSubResource())) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    LabelUtils.validateResource(
        opContext,
        resourceUrn,
        resource.getSubResource(),
        resource.getSubResourceType(),
        _entityService);
  }

  private void batchRemoveTerms(
      @Nonnull OperationContext opContext,
      List<Urn> termUrns,
      List<ResourceRefInput> resources,
      QueryContext context) {
    log.debug("Batch removing Terms. terms: {}, resources: {}", resources, termUrns);
    try {
      LabelUtils.removeTermsFromResources(
          opContext, termUrns, resources, UrnUtils.getUrn(context.getActorUrn()), _entityService);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to remove Terms %s to resources with urns %s!",
              termUrns,
              resources.stream()
                  .map(ResourceRefInput::getResourceUrn)
                  .collect(Collectors.toList())),
          e);
    }
  }
}
