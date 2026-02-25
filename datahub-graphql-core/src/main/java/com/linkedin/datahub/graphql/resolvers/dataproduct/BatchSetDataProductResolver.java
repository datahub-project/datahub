package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BatchSetDataProductInput;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchSetDataProductResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final DataProductService _dataProductService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final BatchSetDataProductInput input =
        bindArgument(environment.getArgument("input"), BatchSetDataProductInput.class);
    final String maybeDataProductUrn = input.getDataProductUrn();
    final List<String> resources = input.getResourceUrns();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          DataProductResolverUtils.verifyResources(resources, context, _dataProductService);
          DataProductResolverUtils.verifyDataProduct(
              maybeDataProductUrn, context, _dataProductService);

          try {
            final List<ResourceReference> resourceReferences =
                DataProductResolverUtils.convertToResourceReferences(resources);
            if (maybeDataProductUrn != null) {
              batchSetDataProduct(maybeDataProductUrn, resourceReferences, context);
            } else {
              batchUnsetDataProduct(resourceReferences, context);
            }
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

  private void batchSetDataProduct(
      @Nonnull final String dataProductUrn,
      @Nonnull final List<ResourceReference> resourceReferences,
      @Nonnull final QueryContext context) {
    log.debug(
        "Batch setting Data Product. dataProduct urn: {}, resources: {}",
        dataProductUrn,
        resourceReferences);
    try {
      _dataProductService.batchSetDataProduct(
          context.getOperationContext(),
          UrnUtils.getUrn(dataProductUrn),
          resourceReferences,
          null); // GraphQL operations don't use appSource
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch set Data Product %s to resources with urns %s!",
              dataProductUrn, resourceReferences),
          e);
    }
  }

  private void batchUnsetDataProduct(
      @Nonnull final List<ResourceReference> resourceReferences,
      @Nonnull final QueryContext context) {
    log.debug("Batch unsetting Data Product. resources: {}", resourceReferences);
    try {
      _dataProductService.batchUnsetDataProduct(
          context.getOperationContext(), resourceReferences, null);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch unset data product for resources with urns %s!", resourceReferences),
          e);
    }
  }
}
