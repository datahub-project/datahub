package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.BatchSetDataProductsInput;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchAddToDataProductsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final DataProductService _dataProductService;
  private final FeatureFlags _featureFlags;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    // Check feature flag
    if (!_featureFlags.isMultipleDataProductsPerAsset()) {
      throw new AuthorizationException(
          "Multiple data products per asset feature is not enabled. Please contact your DataHub administrator.");
    }

    final BatchSetDataProductsInput input =
        bindArgument(environment.getArgument("input"), BatchSetDataProductsInput.class);
    final List<String> dataProductUrns = input.getDataProductUrns();
    final List<String> resourceUrns = input.getResourceUrns();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Verify all resources exist and user has permission
          DataProductResolverUtils.verifyResources(resourceUrns, context, _dataProductService);

          // Verify all data products exist
          DataProductResolverUtils.verifyDataProducts(
              dataProductUrns, context, _dataProductService);

          try {
            final List<Urn> resourceUrnList =
                resourceUrns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

            // Add resources to each data product
            for (String dataProductUrn : dataProductUrns) {
              batchAddToDataProduct(dataProductUrn, resourceUrnList, context);
            }

            return true;
          } catch (Exception e) {
            log.error(
                "Failed to add resources to data products. input: {}, error: {}",
                input,
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to add resources to data products. input: %s", input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void batchAddToDataProduct(
      @Nonnull String dataProductUrn, List<Urn> resourceUrnList, QueryContext context) {
    log.debug(
        "Batch adding to Data Product. dataProduct urn: {}, resources: {}",
        dataProductUrn,
        resourceUrnList);
    try {
      _dataProductService.batchAddToDataProduct(
          context.getOperationContext(), UrnUtils.getUrn(dataProductUrn), resourceUrnList);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to batch add resources to Data Product %s!", dataProductUrn), e);
    }
  }
}
