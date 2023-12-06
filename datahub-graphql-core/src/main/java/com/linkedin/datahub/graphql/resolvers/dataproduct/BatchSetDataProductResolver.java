package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchSetDataProductInput;
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
public class BatchSetDataProductResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final DataProductService _dataProductService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final BatchSetDataProductInput input =
        bindArgument(environment.getArgument("input"), BatchSetDataProductInput.class);
    final String maybeDataProductUrn = input.getDataProductUrn();
    final List<String> resources = input.getResourceUrns();

    return CompletableFuture.supplyAsync(
        () -> {
          verifyResources(resources, context);
          verifyDataProduct(maybeDataProductUrn, context);

          try {
            List<Urn> resourceUrns =
                resources.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
            if (maybeDataProductUrn != null) {
              batchSetDataProduct(maybeDataProductUrn, resourceUrns, context);
            } else {
              batchUnsetDataProduct(resourceUrns, context);
            }
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        });
  }

  private void verifyResources(List<String> resources, QueryContext context) {
    for (String resource : resources) {
      if (!_dataProductService.verifyEntityExists(
          UrnUtils.getUrn(resource), context.getAuthentication())) {
        throw new RuntimeException(
            String.format(
                "Failed to batch set Data Product, %s in resources does not exist", resource));
      }
      Urn resourceUrn = UrnUtils.getUrn(resource);
      if (!DataProductAuthorizationUtils.isAuthorizedToUpdateDataProductsForEntity(
          context, resourceUrn)) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }
    }
  }

  private void verifyDataProduct(String maybeDataProductUrn, QueryContext context) {
    if (maybeDataProductUrn != null
        && !_dataProductService.verifyEntityExists(
            UrnUtils.getUrn(maybeDataProductUrn), context.getAuthentication())) {
      throw new RuntimeException(
          String.format(
              "Failed to batch set Data Product, Data Product urn %s does not exist",
              maybeDataProductUrn));
    }
  }

  private void batchSetDataProduct(
      @Nonnull String dataProductUrn, List<Urn> resources, QueryContext context) {
    log.debug(
        "Batch setting Data Product. dataProduct urn: {}, resources: {}",
        dataProductUrn,
        resources);
    try {
      _dataProductService.batchSetDataProduct(
          UrnUtils.getUrn(dataProductUrn),
          resources,
          context.getAuthentication(),
          UrnUtils.getUrn(context.getActorUrn()));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch set Data Product %s to resources with urns %s!",
              dataProductUrn, resources),
          e);
    }
  }

  private void batchUnsetDataProduct(List<Urn> resources, QueryContext context) {
    log.debug("Batch unsetting Data Product. resources: {}", resources);
    try {
      for (Urn resource : resources) {
        _dataProductService.unsetDataProduct(
            resource, context.getAuthentication(), UrnUtils.getUrn(context.getActorUrn()));
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch unset data product for resources with urns %s!", resources),
          e);
    }
  }
}
