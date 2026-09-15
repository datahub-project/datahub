package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.UpdateDataProductInput;
import com.linkedin.datahub.graphql.types.dataproduct.mappers.DataProductMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateDataProductResolver implements DataFetcher<CompletableFuture<DataProduct>> {

  private final DataProductService _dataProductService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<DataProduct> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateDataProductInput input =
        bindArgument(environment.getArgument("input"), UpdateDataProductInput.class);
    final Urn dataProductUrn = UrnUtils.getUrn(environment.getArgument("urn"));

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!_dataProductService.verifyEntityExists(
              context.getOperationContext(), dataProductUrn)) {
            throw new IllegalArgumentException("The Data Product provided dos not exist");
          }

          Domains domains =
              _dataProductService.getDataProductDomains(
                  context.getOperationContext(), dataProductUrn);
          if (!DataProductAuthorizationUtils.isAuthorizedToManageDataProductsOnAnyDomain(
              context, domains)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            final Urn parentDataProductUrn =
                input.getParentDataProduct() != null
                    ? UrnUtils.getUrn(input.getParentDataProduct())
                    : null;
            if (parentDataProductUrn != null) {
              if (!parentDataProductUrn
                  .getEntityType()
                  .equals(Constants.DATA_PRODUCT_ENTITY_NAME)) {
                throw new IllegalArgumentException("Parent entity is not a data product.");
              }
              if (!_dataProductService.verifyEntityExists(
                  context.getOperationContext(), parentDataProductUrn)) {
                throw new IllegalArgumentException("Parent Data Product does not exist");
              }
              if (parentDataProductUrn.equals(dataProductUrn)
                  || DataProductAncestorUtils.walkParentChain(
                          context.getOperationContext(), _entityClient, parentDataProductUrn)
                      .contains(dataProductUrn)) {
                throw new DataHubGraphQLException(
                    "Cannot move a data product under one of its own descendants.",
                    DataHubGraphQLErrorCode.BAD_REQUEST);
              }
            }

            final Urn urn =
                _dataProductService.updateDataProduct(
                    context.getOperationContext(),
                    dataProductUrn,
                    input.getName(),
                    input.getDescription(),
                    parentDataProductUrn);
            EntityResponse response =
                _dataProductService.getDataProductEntityResponse(
                    context.getOperationContext(), urn);
            if (response != null) {
              return DataProductMapper.map(context, response);
            }
            // should never happen
            log.error(String.format("Unable to find data product with urn %s", dataProductUrn));
            return null;
          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to update DataProduct with urn %s", dataProductUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
