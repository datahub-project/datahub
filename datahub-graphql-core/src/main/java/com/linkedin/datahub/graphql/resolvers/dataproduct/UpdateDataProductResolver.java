package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.UpdateDataProductInput;
import com.linkedin.datahub.graphql.types.dataproduct.mappers.DataProductMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
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

  @Override
  public CompletableFuture<DataProduct> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateDataProductInput input =
        bindArgument(environment.getArgument("input"), UpdateDataProductInput.class);
    final Urn dataProductUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final Authentication authentication = context.getAuthentication();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!_dataProductService.verifyEntityExists(
              context.getOperationContext(), dataProductUrn)) {
            throw new IllegalArgumentException("The Data Product provided dos not exist");
          }

          Domains domains =
              _dataProductService.getDataProductDomains(
                  context.getOperationContext(), dataProductUrn);
          if (domains != null && domains.hasDomains() && domains.getDomains().size() > 0) {
            // get first domain since we only allow one domain right now
            Urn domainUrn = UrnUtils.getUrn(domains.getDomains().get(0).toString());
            if (!DataProductAuthorizationUtils.isAuthorizedToManageDataProducts(
                context, domainUrn)) {
              throw new AuthorizationException(
                  "Unauthorized to perform this action. Please contact your DataHub administrator.");
            }
          }

          try {
            final Urn urn =
                _dataProductService.updateDataProduct(
                    context.getOperationContext(),
                    dataProductUrn,
                    input.getName(),
                    input.getDescription());
            EntityResponse response =
                _dataProductService.getDataProductEntityResponse(
                    context.getOperationContext(), urn);
            if (response != null) {
              return DataProductMapper.map(context, response);
            }
            // should never happen
            log.error(String.format("Unable to find data product with urn %s", dataProductUrn));
            return null;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to update DataProduct with urn %s", dataProductUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
