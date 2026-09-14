package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.MoveDataProductInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.dataproduct.DataProductAncestorUtils;
import com.linkedin.datahub.graphql.resolvers.dataproduct.DataProductAuthorizationUtils;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class MoveDataProductResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService<?> _entityService;
  private final EntityClient _entityClient;
  private final DataProductService _dataProductService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final MoveDataProductInput input =
        ResolverUtils.bindArgument(environment.getArgument("input"), MoveDataProductInput.class);
    final QueryContext context = ResolverUtils.getQueryContext(environment);
    final Urn resourceUrn = UrnUtils.getUrn(input.getResourceUrn());
    final Urn newParentUrn =
        input.getParentDataProduct() != null ? UrnUtils.getUrn(input.getParentDataProduct()) : null;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            if (!resourceUrn.getEntityType().equals(Constants.DATA_PRODUCT_ENTITY_NAME)) {
              throw new IllegalArgumentException("Resource is not a data product.");
            }

            final Domains domains =
                _dataProductService.getDataProductDomains(
                    context.getOperationContext(), resourceUrn);
            if (!DataProductAuthorizationUtils.isAuthorizedToManageDataProductsOnAnyDomain(
                context, domains)) {
              throw new AuthorizationException(
                  "Unauthorized to perform this action. Please contact your DataHub administrator.");
            }

            DataProductProperties properties =
                (DataProductProperties)
                    EntityUtils.getAspectFromEntity(
                        context.getOperationContext(),
                        resourceUrn.toString(),
                        Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
                        _entityService,
                        null);

            if (properties == null) {
              throw new IllegalArgumentException("Data product properties do not exist.");
            }

            if (newParentUrn != null) {
              if (!newParentUrn.getEntityType().equals(Constants.DATA_PRODUCT_ENTITY_NAME)) {
                throw new IllegalArgumentException("Parent entity is not a data product.");
              }
              if (!_entityService.exists(context.getOperationContext(), newParentUrn, true)) {
                throw new IllegalArgumentException("Parent entity does not exist.");
              }
              if (newParentUrn.equals(resourceUrn)
                  || DataProductAncestorUtils.walkParentChain(
                          context.getOperationContext(), _entityClient, newParentUrn)
                      .contains(resourceUrn)) {
                throw new DataHubGraphQLException(
                    "Cannot move a data product under one of its own descendants.",
                    DataHubGraphQLErrorCode.BAD_REQUEST);
              }
            }

            properties.setParentDataProduct(newParentUrn, SetMode.REMOVE_IF_NULL);
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            MutationUtils.persistAspect(
                context.getOperationContext(),
                resourceUrn,
                Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
                properties,
                actor,
                _entityService);
            return true;
          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            log.error(
                "Failed to move data product {} to parent {} : {}",
                input.getResourceUrn(),
                input.getParentDataProduct(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to move data product %s to %s",
                    input.getResourceUrn(), input.getParentDataProduct()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
