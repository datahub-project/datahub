package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canViewRelationship;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Walks the parentDataProduct chain from the source data product up to the root, returning all
 * ancestors in nearest-first order.
 */
@Slf4j
@RequiredArgsConstructor
public class ParentDataProductsResolver
    implements DataFetcher<CompletableFuture<List<DataProduct>>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<List<DataProduct>> get(final DataFetchingEnvironment environment) {
    final QueryContext context = getQueryContext(environment);
    final Urn sourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final List<Urn> parentUrns =
                DataProductAncestorUtils.walkParentChain(
                        context.getOperationContext(), _entityClient, sourceUrn)
                    .stream()
                    .filter(
                        parentUrn ->
                            canViewRelationship(
                                context.getOperationContext(), parentUrn, sourceUrn))
                    .collect(Collectors.toList());

            final List<DataProduct> result = new ArrayList<>(parentUrns.size());
            if (!parentUrns.isEmpty()) {
              final Map<Urn, EntityResponse> responses =
                  _entityClient.batchGetV2(
                      context.getOperationContext(),
                      Constants.DATA_PRODUCT_ENTITY_NAME,
                      new HashSet<>(parentUrns),
                      null,
                      false);

              for (Urn parentUrn : parentUrns) {
                final EntityResponse response = responses.get(parentUrn);
                final DataProduct stub = new DataProduct();
                stub.setUrn(parentUrn.toString());
                stub.setType(EntityType.DATA_PRODUCT);
                if (response != null) {
                  final EnvelopedAspect propsAspect =
                      response.getAspects().get(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
                  if (propsAspect != null) {
                    final DataProductProperties props =
                        new DataProductProperties(propsAspect.getValue().data());
                    final com.linkedin.datahub.graphql.generated.DataProductProperties gqlProps =
                        new com.linkedin.datahub.graphql.generated.DataProductProperties();
                    gqlProps.setName(props.hasName() ? props.getName() : parentUrn.getId());
                    if (props.hasParentDataProduct() && props.getParentDataProduct() != null) {
                      final DataProduct grandparentStub = new DataProduct();
                      grandparentStub.setUrn(props.getParentDataProduct().toString());
                      grandparentStub.setType(EntityType.DATA_PRODUCT);
                      gqlProps.setParentDataProduct(grandparentStub);
                    }
                    stub.setProperties(gqlProps);
                  }
                }
                result.add(stub);
              }
            }
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to load parent data products for " + sourceUrn, e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
