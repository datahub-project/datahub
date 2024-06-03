package com.linkedin.datahub.graphql.resolvers.container;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.ParentContainersResult;
import com.linkedin.datahub.graphql.types.container.mappers.ContainerMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ParentContainersResolver
    implements DataFetcher<CompletableFuture<ParentContainersResult>> {

  private final EntityClient _entityClient;

  public ParentContainersResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  private void aggregateParentContainers(
      List<Container> containers, String urn, QueryContext context) {
    try {
      Urn entityUrn = new Urn(urn);
      EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(CONTAINER_ASPECT_NAME));

      if (entityResponse != null
          && entityResponse.getAspects().containsKey(CONTAINER_ASPECT_NAME)) {
        DataMap dataMap = entityResponse.getAspects().get(CONTAINER_ASPECT_NAME).getValue().data();
        com.linkedin.container.Container container = new com.linkedin.container.Container(dataMap);
        Urn containerUrn = container.getContainer();
        EntityResponse response =
            _entityClient.getV2(
                context.getOperationContext(), containerUrn.getEntityType(), containerUrn, null);
        if (response != null) {
          Container mappedContainer = ContainerMapper.map(context, response);
          containers.add(mappedContainer);
          aggregateParentContainers(containers, mappedContainer.getUrn(), context);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public CompletableFuture<ParentContainersResult> get(DataFetchingEnvironment environment) {

    final QueryContext context = environment.getContext();
    final String urn = ((Entity) environment.getSource()).getUrn();
    final List<Container> containers = new ArrayList<>();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            aggregateParentContainers(containers, urn, context);
            final ParentContainersResult result = new ParentContainersResult();

            List<Container> viewable = new ArrayList<>(containers);

            result.setCount(viewable.size());
            result.setContainers(viewable);
            return result;
          } catch (DataHubGraphQLException e) {
            throw new RuntimeException("Failed to load all containers", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
