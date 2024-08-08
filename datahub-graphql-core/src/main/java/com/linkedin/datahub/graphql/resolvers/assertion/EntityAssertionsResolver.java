package com.linkedin.datahub.graphql.resolvers.assertion;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityAssertionsResult;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** GraphQL Resolver used for fetching the list of Assertions associated with an Entity. */
@Slf4j
public class EntityAssertionsResolver
    implements DataFetcher<CompletableFuture<EntityAssertionsResult>> {

  private static final String ASSERTS_RELATIONSHIP_NAME = "Asserts";

  private final EntityClient _entityClient;
  private final GraphClient _graphClient;

  public EntityAssertionsResolver(final EntityClient entityClient, final GraphClient graphClient) {
    _entityClient = entityClient;
    _graphClient = graphClient;
  }

  @Override
  public CompletableFuture<EntityAssertionsResult> get(DataFetchingEnvironment environment) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();

          final String entityUrn = ((Entity) environment.getSource()).getUrn();
          final Integer start = environment.getArgumentOrDefault("start", 0);
          final Integer count = environment.getArgumentOrDefault("count", 200);
          final Boolean includeSoftDeleted =
              environment.getArgumentOrDefault("includeSoftDeleted", false);

          try {
            // Step 1: Fetch set of assertions associated with the target entity from the Graph
            // Store
            final EntityRelationships relationships =
                _graphClient.getRelatedEntities(
                    entityUrn,
                    ImmutableList.of(ASSERTS_RELATIONSHIP_NAME),
                    RelationshipDirection.INCOMING,
                    start,
                    count,
                    context.getActorUrn());

            final List<Urn> assertionUrns =
                relationships.getRelationships().stream()
                    .map(EntityRelationship::getEntity)
                    .collect(Collectors.toList());

            // Step 2: Hydrate the assertion entities based on the urns from step 1
            final Map<Urn, EntityResponse> entities =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.ASSERTION_ENTITY_NAME,
                    new HashSet<>(assertionUrns),
                    null);

            // Step 3: Map GMS assertion model to GraphQL model
            final List<EntityResponse> gmsResults = new ArrayList<>();
            for (Urn urn : assertionUrns) {
              gmsResults.add(entities.getOrDefault(urn, null));
            }
            final List<Assertion> assertions =
                gmsResults.stream()
                    .filter(Objects::nonNull)
                    .map(r -> AssertionMapper.map(context, r))
                    .filter(assertion -> assertionExists(assertion, includeSoftDeleted, context))
                    .collect(Collectors.toList());

            // Step 4: Package and return result
            final EntityAssertionsResult result = new EntityAssertionsResult();
            result.setCount(relationships.getCount());
            result.setStart(relationships.getStart());
            result.setTotal(relationships.getTotal());
            result.setAssertions(assertions);
            return result;
          } catch (URISyntaxException | RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve Assertion Run Events from GMS", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private boolean assertionExists(
      Assertion assertion, Boolean includeSoftDeleted, QueryContext context) {
    try {
      return _entityClient.exists(
          context.getOperationContext(), UrnUtils.getUrn(assertion.getUrn()), includeSoftDeleted);
    } catch (RemoteInvocationException e) {
      log.error(
          String.format("Unable to check if assertion %s exists, ignoring it", assertion.getUrn()),
          e);
      return false;
    }
  }
}
