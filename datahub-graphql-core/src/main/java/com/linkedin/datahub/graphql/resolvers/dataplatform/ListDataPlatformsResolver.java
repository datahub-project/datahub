package com.linkedin.datahub.graphql.resolvers.dataplatform;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListDomainsInput;
import com.linkedin.datahub.graphql.generated.ListDomainsResult;
import com.linkedin.datahub.graphql.generated.ListSecretsResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

public class ListDataPlatformsResolver implements DataFetcher<CompletableFuture<ListDomainsResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 100;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  public ListDataPlatformsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListDomainsResult> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(() -> {

      final ListDomainsInput input = bindArgument(environment.getArgument("input"), ListDomainsInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
      final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();

      try {
        final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(
            Constants.DATA_PLATFORM_ENTITY_NAME,
            new HashSet<>(gmsResult.getEntities().stream()
                .map(SearchEntity::getEntity)
                .collect(Collectors.toList())),
            ImmutableSet.of(Constants.SECRET_VALUE_ASPECT_NAME),
            context.getAuthentication());


        // Now that we have entities we can bind this to a result.
        final ListSecretsResult result = new ListSecretsResult();
        result.setStart(gmsResult.getFrom());
        result.setCount(gmsResult.getPageSize());
        result.setTotal(gmsResult.getNumEntities());
        result.setSecrets(mapEntities(entities.values()));
        return result;
      } catch (Exception e) {
        throw new RuntimeException("Failed to list domains", e);
      }
    });
  }

  // This method maps urns returned from the list endpoint into Partial Domain objects which will be resolved be a separate Batch resolver.
  private List<Domain> mapUnresolvedDomains(final List<Urn> entityUrns) {
    final List<Domain> results = new ArrayList<>();
    for (final Urn urn : entityUrns) {
      final Domain unresolvedDomain = new Domain();
      unresolvedDomain.setUrn(urn.toString());
      unresolvedDomain.setType(EntityType.DOMAIN);
      results.add(unresolvedDomain);
    }
    return results;
  }
}
