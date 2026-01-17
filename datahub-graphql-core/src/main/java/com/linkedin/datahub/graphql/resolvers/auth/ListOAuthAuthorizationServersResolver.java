package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ListOAuthAuthorizationServersInput;
import com.linkedin.datahub.graphql.generated.ListOAuthAuthorizationServersResult;
import com.linkedin.datahub.graphql.generated.OAuthAuthorizationServer;
import com.linkedin.datahub.graphql.resolvers.connection.ConnectionUtils;
import com.linkedin.datahub.graphql.types.auth.mappers.OAuthAuthorizationServerMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Lists all OAuth Authorization Server entities present within DataHub. */
@Slf4j
public class ListOAuthAuthorizationServersResolver
    implements DataFetcher<CompletableFuture<ListOAuthAuthorizationServersResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private static final Set<String> ASPECTS_TO_FETCH =
      Set.of(
          Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
          Constants.OWNERSHIP_ASPECT_NAME,
          Constants.STATUS_ASPECT_NAME);

  private final EntityClient entityClient;

  public ListOAuthAuthorizationServersResolver(@Nonnull final EntityClient entityClient) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<ListOAuthAuthorizationServersResult> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final ListOAuthAuthorizationServersInput input =
        bindArgument(environment.getArgument("input"), ListOAuthAuthorizationServersInput.class);

    // Reuse MANAGE_CONNECTIONS privilege since OAuth auth servers are external connections
    if (!ConnectionUtils.canManageConnections(context)) {
      throw new AuthorizationException(
          "Unauthorized to list OAuth authorization servers. "
              + "Please contact your DataHub administrator.");
    }

    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // Search for authorization servers
            final SearchResult searchResult =
                entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME,
                    query,
                    null,
                    ImmutableList.of(
                        new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)),
                    start,
                    count);

            // Batch get all entities
            final Map<Urn, EntityResponse> entities =
                entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME,
                    new HashSet<>(
                        searchResult.getEntities().stream()
                            .map(SearchEntity::getEntity)
                            .collect(Collectors.toList())),
                    ASPECTS_TO_FETCH);

            // Build result
            final ListOAuthAuthorizationServersResult result =
                new ListOAuthAuthorizationServersResult();
            result.setStart(searchResult.getFrom());
            result.setCount(searchResult.getPageSize());
            result.setTotal(searchResult.getNumEntities());
            result.setAuthorizationServers(mapEntities(context, searchResult, entities));
            return result;

          } catch (Exception e) {
            throw new RuntimeException("Failed to list OAuth authorization servers", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private List<OAuthAuthorizationServer> mapEntities(
      final QueryContext context,
      final SearchResult searchResult,
      final Map<Urn, EntityResponse> entities) {
    final List<OAuthAuthorizationServer> results = new ArrayList<>();
    for (SearchEntity searchEntity : searchResult.getEntities()) {
      final EntityResponse response = entities.get(searchEntity.getEntity());
      if (response != null) {
        final OAuthAuthorizationServer server =
            OAuthAuthorizationServerMapper.map(context, response);
        if (server != null) {
          results.add(server);
        }
      }
    }
    return results;
  }
}
