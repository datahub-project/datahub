package com.linkedin.datahub.graphql.resolvers.group;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListGroupsInput;
import com.linkedin.datahub.graphql.generated.ListGroupsResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
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

public class ListGroupsResolver implements DataFetcher<CompletableFuture<ListGroupsResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  public ListGroupsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListGroupsResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final ListGroupsInput input =
          bindArgument(environment.getArgument("input"), ListGroupsInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
      final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();

      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              // First, get all group Urns.
              final SearchResult gmsResult =
                  _entityClient.search(
                      context
                          .getOperationContext()
                          .withSearchFlags(flags -> flags.setFulltext(true)),
                      CORP_GROUP_ENTITY_NAME,
                      query,
                      null,
                      Collections.singletonList(
                          new SortCriterion()
                              .setField(CORP_GROUP_CREATED_TIME_INDEX_FIELD_NAME)
                              .setOrder(SortOrder.DESCENDING)),
                      start,
                      count);

              // Then, get hydrate all groups.
              final Map<Urn, EntityResponse> entities =
                  _entityClient.batchGetV2(
                      context.getOperationContext(),
                      CORP_GROUP_ENTITY_NAME,
                      new HashSet<>(
                          gmsResult.getEntities().stream()
                              .map(SearchEntity::getEntity)
                              .collect(Collectors.toList())),
                      null);

              // Now that we have entities we can bind this to a result.
              final ListGroupsResult result = new ListGroupsResult();
              result.setStart(gmsResult.getFrom());
              result.setCount(gmsResult.getPageSize());
              result.setTotal(gmsResult.getNumEntities());
              result.setGroups(
                  mapUnresolvedGroups(
                      gmsResult.getEntities().stream()
                          .map(SearchEntity::getEntity)
                          .collect(Collectors.toList())));
              return result;
            } catch (Exception e) {
              throw new RuntimeException("Failed to list groups", e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  // This method maps urns returned from the list endpoint into Partial Group objects which will be
  // resolved be a separate Batch resolver.
  private List<CorpGroup> mapUnresolvedGroups(final List<Urn> entityUrns) {
    final List<CorpGroup> results = new ArrayList<>();
    for (final Urn urn : entityUrns) {
      final CorpGroup unresolvedGroup = new CorpGroup();
      unresolvedGroup.setUrn(urn.toString());
      unresolvedGroup.setType(EntityType.CORP_GROUP);
      results.add(unresolvedGroup);
    }
    return results;
  }
}
