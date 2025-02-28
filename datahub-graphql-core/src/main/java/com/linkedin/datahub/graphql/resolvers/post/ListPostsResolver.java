package com.linkedin.datahub.graphql.resolvers.post;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListPostsInput;
import com.linkedin.datahub.graphql.generated.ListPostsResult;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.post.PostMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
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
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ListPostsResolver implements DataFetcher<CompletableFuture<ListPostsResult>> {
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<ListPostsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();

    final ListPostsInput input =
        bindArgument(environment.getArgument("input"), ListPostsInput.class);
    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
    final String maybeResourceUrn = input.getResourceUrn() == null ? null : input.getResourceUrn();
    final List<AndFilterInput> filters =
        input.getOrFilters() == null ? new ArrayList<>() : input.getOrFilters();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final List<SortCriterion> sortCriteria =
                Collections.singletonList(
                    new SortCriterion()
                        .setField(LAST_MODIFIED_FIELD_NAME)
                        .setOrder(SortOrder.DESCENDING));

            // First, get all Post Urns.
            final SearchResult gmsResult =
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    POST_ENTITY_NAME,
                    query,
                    buildFilters(maybeResourceUrn, filters),
                    sortCriteria,
                    start,
                    count);

            // Then, get and hydrate all Posts.
            final Map<Urn, EntityResponse> entities =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    POST_ENTITY_NAME,
                    new HashSet<>(
                        gmsResult.getEntities().stream()
                            .map(SearchEntity::getEntity)
                            .collect(Collectors.toList())),
                    null);

            final ListPostsResult result = new ListPostsResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setPosts(
                entities.values().stream()
                    .map(e -> PostMapper.map(context, e))
                    .collect(Collectors.toList()));
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list posts", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  @Nullable
  private Filter buildFilters(@Nullable String maybeResourceUrn, List<AndFilterInput> filters) {
    // Or between filters provided by the user and the maybeResourceUrn if present
    if (maybeResourceUrn != null) {
      filters.add(
          new AndFilterInput(
              List.of(
                  new FacetFilterInput(
                      "target",
                      null,
                      ImmutableList.of(maybeResourceUrn),
                      false,
                      FilterOperator.EQUAL))));
    }

    return ResolverUtils.buildFilter(null, filters);
  }
}
