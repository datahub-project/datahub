package com.linkedin.datahub.graphql.resolvers.post;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListPostsInput;
import com.linkedin.datahub.graphql.generated.ListPostsResult;
import com.linkedin.datahub.graphql.types.post.PostMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
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

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final SortCriterion sortCriterion =
                new SortCriterion()
                    .setField(LAST_MODIFIED_FIELD_NAME)
                    .setOrder(SortOrder.DESCENDING);

            // First, get all Post Urns.
            final SearchResult gmsResult =
                _entityClient.search(
                    POST_ENTITY_NAME,
                    query,
                    null,
                    sortCriterion,
                    start,
                    count,
                    context.getAuthentication(),
                    new SearchFlags().setFulltext(true));

            // Then, get and hydrate all Posts.
            final Map<Urn, EntityResponse> entities =
                _entityClient.batchGetV2(
                    POST_ENTITY_NAME,
                    new HashSet<>(
                        gmsResult.getEntities().stream()
                            .map(SearchEntity::getEntity)
                            .collect(Collectors.toList())),
                    null,
                    authentication);

            final ListPostsResult result = new ListPostsResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setPosts(
                entities.values().stream().map(PostMapper::map).collect(Collectors.toList()));
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list posts", e);
          }
        });
  }
}
