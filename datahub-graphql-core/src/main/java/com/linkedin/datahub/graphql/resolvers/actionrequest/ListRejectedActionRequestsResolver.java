package com.linkedin.datahub.graphql.resolvers.actionrequest;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.generated.ListRejectedActionRequestsInput;
import com.linkedin.datahub.graphql.generated.ListRejectedActionRequestsResult;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;


/**
 * Resolver responsible for resolving the 'listRejectedActionRequests' Query.
 */
public class ListRejectedActionRequestsResolver
    implements DataFetcher<CompletableFuture<ListRejectedActionRequestsResult>> {
  private static final String LAST_MODIFIED_FIELD_NAME = "lastModified";

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  public ListRejectedActionRequestsResolver(final EntityClient entityClient, final EntityService entityService) {
    _entityClient = entityClient;
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<ListRejectedActionRequestsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final ListRejectedActionRequestsInput input =
        bindArgument(environment.getArgument("input"), ListRejectedActionRequestsInput.class);
    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final ActionRequestType type = input.getType() == null ? null : input.getType();
    final Long startTimestampMillis = input.getStartTimestampMillis() == null ? null : input.getStartTimestampMillis();
    final Long endTimestampMillis = input.getEndTimestampMillis() == null ? null : input.getEndTimestampMillis();

    return CompletableFuture.supplyAsync(() -> {
      try {

        final Filter filter = createFilter(type, startTimestampMillis, endTimestampMillis);

        final SortCriterion sortCriterion =
            new SortCriterion().setField(LAST_MODIFIED_FIELD_NAME).setOrder(SortOrder.DESCENDING);

        final SearchResult searchResult =
            _entityClient.filter(ACTION_REQUEST_ENTITY_NAME, filter, sortCriterion, start, count,
                context.getAuthentication());

        final Map<Urn, Entity> entities = _entityClient.batchGet(new HashSet<>(
                searchResult.getEntities().stream().map(result -> result.getEntity()).collect(Collectors.toList())),
            context.getAuthentication());

        final ListRejectedActionRequestsResult result = new ListRejectedActionRequestsResult();
        result.setStart(searchResult.getFrom());
        result.setCount(searchResult.getPageSize());
        result.setTotal(searchResult.getNumEntities());
        result.setRejectedActionRequests(
            ActionRequestUtils.mapRejectedActionRequests(entities.values(), _entityService, type));
        return result;
      } catch (Exception e) {
        throw new RuntimeException("Failed to list rejected action requests", e);
      }
    });
  }

  private Filter createFilter(final @Nullable ActionRequestType type, final @Nullable Long startTimestampMillis,
      final @Nullable Long endTimestampMillis) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();
    andCriterion.add(ActionRequestUtils.createResultCriterion(ActionRequestResult.REJECTED));
    if (type != null) {
      andCriterion.add(ActionRequestUtils.createTypeCriterion(type));
    }

    if (startTimestampMillis != null) {
      andCriterion.add(ActionRequestUtils.createStartTimestampCriterion(startTimestampMillis));
    }

    if (endTimestampMillis != null) {
      andCriterion.add(ActionRequestUtils.createEndTimestampCriterion(endTimestampMillis));
    }

    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    filter.setOr(disjunction);
    return filter;
  }
}
