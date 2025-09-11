package com.linkedin.datahub.graphql.resolvers.user;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.GetUserRecommendationsInput;
import com.linkedin.datahub.graphql.generated.GetUserRecommendationsResult;
import com.linkedin.datahub.graphql.generated.UserUsageSortField;
import com.linkedin.datahub.graphql.types.corpuser.mappers.CorpUserMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class GetUserRecommendationsResolver
    implements DataFetcher<CompletableFuture<GetUserRecommendationsResult>> {

  private static final Integer DEFAULT_LIMIT = 6;
  private static final UserUsageSortField DEFAULT_SORT_FIELD =
      UserUsageSortField.USAGE_TOTAL_PAST_30_DAYS;
  private static final String DEFAULT_QUERY = "";

  // OpenSearch field mappings (based on actual index mapping)
  private static final String USAGE_TOTAL_FIELD = "userUsageTotalPast30DaysFeature";
  private static final String USAGE_PERCENTILE_FIELD = "userUsagePercentilePast30DaysFeature";
  private static final String INVITATION_STATUS_FIELD = "invitationStatus";

  private final EntityClient _entityClient;

  public GetUserRecommendationsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<GetUserRecommendationsResult> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final GetUserRecommendationsInput input =
          bindArgument(environment.getArgument("input"), GetUserRecommendationsInput.class);

      final Integer limit = input.getLimit() == null ? DEFAULT_LIMIT : input.getLimit();
      final Integer start = input.getStart() == null ? 0 : input.getStart();
      final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
      final UserUsageSortField sortBy =
          input.getSortBy() == null ? DEFAULT_SORT_FIELD : input.getSortBy();
      final String platformFilter = input.getPlatformFilter();

      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              // Build filter to exclude invited users and optionally filter by platform
              final Filter filter = buildFilter(platformFilter);

              // Build sort criteria with fallback for better sorting reliability
              final List<SortCriterion> sortCriteria = buildSortCriteriaWithFallback(sortBy);

              // Use searchAcrossEntities for more robust search infrastructure
              final SearchResult gmsResult =
                  _entityClient.searchAcrossEntities(
                      context
                          .getOperationContext()
                          .withSearchFlags(flags -> flags.setFulltext(true)),
                      Collections.singletonList(CORP_USER_ENTITY_NAME),
                      query,
                      filter,
                      start,
                      limit,
                      sortCriteria,
                      Collections.emptyList(), // facets
                      null // predicateJson
                      );

              // Hydrate users with full entity data including usage features
              // Preserve the original search order by maintaining the ordered list of URNs
              final List<Urn> orderedUrns =
                  gmsResult.getEntities().stream()
                      .map(SearchEntity::getEntity)
                      .collect(Collectors.toList());

              final Map<Urn, EntityResponse> entities =
                  _entityClient.batchGetV2(
                      context.getOperationContext(),
                      CORP_USER_ENTITY_NAME,
                      new HashSet<>(orderedUrns),
                      null);

              // Build result preserving the original search sort order
              final GetUserRecommendationsResult result = new GetUserRecommendationsResult();
              result.setStart(gmsResult.getFrom());
              result.setCount(gmsResult.getPageSize());
              result.setTotal(gmsResult.getNumEntities());

              // Map entities in the same order as returned by the search
              final List<EntityResponse> orderedEntities =
                  orderedUrns.stream()
                      .map(entities::get)
                      .filter(entity -> entity != null)
                      .collect(Collectors.toList());
              result.setUsers(mapEntities(context, orderedEntities));

              return result;
            } catch (Exception e) {
              throw new RuntimeException("Failed to get user recommendations", e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private Filter buildFilter(@Nullable final String platformFilter) {
    final List<Criterion> criteria = new ArrayList<>();

    // Always exclude users with invitation status (any status - SENT, etc.)
    final Criterion excludeInvitedCriterion = new Criterion();
    excludeInvitedCriterion.setField(INVITATION_STATUS_FIELD);
    excludeInvitedCriterion.setCondition(Condition.IS_NULL);
    excludeInvitedCriterion.setValue("");
    criteria.add(excludeInvitedCriterion);

    // Require users to have usage data (greater than 0)
    final Criterion hasUsageDataCriterion = new Criterion();
    hasUsageDataCriterion.setField(USAGE_TOTAL_FIELD);
    hasUsageDataCriterion.setCondition(Condition.GREATER_THAN);
    hasUsageDataCriterion.setValue("0");
    criteria.add(hasUsageDataCriterion);

    // Only include inactive users (missing 'active' field means user hasn't been activated in
    // DataHub)
    final Criterion inactiveUserCriterion = new Criterion();
    inactiveUserCriterion.setField("active");
    inactiveUserCriterion.setCondition(Condition.IS_NULL);
    inactiveUserCriterion.setValue("");
    criteria.add(inactiveUserCriterion);

    // Optional platform filter using platformUsageTotal mapping
    if (platformFilter != null && !platformFilter.trim().isEmpty()) {
      final String platformUsageField = "platformUsageTotal." + platformFilter;
      final Criterion platformCriterion = new Criterion();
      platformCriterion.setField(platformUsageField);
      platformCriterion.setCondition(Condition.GREATER_THAN);
      platformCriterion.setValue("0");
      criteria.add(platformCriterion);
    }

    final CriterionArray criterionArray = new CriterionArray(criteria);
    final ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    final Filter filter = new Filter();
    final ConjunctiveCriterionArray conjunctiveCriterionArray =
        new ConjunctiveCriterionArray(ImmutableList.of(conjunctiveCriterion));
    filter.setOr(conjunctiveCriterionArray);

    return filter;
  }

  private List<SortCriterion> buildSortCriteriaWithFallback(final UserUsageSortField sortBy) {
    final List<SortCriterion> sortCriteria = new ArrayList<>();

    // Primary sort criterion based on requested field
    final SortCriterion primarySort = new SortCriterion();
    primarySort.setOrder(SortOrder.DESCENDING);

    switch (sortBy) {
      case USAGE_TOTAL_PAST_30_DAYS:
        primarySort.setField(USAGE_TOTAL_FIELD);
        break;
      case USAGE_PERCENTILE_PAST_30_DAYS:
        primarySort.setField(USAGE_PERCENTILE_FIELD);
        // Add fallback to usage total when percentile data is unavailable/sparse
        final SortCriterion fallbackSort = new SortCriterion();
        fallbackSort.setField(USAGE_TOTAL_FIELD);
        fallbackSort.setOrder(SortOrder.DESCENDING);
        sortCriteria.add(primarySort);
        sortCriteria.add(fallbackSort);
        return sortCriteria;
      default:
        primarySort.setField(USAGE_TOTAL_FIELD);
        break;
    }

    // For non-percentile sorts, add username as tie-breaker for consistency
    sortCriteria.add(primarySort);
    final SortCriterion tieBreaker = new SortCriterion();
    tieBreaker.setField("username");
    tieBreaker.setOrder(SortOrder.ASCENDING);
    sortCriteria.add(tieBreaker);

    return sortCriteria;
  }

  private static List<CorpUser> mapEntities(
      @Nullable QueryContext context, final Collection<EntityResponse> entities) {
    return entities.stream().map(e -> CorpUserMapper.map(context, e)).collect(Collectors.toList());
  }
}
