package com.linkedin.datahub.graphql.resolvers.health;

import static com.linkedin.metadata.Constants.ASSERTION_RUN_EVENT_STATUS_COMPLETE;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.EntityRelationships;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.datahub.graphql.generated.HealthStatusType;
import com.linkedin.datahub.graphql.generated.IncidentState;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for generating the health badge for an asset, which depends on
 *
 * <p>1. Assertions status - whether the asset has active assertions 2. Incidents status - whether
 * the asset has active incidents
 */
@Slf4j
public class EntityHealthResolver implements DataFetcher<CompletableFuture<List<Health>>> {
  private static final String ASSERTS_RELATIONSHIP_NAME = "Asserts";
  private static final String ASSERTION_RUN_EVENT_SUCCESS_TYPE = "SUCCESS";
  private static final String INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME = "entities.keyword";
  private static final String INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME = "state";

  private final EntityClient _entityClient;
  private final GraphClient _graphClient;
  private final TimeseriesAspectService _timeseriesAspectService;

  private final Config _config;

  public EntityHealthResolver(
      @Nonnull final EntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final TimeseriesAspectService timeseriesAspectService) {
    this(entityClient, graphClient, timeseriesAspectService, new Config(true, true));
  }

  public EntityHealthResolver(
      @Nonnull final EntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final TimeseriesAspectService timeseriesAspectService,
      @Nonnull final Config config) {
    _entityClient = entityClient;
    _graphClient = graphClient;
    _timeseriesAspectService = timeseriesAspectService;
    _config = config;
  }

  @Override
  public CompletableFuture<List<Health>> get(final DataFetchingEnvironment environment)
      throws Exception {
    final Entity parent = environment.getSource();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final HealthStatuses statuses =
                computeHealthStatusForAsset(parent.getUrn(), environment.getContext());
            return statuses.healths;
          } catch (Exception e) {
            throw new RuntimeException("Failed to resolve asset's health status.", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Computes the "resolved health status" for an asset by
   *
   * <p>- fetching active (non-deleted) assertions - fetching latest assertion run for each -
   * checking whether any of the assertions latest runs are failing
   */
  private HealthStatuses computeHealthStatusForAsset(
      final String entityUrn, final QueryContext context) {
    final List<Health> healthStatuses = new ArrayList<>();

    if (_config.getIncidentsEnabled()) {
      final Health incidentsHealth = computeIncidentsHealthForAsset(entityUrn, context);
      if (incidentsHealth != null) {
        healthStatuses.add(incidentsHealth);
      }
    }

    if (_config.getAssertionsEnabled()) {
      final Health assertionsHealth = computeAssertionHealthForAsset(entityUrn, context);
      if (assertionsHealth != null) {
        healthStatuses.add(assertionsHealth);
      }
    }

    return new HealthStatuses(healthStatuses);
  }

  /**
   * Returns the resolved "incidents health", which is currently a static function of whether there
   * are any active incidents open on an asset
   *
   * @param entityUrn the asset to compute health for
   * @param context the query context
   * @return an instance of {@link Health} for the entity, null if one cannot be computed.
   */
  private Health computeIncidentsHealthForAsset(
      final String entityUrn, final QueryContext context) {
    try {
      final Filter filter = buildIncidentsEntityFilter(entityUrn, IncidentState.ACTIVE.toString());
      final SearchResult searchResult =
          _entityClient.filter(
              context.getOperationContext(), Constants.INCIDENT_ENTITY_NAME, filter, null, 0, 1);
      final Integer activeIncidentCount = searchResult.getNumEntities();
      if (activeIncidentCount > 0) {
        // There are active incidents.
        return new Health(
            HealthStatusType.INCIDENTS,
            HealthStatus.FAIL,
            String.format(
                "%s active incident%s", activeIncidentCount, activeIncidentCount > 1 ? "s" : ""),
            ImmutableList.of("ACTIVE_INCIDENTS"));
      }
      // Report pass if there are no active incidents.
      return new Health(HealthStatusType.INCIDENTS, HealthStatus.PASS, null, null);
    } catch (RemoteInvocationException e) {
      log.error("Failed to compute incident health status!", e);
      return null;
    }
  }

  private Filter buildIncidentsEntityFilter(final String entityUrn, final String state) {
    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME, entityUrn);
    criterionMap.put(INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME, state);
    return QueryUtils.newFilter(criterionMap);
  }

  /**
   * TODO: Replace this with the assertions summary aspect.
   *
   * <p>Returns the resolved "assertions health", which is currently a static function of whether
   * the most recent run of all asset assertions has succeeded.
   *
   * @param entityUrn the entity to compute health for
   * @param context the query context
   * @return an instance of {@link Health} for the asset, null if one cannot be computed.
   */
  @Nullable
  private Health computeAssertionHealthForAsset(
      final String entityUrn, final QueryContext context) {
    // Get active assertion urns
    final EntityRelationships relationships =
        _graphClient.getRelatedEntities(
            entityUrn,
            ImmutableList.of(ASSERTS_RELATIONSHIP_NAME),
            RelationshipDirection.INCOMING,
            0,
            500,
            context.getActorUrn());

    if (relationships.getTotal() > 0) {

      // If there are assertions defined, then we should return a non-null health for this asset.
      final Set<String> activeAssertionUrns =
          relationships.getRelationships().stream()
              .map(relationship -> relationship.getEntity().toString())
              .collect(Collectors.toSet());

      final GenericTable assertionRunResults =
          getAssertionRunsTable(context.getOperationContext(), entityUrn);

      if (!assertionRunResults.hasRows() || assertionRunResults.getRows().size() == 0) {
        // No assertion run results found. Return empty health!
        return null;
      }

      final List<String> failingAssertionUrns =
          getFailingAssertionUrns(assertionRunResults, activeAssertionUrns);

      // Finally compute & return the health.
      final Health health = new Health();
      health.setType(HealthStatusType.ASSERTIONS);
      if (failingAssertionUrns.size() > 0) {
        health.setStatus(HealthStatus.FAIL);
        health.setMessage(
            String.format(
                "%s of %s assertions are failing",
                failingAssertionUrns.size(), activeAssertionUrns.size()));
        health.setCauses(failingAssertionUrns);
      } else {
        health.setStatus(HealthStatus.PASS);
        health.setMessage("All assertions are passing");
      }
      return health;
    }
    return null;
  }

  private GenericTable getAssertionRunsTable(
      @Nonnull OperationContext opContext, final String asserteeUrn) {
    return _timeseriesAspectService.getAggregatedStats(
        opContext,
        Constants.ASSERTION_ENTITY_NAME,
        Constants.ASSERTION_RUN_EVENT_ASPECT_NAME,
        createAssertionAggregationSpecs(),
        createAssertionsFilter(asserteeUrn),
        createAssertionGroupingBuckets());
  }

  private List<String> getFailingAssertionUrns(
      final GenericTable assertionRunsResult, final Set<String> candidateAssertionUrns) {
    // Create the buckets based on the result
    return resultToFailedAssertionUrns(assertionRunsResult.getRows(), candidateAssertionUrns);
  }

  private Filter createAssertionsFilter(final String datasetUrn) {
    final Filter filter = new Filter();
    final ArrayList<Criterion> criteria = new ArrayList<>();

    // Add filter for asserteeUrn == datasetUrn
    Criterion datasetUrnCriterion = buildCriterion("asserteeUrn", Condition.EQUAL, datasetUrn);
    criteria.add(datasetUrnCriterion);

    // Add filter for result == result
    Criterion startTimeCriterion =
        buildCriterion("status", Condition.EQUAL, ASSERTION_RUN_EVENT_STATUS_COMPLETE);
    criteria.add(startTimeCriterion);

    filter.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria)))));
    return filter;
  }

  private AggregationSpec[] createAssertionAggregationSpecs() {
    // Simply fetch the timestamp, result type for the assertion URN.
    AggregationSpec resultTypeAggregation =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("type");
    AggregationSpec timestampAggregation =
        new AggregationSpec()
            .setAggregationType(AggregationType.LATEST)
            .setFieldPath("timestampMillis");
    return new AggregationSpec[] {resultTypeAggregation, timestampAggregation};
  }

  private GroupingBucket[] createAssertionGroupingBuckets() {
    // String grouping bucket on "assertionUrn"
    GroupingBucket assertionUrnBucket = new GroupingBucket();
    assertionUrnBucket.setKey("assertionUrn").setType(GroupingBucketType.STRING_GROUPING_BUCKET);
    return new GroupingBucket[] {assertionUrnBucket};
  }

  private List<String> resultToFailedAssertionUrns(
      final StringArrayArray rows, final Set<String> activeAssertionUrns) {
    final List<String> failedAssertionUrns = new ArrayList<>();
    for (StringArray row : rows) {
      // Result structure should be assertionUrn, event.result.type, timestampMillis
      if (row.size() != 3) {
        throw new RuntimeException(
            String.format(
                "Failed to fetch assertion run events from Timeseries index! Expected row of size 3, found %s",
                row.size()));
      }

      final String assertionUrn = row.get(0);
      final String resultType = row.get(1);

      // If assertion is "active" (not deleted) & is failing, then we report a degradation in
      // health.
      if (activeAssertionUrns.contains(assertionUrn)
          && !ASSERTION_RUN_EVENT_SUCCESS_TYPE.equals(resultType)) {
        failedAssertionUrns.add(assertionUrn);
      }
    }
    return failedAssertionUrns;
  }

  @Data
  @AllArgsConstructor
  public static class Config {
    private Boolean assertionsEnabled;
    private Boolean incidentsEnabled;
  }

  @AllArgsConstructor
  private static class HealthStatuses {
    private final List<Health> healths;
  }
}
