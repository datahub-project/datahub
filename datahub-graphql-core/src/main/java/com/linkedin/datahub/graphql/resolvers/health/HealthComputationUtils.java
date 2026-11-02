package com.linkedin.datahub.graphql.resolvers.health;

import static com.linkedin.metadata.Constants.ASSERTION_RUN_EVENT_STATUS_COMPLETE;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.generated.ActiveIncidentHealthDetails;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.datahub.graphql.generated.HealthStatusType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.test.TestResults;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Pure health-computation helpers shared by {@link EntityHealthResolver} (per-entity path) and
 * {@link com.linkedin.datahub.graphql.resolvers.load.EntityHealthBatchLoader} (batched path).
 *
 * <p>Both paths MUST produce identical health for the same inputs; keeping the aggregation specs,
 * filters, row parsing, and {@link Health} assembly in one place is what guarantees that — the two
 * callers differ only in how they fetch the raw data (one URN at a time vs. batched), not in how
 * they interpret it.
 */
public final class HealthComputationUtils {

  public static final String ASSERTS_RELATIONSHIP_NAME = "Asserts";
  public static final String ASSERTEE_URN_FIELD = "asserteeUrn";
  public static final int MAX_ACTIVE_ASSERTIONS = 500;

  private static final String ASSERTION_RUN_EVENT_SUCCESS_TYPE = "SUCCESS";
  private static final String INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME = "entities.keyword";
  private static final String INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME = "state";

  private HealthComputationUtils() {}

  // ---- Assertions -----------------------------------------------------------

  public static AggregationSpec[] assertionAggregationSpecs() {
    // Fetch the latest result type + timestamp per assertion URN.
    final AggregationSpec resultTypeAggregation =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("type");
    final AggregationSpec timestampAggregation =
        new AggregationSpec()
            .setAggregationType(AggregationType.LATEST)
            .setFieldPath("timestampMillis");
    return new AggregationSpec[] {resultTypeAggregation, timestampAggregation};
  }

  public static GroupingBucket[] assertionGroupingBuckets() {
    final GroupingBucket assertionUrnBucket = new GroupingBucket();
    assertionUrnBucket.setKey("assertionUrn").setType(GroupingBucketType.STRING_GROUPING_BUCKET);
    return new GroupingBucket[] {assertionUrnBucket};
  }

  /** Per-URN filter (single-entity path): {@code asserteeUrn == urn AND status == COMPLETE}. */
  public static Filter assertionsFilter(final String asserteeUrn) {
    final List<Criterion> criteria = new ArrayList<>();
    criteria.add(buildCriterion(ASSERTEE_URN_FIELD, Condition.EQUAL, asserteeUrn));
    criteria.add(buildCriterion("status", Condition.EQUAL, ASSERTION_RUN_EVENT_STATUS_COMPLETE));
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria)))));
  }

  /**
   * Shared filter (batch path): {@code status == COMPLETE} only. The per-URN {@code asserteeUrn}
   * criterion is intentionally omitted — {@code batchGetAggregatedStats} appends {@code asserteeUrn
   * IN [urns]} itself as the outer terms bucket.
   */
  public static Filter sharedAssertionsFilter() {
    final Criterion statusCriterion =
        buildCriterion("status", Condition.EQUAL, ASSERTION_RUN_EVENT_STATUS_COMPLETE);
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                ImmutableList.of(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(statusCriterion)))));
  }

  /**
   * Assembles the assertions {@link Health} from an assertion-run aggregation table and the set of
   * currently-active (non-deleted) assertion URNs. Returns null when there are no active assertions
   * or no run results to evaluate.
   */
  @Nullable
  public static Health buildAssertionsHealth(
      @Nullable final GenericTable assertionRunResults, final Set<String> activeAssertionUrns) {
    if (activeAssertionUrns.isEmpty()) {
      return null;
    }
    if (assertionRunResults == null
        || !assertionRunResults.hasRows()
        || assertionRunResults.getRows().isEmpty()) {
      return null;
    }

    final List<String> failingAssertionUrns =
        resultToFailedAssertionUrns(assertionRunResults.getRows(), activeAssertionUrns);

    final Health health = new Health();
    health.setType(HealthStatusType.ASSERTIONS);
    if (!failingAssertionUrns.isEmpty()) {
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

  private static List<String> resultToFailedAssertionUrns(
      final StringArrayArray rows, final Set<String> activeAssertionUrns) {
    final List<String> failedAssertionUrns = new ArrayList<>();
    for (StringArray row : rows) {
      // Row structure: assertionUrn, event.result.type, timestampMillis
      if (row.size() != 3) {
        throw new RuntimeException(
            String.format(
                "Failed to fetch assertion run events from Timeseries index! Expected row of size 3, found %s",
                row.size()));
      }
      final String assertionUrn = row.get(0);
      final String resultType = row.get(1);
      if (activeAssertionUrns.contains(assertionUrn)
          && !ASSERTION_RUN_EVENT_SUCCESS_TYPE.equals(resultType)) {
        failedAssertionUrns.add(assertionUrn);
      }
    }
    return failedAssertionUrns;
  }

  // ---- Tests ----------------------------------------------------------------

  @Nullable
  public static TestResults extractTestResults(@Nullable final EntityResponse entityResponse) {
    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(Constants.TEST_RESULTS_ASPECT_NAME)) {
      return null;
    }
    return new TestResults(
        entityResponse.getAspects().get(Constants.TEST_RESULTS_ASPECT_NAME).getValue().data());
  }

  @Nullable
  public static Health buildTestsHealth(@Nullable final TestResults testResults) {
    if (testResults == null) {
      return null;
    }
    final int passingCount = testResults.getPassing().size();
    final int failingCount = testResults.getFailing().size();
    final int totalCount = passingCount + failingCount;
    if (totalCount == 0) {
      return null;
    }
    final Health health = new Health();
    health.setType(HealthStatusType.TESTS);
    if (failingCount > 0) {
      health.setStatus(HealthStatus.FAIL);
      health.setMessage(String.format("%s of %s tests failing", failingCount, totalCount));
    } else {
      health.setStatus(HealthStatus.PASS);
      health.setMessage("All tests are passing");
    }
    return health;
  }

  // ---- Incidents ------------------------------------------------------------

  public static List<SortCriterion> incidentsSort() {
    return List.of(new SortCriterion().setOrder(SortOrder.DESCENDING).setField("lastUpdated"));
  }

  public static Filter incidentsEntityFilter(final String entityUrn, final String state) {
    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME, entityUrn);
    criterionMap.put(INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME, state);
    return QueryUtils.newFilter(criterionMap);
  }

  /**
   * Assembles the incidents {@link Health} from an active-incident count and (when {@code count >
   * 0}) the latest incident's URN and info. {@code count == 0} yields a PASS health.
   */
  public static Health buildIncidentHealth(
      final int activeIncidentCount,
      @Nullable final Urn latestIncidentUrn,
      @Nullable final IncidentInfo latestIncidentInfo) {
    if (activeIncidentCount <= 0) {
      return new Health(
          HealthStatusType.INCIDENTS, null, HealthStatus.PASS, null, null, null, null);
    }
    final Long latestIncidentTimestamp =
        latestIncidentInfo != null
            ? latestIncidentInfo.getStatus().getLastUpdated().getTime()
            : null;
    return new Health(
        HealthStatusType.INCIDENTS,
        latestIncidentTimestamp != null ? latestIncidentTimestamp : 0,
        HealthStatus.FAIL,
        String.format(
            "%s active incident%s", activeIncidentCount, activeIncidentCount > 1 ? "s" : ""),
        new ActiveIncidentHealthDetails(
            latestIncidentUrn != null ? latestIncidentUrn.toString() : null,
            latestIncidentInfo != null ? latestIncidentInfo.getTitle() : null,
            latestIncidentTimestamp,
            activeIncidentCount),
        null,
        ImmutableList.of("ACTIVE_INCIDENTS"));
  }
}
