package com.linkedin.datahub.graphql.resolvers.health;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.ActiveIncidentHealthDetails;
import com.linkedin.datahub.graphql.generated.AssertionHealthStatusByType;
import com.linkedin.datahub.graphql.generated.AssertionType;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.datahub.graphql.generated.HealthStatusType;
import com.linkedin.datahub.graphql.generated.IncidentState;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
public class AcrylEntityHealthResolver implements DataFetcher<CompletableFuture<List<Health>>> {
  private static final String INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME = "entities.keyword";
  private static final String INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME = "state";

  private final EntityClient _entityClient;
  private final Config _config;

  public AcrylEntityHealthResolver(@Nonnull final EntityClient entityClient) {
    this(entityClient, new Config(true, true));
  }

  public AcrylEntityHealthResolver(
      @Nonnull final EntityClient entityClient, @Nonnull final Config config) {
    _entityClient = entityClient;
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
              context.getOperationContext(),
              Constants.INCIDENT_ENTITY_NAME,
              filter,
              buildIncidentsSort(),
              0,
              1);
      final Integer activeIncidentCount = searchResult.getNumEntities();

      final Health.Builder healthBuilder = new Health.Builder().setType(HealthStatusType.INCIDENTS);
      if (activeIncidentCount > 0) {
        final Urn latestIncidentUrn = searchResult.getEntities().get(0).getEntity();
        final IncidentInfo latestIncidentInfo = getIncidentInfo(context, latestIncidentUrn);
        final Long latestIncidentTimestamp =
            latestIncidentInfo != null
                ? latestIncidentInfo.getStatus().getLastUpdated().getTime()
                : null;
        // There are active incidents.
        healthBuilder.setReportedAt(latestIncidentTimestamp != null ? latestIncidentTimestamp : 0);
        healthBuilder.setStatus(HealthStatus.FAIL);
        healthBuilder.setMessage(
            String.format(
                "%s active incident%s", activeIncidentCount, activeIncidentCount > 1 ? "s" : ""));
        healthBuilder.setCauses(ImmutableList.of("ACTIVE_INCIDENTS"));
        healthBuilder.setActiveIncidentHealthDetails(
            new ActiveIncidentHealthDetails(
                latestIncidentUrn.toString(),
                latestIncidentInfo != null ? latestIncidentInfo.getTitle() : null,
                latestIncidentTimestamp,
                activeIncidentCount));
        return healthBuilder.build();
      }
      // Report pass if there are no active incidents.
      healthBuilder.setStatus(HealthStatus.PASS);
      return healthBuilder.build();
    } catch (RemoteInvocationException e) {
      log.error("Failed to compute incident health status!", e);
      return null;
    } catch (URISyntaxException e) {
      log.error(
          String.format(
              "Failed to compute incident health status for entity %s! Invalid URN.", entityUrn),
          e);
      return null;
    }
  }

  @Nullable
  private IncidentInfo getIncidentInfo(final QueryContext context, final Urn incidentUrn)
      throws URISyntaxException, RemoteInvocationException {
    final EntityResponse entityResponse =
        _entityClient.getV2(
            context.getOperationContext(),
            Constants.INCIDENT_ENTITY_NAME,
            incidentUrn,
            Set.of(Constants.INCIDENT_INFO_ASPECT_NAME),
            false);

    if (entityResponse == null) {
      return null;
    }
    if (!entityResponse.getAspects().containsKey(Constants.INCIDENT_INFO_ASPECT_NAME)) {
      return null;
    }
    return new IncidentInfo(
        entityResponse.getAspects().get(Constants.INCIDENT_INFO_ASPECT_NAME).getValue().data());
  }

  private List<SortCriterion> buildIncidentsSort() {
    return List.of(new SortCriterion().setOrder(SortOrder.DESCENDING).setField("lastUpdated"));
  }

  private Filter buildIncidentsEntityFilter(final String entityUrn, final String state) {
    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME, entityUrn);
    criterionMap.put(INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME, state);
    return QueryUtils.newFilter(criterionMap);
  }

  /**
   * Returns the resolved "assertions health", which is currently a static function of whether the
   * most recent run of all asset assertions has succeeded, as record by the AssertionsSummary
   * aspect.
   *
   * @param entityUrn the entity to compute health for
   * @param context the query context
   * @return an instance of {@link Health} for the asset, null if one cannot be computed.
   */
  @Nullable
  private Health computeAssertionHealthForAsset(
      final String entityUrn, final QueryContext context) {

    // SaaS only! In Acryl-DataHub we use the AssertionsSummary aspect to determine whether the
    // entity is passing or failing
    // it's assertions. This is more efficient, as we can skip expensive calls to elasticsearch!
    // Once OSS has the AssertionsSummaryHook, we can port this to OSS as well.
    final AssertionsSummary summary = getAssertionsSummary(entityUrn, context);

    if (summary != null && !isEmptyAssertionsSummary(summary)) {

      final int failingAssertionCount =
          summary.hasFailingAssertionDetails() ? summary.getFailingAssertionDetails().size() : 0;
      final int erroringAssertionCount =
          summary.hasErroringAssertionDetails() ? summary.getErroringAssertionDetails().size() : 0;
      final int passingAssertionCount =
          summary.hasPassingAssertionDetails() ? summary.getPassingAssertionDetails().size() : 0;
      final int initializingAssertionCount =
          summary.hasInitializingAssertionDetails()
              ? summary.getInitializingAssertionDetails().size()
              : 0;
      final int totalAssertionCount =
          failingAssertionCount
              + erroringAssertionCount
              + passingAssertionCount
              + initializingAssertionCount;

      final List<String> failingAssertionUrns =
          summary.hasFailingAssertionDetails()
              ? summary.getFailingAssertionDetails().stream()
                  .map(details -> details.getUrn().toString())
                  .toList()
              : Collections.emptyList();
      final List<String> erroringAssertionUrns =
          summary.hasErroringAssertionDetails()
              ? summary.getErroringAssertionDetails().stream()
                  .map(details -> details.getUrn().toString())
                  .toList()
              : Collections.emptyList();

      final List<String> initializingAssertionUrns =
          summary.hasInitializingAssertionDetails()
              ? summary.getInitializingAssertionDetails().stream()
                  .map(details -> details.getUrn().toString())
                  .toList()
              : Collections.emptyList();

      // Finally compute & return the health.
      final Health health = new Health();
      health.setType(HealthStatusType.ASSERTIONS);
      if (failingAssertionUrns.size() > 0) {
        health.setStatus(HealthStatus.FAIL);
        health.setMessage(
            String.format(
                "%s of %s assertions are failing",
                failingAssertionUrns.size(), totalAssertionCount));
        health.setCauses(failingAssertionUrns);
        final Long latestFailingAssertionRun =
            Collections.max(
                summary.getFailingAssertionDetails().stream()
                    .map(AssertionSummaryDetails::getLastResultAt)
                    .toList());
        health.setReportedAt(latestFailingAssertionRun);
      } else if (erroringAssertionUrns.size() > 0) {
        health.setStatus(HealthStatus.WARN);
        health.setMessage(
            String.format(
                "%s of %s assertions are erroring",
                erroringAssertionUrns.size(), totalAssertionCount));
        health.setCauses(erroringAssertionUrns);
        final Long latestErroringAssertionRun =
            Collections.max(
                summary.getErroringAssertionDetails().stream()
                    .map(AssertionSummaryDetails::getLastResultAt)
                    .toList());
        health.setReportedAt(latestErroringAssertionRun);
      } else if (initializingAssertionUrns.size() > 0) {
        health.setStatus(HealthStatus.INIT);
        health.setMessage(
            String.format(
                "%s of %s assertions are initializing",
                initializingAssertionUrns.size(), totalAssertionCount));
        final Long latestInitializingAssertionRun =
            Collections.max(
                summary.getInitializingAssertionDetails().stream()
                    .map(AssertionSummaryDetails::getLastResultAt)
                    .toList());
        health.setReportedAt(latestInitializingAssertionRun);
      } else {
        health.setStatus(HealthStatus.PASS);
        health.setReportedAt(summary.getLastAssertionResultAt());
        health.setMessage("All assertions are passing");
      }

      // Compute statuses by assertion type
      health.setLatestAssertionStatusByType(getLatestAssertionStatusByType(summary));

      return health;
    }
    // No assertions passing or failing. Simply return null.
    return null;
  }

  private List<AssertionHealthStatusByType> getLatestAssertionStatusByType(
      AssertionsSummary summary) {
    final List<AssertionHealthStatusByType> latestAssertionStatusByType = new ArrayList<>();
    for (AssertionType assertionType : AssertionType.values()) {
      final AssertionHealthStatusByType assertionHealthStatusByType =
          new AssertionHealthStatusByType();
      assertionHealthStatusByType.setType(assertionType);

      // 1. Get the assertion details for each status
      final List<AssertionSummaryDetails> failingAssertionsForType =
          summary.getFailingAssertionDetails().stream()
              .filter(details -> details.getType().equals(assertionType.name()))
              .toList();
      final List<AssertionSummaryDetails> erroringAssertionsForType =
          summary.getErroringAssertionDetails().stream()
              .filter(details -> details.getType().equals(assertionType.name()))
              .toList();
      final List<AssertionSummaryDetails> passingAssertionsForType =
          summary.getPassingAssertionDetails().stream()
              .filter(details -> details.getType().equals(assertionType.name()))
              .toList();
      final List<AssertionSummaryDetails> initializingAssertionsForType =
          summary.hasInitializingAssertionDetails()
              ? summary.getInitializingAssertionDetails().stream()
                  .filter(details -> details.getType().equals(assertionType.name()))
                  .toList()
              : Collections.emptyList();

      // 2. Set the overall count of assertions for this type
      assertionHealthStatusByType.setTotal(
          failingAssertionsForType.size()
              + erroringAssertionsForType.size()
              + passingAssertionsForType.size()
              + initializingAssertionsForType.size());

      // 3. Set the status related fields
      if (failingAssertionsForType.size() > 0) {
        assertionHealthStatusByType.setStatus(HealthStatus.FAIL);
        assertionHealthStatusByType.setLastStatusResultAt(
            Collections.max(
                failingAssertionsForType.stream()
                    .map(AssertionSummaryDetails::getLastResultAt)
                    .toList()));
        assertionHealthStatusByType.setStatusCount(failingAssertionsForType.size());
      } else if (erroringAssertionsForType.size() > 0) {
        assertionHealthStatusByType.setStatus(HealthStatus.FAIL);
        assertionHealthStatusByType.setLastStatusResultAt(
            Collections.max(
                erroringAssertionsForType.stream()
                    .map(AssertionSummaryDetails::getLastResultAt)
                    .toList()));
        assertionHealthStatusByType.setStatusCount(erroringAssertionsForType.size());
      } else if (initializingAssertionsForType.size() > 0) {
        assertionHealthStatusByType.setStatus(HealthStatus.INIT);
        assertionHealthStatusByType.setLastStatusResultAt(
            Collections.max(
                initializingAssertionsForType.stream()
                    .map(AssertionSummaryDetails::getLastResultAt)
                    .toList()));
        assertionHealthStatusByType.setStatusCount(initializingAssertionsForType.size());
      } else if (passingAssertionsForType.size() > 0) {
        assertionHealthStatusByType.setStatus(HealthStatus.PASS);
        assertionHealthStatusByType.setLastStatusResultAt(
            Collections.max(
                passingAssertionsForType.stream()
                    .map(AssertionSummaryDetails::getLastResultAt)
                    .toList()));
        assertionHealthStatusByType.setStatusCount(passingAssertionsForType.size());
      } else {
        continue;
      }

      // 4. Add to the list of latest assertion statuses by type
      latestAssertionStatusByType.add(assertionHealthStatusByType);
      ;
    }
    return latestAssertionStatusByType;
  }

  @Nullable
  private AssertionsSummary getAssertionsSummary(
      @Nonnull final String urnStr, @Nonnull final QueryContext context) {
    try {
      final Urn urn = UrnUtils.getUrn(urnStr);
      final EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              urn.getEntityType(),
              urn,
              ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME));

      if (entityResponse != null
          && entityResponse.getAspects().containsKey(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)) {
        return new AssertionsSummary(
            entityResponse
                .getAspects()
                .get(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)
                .getValue()
                .data());
      }
      return null;
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to retrieve assertions summary for urn %s! Returning null assertions summary.",
              urnStr),
          e);
      return null;
    }
  }

  private boolean isEmptyAssertionsSummary(@Nonnull final AssertionsSummary summary) {
    return (!summary.hasPassingAssertionDetails() || summary.getPassingAssertionDetails().isEmpty())
        && (!summary.hasFailingAssertionDetails() || summary.getFailingAssertionDetails().isEmpty())
        && (!summary.hasErroringAssertionDetails()
            || summary.getErroringAssertionDetails().isEmpty())
        && (!summary.hasInitializingAssertionDetails()
            || summary.getInitializingAssertionDetails().isEmpty());
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
