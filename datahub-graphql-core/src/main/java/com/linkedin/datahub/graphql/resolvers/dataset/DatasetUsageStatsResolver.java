package com.linkedin.datahub.graphql.resolvers.dataset;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.isViewDatasetUsageAuthorized;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.Constants;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.UsageQueryResult;
import com.linkedin.datahub.graphql.types.usage.UsageQueryResultMapper;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import com.linkedin.usage.UsageClient;
import com.linkedin.usage.UsageTimeRange;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatasetUsageStatsResolver implements DataFetcher<CompletableFuture<UsageQueryResult>> {

  /** Selection-set glob matching the topSqlQueries field anywhere under usageStats. */
  private static final String TOP_SQL_QUERIES_FIELD_GLOB = "**/topSqlQueries";

  private final UsageClient usageClient;

  public DatasetUsageStatsResolver(final UsageClient usageClient) {
    this.usageClient = usageClient;
  }

  @Override
  public CompletableFuture<UsageQueryResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn resourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());
    final UsageTimeRange range =
        UsageTimeRange.valueOf(environment.getArgument(Constants.RANGE_INPUT_FIELD));
    final Long startTimeMillis =
        environment.getArgumentOrDefault(Constants.START_TIME_MILLIS_INPUT_FIELD, null);
    final String timeZone = environment.getArgument(Constants.TIME_ZONE_INPUT_FIELD);
    final boolean requestsSqlQueries =
        environment.getSelectionSet() != null
            && environment.getSelectionSet().contains(TOP_SQL_QUERIES_FIELD_GLOB);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!isViewDatasetUsageAuthorized(context, resourceUrn)) {
            log.debug(
                "User {} is not authorized to view usage information for dataset {}",
                context.getActorUrn(),
                resourceUrn.toString());
            return null;
          }
          final boolean sqlQueriesRestricted = isTopSqlQueriesRestricted(context, resourceUrn);
          if (requestsSqlQueries && sqlQueriesRestricted) {
            throw new AuthorizationException(
                String.format(
                    "Unauthorized to view query SQL (topSqlQueries) for dataset %s. Requires the"
                        + " View Entity Queries privilege on the dataset, or the platform-level"
                        + " View All Queries privilege. Under strict subject enforcement this"
                        + " field is denied even with those privileges, since query SQL text"
                        + " carries no per-statement dataset record to verify against. Re-request"
                        + " without the topSqlQueries field to read usage statistics.",
                    resourceUrn));
          }
          try {
            com.linkedin.usage.UsageQueryResult usageQueryResult =
                usageClient.getUsageStats(
                    context.getOperationContext(),
                    resourceUrn.toString(),
                    range,
                    startTimeMillis,
                    timeZone);
            UsageQueryResult mapped = UsageQueryResultMapper.map(context, usageQueryResult);
            if (sqlQueriesRestricted) {
              // Safety net for selections the glob did not detect: never return the SQL.
              removeTopSqlQueries(mapped);
            }
            return mapped;
          } catch (Exception e) {
            log.error(String.format("Failed to load Usage Stats for resource %s", resourceUrn), e);
            context
                .getOperationContext()
                .getMetricUtils()
                .ifPresent(
                    metricUtils ->
                        metricUtils.increment(this.getClass(), "usage_stats_dropped", 1));
          }

          return UsageQueryResultMapper.EMPTY;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Usage statistics embed raw SQL statements (topSqlQueries), which are protected by {@code
   * VIEW_ENTITY_QUERIES} — a privilege distinct from {@code VIEW_DATASET_USAGE}, which gates the
   * numeric usage data. Selections including topSqlQueries are denied with an explicit
   * authorization error when the actor is not entitled to the SQL; selections without it are served
   * normally. The stored statements are plain strings with no recorded dataset associations, so
   * per-statement authorization is impossible here:
   *
   * <ul>
   *   <li>{@code VIEW_ALL_QUERIES} (see {@link
   *       com.linkedin.metadata.authorization.PoliciesConfig#VIEW_ALL_QUERIES_PRIVILEGE}):
   *       platform-level, unrestricted in every mode. Since it grants visibility into every query
   *       platform-wide regardless of subject datasets, per-statement dataset association is moot
   *       for an actor holding it — there is no "other dataset" whose SQL they aren't already
   *       entitled to see.
   *   <li>Default (any-subject) mode, without {@code VIEW_ALL_QUERIES}: SQL is permitted iff the
   *       actor holds {@code VIEW_ENTITY_QUERIES} on THIS dataset — consistent with the any-subject
   *       rule, since every statement in the list ran against this dataset.
   *   <li>Strict ({@code requireAllSubjects}) mode, without {@code VIEW_ALL_QUERIES}: SQL is ALWAYS
   *       restricted, even for actors holding {@code VIEW_ENTITY_QUERIES} on this dataset — a
   *       statement may reference other datasets the actor cannot see, and that cannot be verified
   *       for bare strings. Documented limitation of strict mode. Supporting topSqlQueries under
   *       strict mode would require the usage documents written to Elasticsearch ({@code
   *       DatasetUsageStatistics}) to carry something like a {@code
   *       topSqlQueries[].referencedDatasets} attribute alongside each statement (or a Query entity
   *       urn reference), so the privilege could be enforced per statement at read time; ingestion
   *       already computes those associations when parsing query logs, but today it stores only the
   *       bare strings.
   * </ul>
   *
   * <p>Delegates to {@link EntityAspectAuthorizationUtils#isTopSqlQueriesRestricted}, which is
   * governed by {@link EntityAspectAuthorizationUtils#isQueryViewAuthorizationEnabled} (dedicated
   * flag OR the legacy view-authorization master switch) rather than the dedicated flag alone, so
   * this resolver and Rest.li's {@code UsageStats} action resource — the other caller — can never
   * drift out of agreement on when enforcement is active the way this method's own inline copy of
   * the enabled-check once did.
   */
  private static boolean isTopSqlQueriesRestricted(
      final QueryContext context, final Urn resourceUrn) {
    return EntityAspectAuthorizationUtils.isTopSqlQueriesRestricted(
        context.getOperationContext(), resourceUrn);
  }

  private static void removeTopSqlQueries(final UsageQueryResult result) {
    if (result == null || result.getBuckets() == null) {
      return;
    }
    result
        .getBuckets()
        .forEach(
            bucket -> {
              if (bucket != null && bucket.getMetrics() != null) {
                bucket.getMetrics().setTopSqlQueries(null);
              }
            });
  }
}
