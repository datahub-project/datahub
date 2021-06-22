package com.linkedin.metadata.resources.usage;

import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.usage.UsageService;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;
import com.linkedin.usage.UsageAggregation;
import com.linkedin.usage.UsageAggregationArray;
import com.linkedin.usage.UsageQueryResult;
import com.linkedin.usage.UsageQueryResultAggregations;
import com.linkedin.usage.UserUsageCounts;
import com.linkedin.usage.UserUsageCountsArray;
import com.linkedin.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Rest.li entry point: /usageStats
 */
@RestLiSimpleResource(name = "usageStats", namespace = "com.linkedin.usage")
public class UsageStats extends SimpleResourceTemplate<UsageAggregation> {
    private static final String ACTION_BATCH_INGEST = "batchIngest";
    private static final String PARAM_BUCKETS = "buckets";

    private static final String ACTION_QUERY = "query";
    private static final String PARAM_RESOURCE = "resource";
    private static final String PARAM_WINDOW = "duration";
    private static final String PARAM_START_TIME = "startTime";
    private static final String PARAM_END_TIME = "endTime";
    private static final String PARAM_MAX_BUCKETS = "maxBuckets";

    @Inject
    @Named("usageService")
    private UsageService _usageService;

    private final Logger _logger = LoggerFactory.getLogger(UsageStats.class.getName());

    @Action(name = ACTION_BATCH_INGEST)
    @Nonnull
    public Task<Void> batchIngest(@ActionParam(PARAM_BUCKETS) @Nonnull UsageAggregation[] buckets) {
        _logger.info("Ingesting {} usage stats aggregations", buckets.length);
        return RestliUtils.toTask(() -> {
            for (UsageAggregation agg: buckets) {
                this.ingest(agg);
            }
            return null;
        });
    }

    @Action(name = ACTION_QUERY)
    @Nonnull
    public Task<UsageQueryResult> query(@ActionParam(PARAM_RESOURCE) @Nonnull String resource,
                                        @ActionParam(PARAM_WINDOW) @Nonnull WindowDuration duration,
                                        @ActionParam(PARAM_START_TIME) @com.linkedin.restli.server.annotations.Optional Long startTime,
                                        @ActionParam(PARAM_END_TIME) @com.linkedin.restli.server.annotations.Optional Long endTime,
                                        @ActionParam(PARAM_MAX_BUCKETS) @com.linkedin.restli.server.annotations.Optional Integer maxBuckets) {
        _logger.info("Attempting to query usage stats");
        return RestliUtils.toTask(() -> {
            UsageAggregationArray buckets = new UsageAggregationArray();
            buckets.addAll(_usageService.query(resource, duration, startTime, endTime, maxBuckets));

            UsageQueryResultAggregations aggregations = new UsageQueryResultAggregations();
            // TODO: make the aggregation computation logic reusable

            // Compute aggregations for users and unique user count.
            {
                Map<Pair<Urn, String>, Integer> userAgg = new HashMap<>();
                buckets.forEach((bucket) -> {
                    Optional.ofNullable(bucket.getMetrics().getUsers()).ifPresent(usersUsageCounts -> {
                        usersUsageCounts.forEach((userCount -> {
                            Pair<Urn, String> key = new Pair<>(userCount.getUser(), userCount.getUserEmail());
                            int count = userAgg.getOrDefault(key, 0);
                            count += userCount.getCount();
                            userAgg.put(key, count);
                        }));
                    });
                });

                if (!userAgg.isEmpty()) {
                    UserUsageCountsArray users = new UserUsageCountsArray();
                    users.addAll(userAgg.entrySet().stream().map((mapping) -> new UserUsageCounts()
                            .setUser(mapping.getKey().getFirst())
                            .setUserEmail(mapping.getKey().getSecond())
                            .setCount(mapping.getValue())).collect(Collectors.toList()));
                    aggregations.setUsers(users);
                    aggregations.setUniqueUserCount(userAgg.size());
                }
            }

            // Compute aggregation for total query count.
            {
                Integer totalQueryCount = null;

                for (UsageAggregation bucket : buckets) {
                    if (bucket.getMetrics().getTotalSqlQueries() != null) {
                        if (totalQueryCount == null) {
                            totalQueryCount = 0;
                        }
                        totalQueryCount += bucket.getMetrics().getTotalSqlQueries();
                    }
                }

                if (totalQueryCount != null) {
                    aggregations.setTotalSqlQueries(totalQueryCount);
                }
            }

            return new UsageQueryResult()
                    .setBuckets(buckets)
                    .setAggregations(aggregations);
        });
    }


    private void ingest(@Nonnull UsageAggregation bucket) {
        // TODO attempt to resolve users into emails
        _usageService.upsertDocument(bucket);
    }

}
