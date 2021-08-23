package com.linkedin.metadata.resources.usage;

import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.usage.UsageService;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;
import com.linkedin.usage.FieldUsageCounts;
import com.linkedin.usage.FieldUsageCountsArray;
import com.linkedin.usage.UsageAggregation;
import com.linkedin.usage.UsageAggregationArray;
import com.linkedin.usage.UsageQueryResult;
import com.linkedin.usage.UsageQueryResultAggregations;
import com.linkedin.usage.UsageTimeRange;
import com.linkedin.usage.UserUsageCounts;
import com.linkedin.usage.UserUsageCountsArray;
import com.linkedin.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import java.time.Instant;
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
    private static final String PARAM_DURATION = "duration";
    private static final String PARAM_START_TIME = "startTime";
    private static final String PARAM_END_TIME = "endTime";
    private static final String PARAM_MAX_BUCKETS = "maxBuckets";

    private static final String ACTION_QUERY_RANGE = "queryRange";
    private static final String PARAM_RANGE = "rangeFromEnd";

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
                                        @ActionParam(PARAM_DURATION) @Nonnull WindowDuration duration,
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
                            .setUser(mapping.getKey().getFirst(), SetMode.REMOVE_IF_NULL)
                            .setUserEmail(mapping.getKey().getSecond(), SetMode.REMOVE_IF_NULL)
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

            // Compute aggregations for field usage counts.
            {
                Map<String, Integer> fieldAgg = new HashMap<>();
                buckets.forEach((bucket) -> {
                    Optional.ofNullable(bucket.getMetrics().getFields()).ifPresent(fieldUsageCounts -> {
                        fieldUsageCounts.forEach((fieldCount -> {
                            String key = fieldCount.getFieldName();
                            int count = fieldAgg.getOrDefault(key, 0);
                            count += fieldCount.getCount();
                            fieldAgg.put(key, count);
                        }));
                    });
                });

                if (!fieldAgg.isEmpty()) {
                    FieldUsageCountsArray fields = new FieldUsageCountsArray();
                    fields.addAll(fieldAgg.entrySet().stream().map((mapping) -> new FieldUsageCounts()
                            .setFieldName(mapping.getKey())
                            .setCount(mapping.getValue())).collect(Collectors.toList()));
                    aggregations.setFields(fields);
                }
            }

            return new UsageQueryResult()
                    .setBuckets(buckets)
                    .setAggregations(aggregations);
        });
    }

    @Action(name = ACTION_QUERY_RANGE)
    @Nonnull
    public Task<UsageQueryResult> queryRange(@ActionParam(PARAM_RESOURCE) @Nonnull String resource,
                                        @ActionParam(PARAM_DURATION) @Nonnull WindowDuration duration,
                                        @ActionParam(PARAM_RANGE) UsageTimeRange range) {
        final long now = Instant.now().toEpochMilli();
        return this.query(resource, duration, convertRangeToStartTime(range, now), now, null);
    }

    private void ingest(@Nonnull UsageAggregation bucket) {
        // TODO attempt to resolve users into emails
        _usageService.upsertDocument(bucket);
    }

    @Nonnull
    Long convertRangeToStartTime(@Nonnull UsageTimeRange range, long currentEpochMillis) {
        // TRICKY: since start_time must be before the bucket's start, we actually
        // need to subtract extra from the current time to ensure that we get precisely
        // what we're looking for. Note that start_time and end_time are both inclusive,
        // so we must also do an off-by-one adjustment.
        final long oneHourMillis = 60 * 60 * 1000;
        final long oneDayMillis = 24 * oneHourMillis;

        if (range == UsageTimeRange.HOUR) {
            return currentEpochMillis - (2 * oneHourMillis + 1);
        } else if (range == UsageTimeRange.DAY) {
            return currentEpochMillis - (2 * oneDayMillis + 1);
        } else if (range == UsageTimeRange.WEEK) {
            return currentEpochMillis - (8 * oneDayMillis + 1);
        } else if (range == UsageTimeRange.MONTH) {
            // Assuming month is last 30 days.
            return currentEpochMillis - (31 * oneDayMillis + 1);
        } else if (range == UsageTimeRange.QUARTER) {
            // Assuming a quarter is 91 days.
            return currentEpochMillis - (92 * oneDayMillis + 1);
        } else if (range == UsageTimeRange.YEAR) {
            return currentEpochMillis - (366 * oneDayMillis + 1);
        } else if (range == UsageTimeRange.ALL) {
            return 0L;
        } else {
            throw new IllegalArgumentException("invalid UsageTimeRange enum state: " + range.name());
        }
    }

}
