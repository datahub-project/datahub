package com.linkedin.metadata.resources.usage;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.WindowDuration;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;

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
            // TODO: compute the aggregations here

            return new UsageQueryResult()
                    .setBuckets(buckets)
                    .setAggregations(aggregations);
        });
    }


    private void ingest(@Nonnull UsageAggregation bucket) {
        String id = String.format("(%d,%s,%s)", bucket.getBucket(), bucket.getDuration(), bucket.getResource());

        // TODO move documentation generation logic into ElasticUsageService class
        ObjectNode document = JsonNodeFactory.instance.objectNode();
        document.set("bucket", JsonNodeFactory.instance.numberNode(bucket.getBucket()));
        document.set("duration", JsonNodeFactory.instance.textNode(bucket.getDuration().toString()));
        document.set("bucket_end", JsonNodeFactory.instance.numberNode(bucket.getBucket() + windowDurationToMillis(bucket.getDuration())));
        document.set("resource", JsonNodeFactory.instance.textNode(bucket.getResource().toString()));

        document.set("metrics.unique_user_count", JsonNodeFactory.instance.numberNode(bucket.getMetrics().getUniqueUserCount()));
        Optional.ofNullable(bucket.getMetrics().getUsers()).ifPresent(usersUsageCounts -> {
            ArrayNode users = JsonNodeFactory.instance.arrayNode();
            // TODO attempt to resolve users into emails
            usersUsageCounts.forEach(userUsage -> {
                ObjectNode userDocument = JsonNodeFactory.instance.objectNode();
                if (userUsage.getUser() != null) {
                    userDocument.set("user", JsonNodeFactory.instance.textNode(userUsage.getUser().toString()));
                }
                userDocument.set("user_email", JsonNodeFactory.instance.textNode(userUsage.getUserEmail()));
                userDocument.set("count", JsonNodeFactory.instance.numberNode(userUsage.getCount()));
                users.add(userDocument);
            });
            document.set("metrics.users", users);
        });

        document.set("metrics.total_sql_queries", JsonNodeFactory.instance.numberNode(bucket.getMetrics().getTotalSqlQueries()));
        Optional.ofNullable(bucket.getMetrics().getTopSqlQueries()).ifPresent(top_sql_queries -> {
            ArrayNode sqlQueriesDocument = JsonNodeFactory.instance.arrayNode();
            top_sql_queries.forEach(sqlQueriesDocument::add);
            document.set("metrics.top_sql_queries", sqlQueriesDocument);
        });

        _usageService.upsertDocument(document.toString(), id);
    }

    private static int windowDurationToMillis(@Nonnull WindowDuration duration) {
        if (duration == WindowDuration.DAY) {
            return 24 * 60 * 60 * 1000;
        } else if (duration == WindowDuration.HOUR) {
            return 60 * 60 * 1000;
        } else {
            throw new IllegalArgumentException("invalid WindowDuration enum state");
        }

    }

}
