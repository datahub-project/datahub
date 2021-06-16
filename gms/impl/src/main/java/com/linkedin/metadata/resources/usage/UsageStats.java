package com.linkedin.metadata.resources.usage;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.metrics.UsageService;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.RoutingException;
import com.linkedin.restli.server.annotations.*;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;
import com.linkedin.usage.UsageAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.*;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Rest.li entry point: /usageStats
 */
@RestLiSimpleResource(name = "usageStats", namespace = "com.linkedin.usage")
public class UsageStats extends SimpleResourceTemplate<UsageAggregation> {
    private static final String ACTION_BATCH_INGEST = "batchIngest";
    private static final String PARAM_BUCKETS = "buckets";

    @Inject
    @Named("usageService")
    private UsageService _usageService;

    private final Logger _logger = LoggerFactory.getLogger("UsageStats");

    @Action(name = ACTION_BATCH_INGEST)
    @Nonnull
    public Task<Void> batchIngest(@ActionParam(PARAM_BUCKETS) @Nonnull UsageAggregation[] buckets)
    {
        _logger.info("Ingesting {} usage stats aggregations", buckets.length);
        return RestliUtils.toTask(() -> {
            for (UsageAggregation agg: buckets) {
                ingest(agg);
            }
            return null;
        });
    }

    private void ingest(@Nonnull UsageAggregation bucket) {
        String id = String.format("(%d,%s,%s)", bucket.getBucket(), bucket.getDuration(), bucket.getResource());

        ObjectNode document = JsonNodeFactory.instance.objectNode();
        document.set("bucket", JsonNodeFactory.instance.numberNode(bucket.getBucket()));
        document.set("duration", JsonNodeFactory.instance.textNode(bucket.getDuration().toString()));
        document.set("bucket_end", JsonNodeFactory.instance.numberNode(bucket.getBucket() + windowDurationToMillis(bucket.getDuration())));
        document.set("resource", JsonNodeFactory.instance.textNode(bucket.getResource().toString()));

        Optional.ofNullable(bucket.getMetrics().getUsers()).ifPresent(usersUsageCounts -> {
            ArrayNode users = JsonNodeFactory.instance.arrayNode();
            // TODO attempt to resolve users into emails
            usersUsageCounts.forEach(userUsage -> {
                ObjectNode userDocument = JsonNodeFactory.instance.objectNode();
                userDocument.set("user", JsonNodeFactory.instance.textNode(userUsage.getUser().toString()));
                userDocument.set("count", JsonNodeFactory.instance.numberNode(userUsage.getCount()));
            });
            document.set("metrics.users", users);
        });

        // TODO: metrics here
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
