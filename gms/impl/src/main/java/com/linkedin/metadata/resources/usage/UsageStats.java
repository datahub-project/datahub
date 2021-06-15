package com.linkedin.metadata.resources.usage;

import com.linkedin.metadata.search.SearchService;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.RoutingException;
import com.linkedin.restli.server.annotations.*;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;
import com.linkedin.usage.UsageAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;

/**
 * Rest.li entry point: /usageStats
 */
@RestLiSimpleResource(name = "usageStats", namespace = "com.linkedin.usage")
public class UsageStats extends SimpleResourceTemplate<UsageAggregation> {
    private static final String ACTION_BATCH_INGEST = "batchIngest";
    private static final String PARAM_BUCKETS = "buckets";

    @Inject
    @Named("uageService")
    private SearchService _searchService;

    private final Logger _logger = LoggerFactory.getLogger("UsageStats");

    @Action(name = ACTION_BATCH_INGEST)
    @Nonnull
    public Task<Void> batchIngest(@ActionParam(PARAM_BUCKETS) @Nonnull UsageAggregation[] buckets)
    {
        _logger.info("Got {} usage stats aggregations to ingest", buckets.length);
        throw new RoutingException("'batch_ingest' not implemented", 400);
    }

}
