package com.linkedin.metadata.resources.usage;

import com.linkedin.metadata.search.SearchService;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.RoutingException;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;
import com.linkedin.usage.UsageAggregation;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;

/**
 * Rest.li entry point: /usageStats
 */
@RestLiSimpleResource(name = "usageStats", namespace = "com.linkedin.usage")
public class UsageStats extends SimpleResourceTemplate<UsageAggregation> {
    @Inject
    @Named("searchService")
    private SearchService _searchService;

    @RestMethod.BatchCreate
    @Nonnull
    public Task<CreateResponse> batchCreate(@Nonnull List<UsageAggregation> data)
    {
        throw new RoutingException("'batch_create' not implemented", 400);
    }

}
