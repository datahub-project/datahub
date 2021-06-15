package com.linkedin.metadata.resources.usage;

import com.linkedin.common.EntityRelationships;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * Rest.li entry point: /usageStats
 */
@RestLiSimpleResource(name = "usageStats", namespace = "com.linkedin.usage")
public class UsageStats extends SimpleResourceTemplate<EntityRelationships> {
    @Inject
    @Named("searchService")
    private SearchService _searchService;

}
