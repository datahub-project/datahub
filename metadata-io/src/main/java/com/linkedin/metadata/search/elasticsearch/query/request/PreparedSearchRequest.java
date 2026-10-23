package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.search.elasticsearch.query.v3.RenderedSearchTemplate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchRequest;

/** Elasticsearch request with metadata needed to interpret its response. */
public record PreparedSearchRequest(
    @Nonnull SearchRequest request, @Nullable RenderedSearchTemplate template) {}
