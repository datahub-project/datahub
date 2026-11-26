package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.search.elasticsearch.query.v3.RenderedSearchTemplate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchRequest;

/**
 * Elasticsearch request with metadata needed to interpret its response. {@code templateSort} is
 * true when the template's own sort orders the request.
 */
public record PreparedSearchRequest(
    @Nonnull SearchRequest request,
    @Nullable RenderedSearchTemplate template,
    boolean templateSort) {}
