package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import org.opensearch.client.RequestOptions;

/**
 * Holds OpenSearch {@link RequestOptions} so adapters avoid clashes with Elasticsearch REST {@code
 * RequestOptions}.
 */
public record OpenSearchRestRequestOptions(RequestOptions options) {}
