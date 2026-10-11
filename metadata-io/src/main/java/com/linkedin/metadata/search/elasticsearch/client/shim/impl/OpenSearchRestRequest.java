package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import org.opensearch.client.Request;

/**
 * Holds an OpenSearch REST {@link Request} so adapters avoid simple-name clashes with Elasticsearch
 * REST types.
 */
public record OpenSearchRestRequest(Request request) {}
