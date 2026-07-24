package com.linkedin.metadata.utils.elasticsearch;

/**
 * One per-document failure from an ES/OpenSearch async task {@code response.failures[]} entry.
 *
 * <p>{@link org.opensearch.client.tasks.GetTaskResponse} drops this array; we parse it from the raw
 * task JSON for logging.
 */
public record TaskFailureDetail(
    String index, String documentId, String causeType, String causeReason, int shard) {}
