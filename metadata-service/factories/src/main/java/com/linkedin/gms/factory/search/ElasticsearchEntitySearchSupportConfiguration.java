package com.linkedin.gms.factory.search;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Pulls in {@link ElasticSearchServiceFactory} only when Elasticsearch/OpenSearch integration is
 * enabled. {@link EntitySearchServiceFactory} imports this so PostgreSQL-only entity search does
 * not transitively register OpenSearch beans.
 */
@Configuration
@ConditionalOnProperty(
    prefix = "elasticsearch",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@Import(ElasticSearchServiceFactory.class)
public class ElasticsearchEntitySearchSupportConfiguration {}
