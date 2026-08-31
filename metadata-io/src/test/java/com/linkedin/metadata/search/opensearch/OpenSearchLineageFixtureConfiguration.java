package com.linkedin.metadata.search.opensearch;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import io.datahubproject.test.fixtures.search.SearchLineageFixtureConfiguration;
import org.springframework.boot.test.context.TestConfiguration;

/**
 * OpenSearch-specific configuration for lineage fixture tests. This configuration provides
 * TEST_OS_SEARCH_CONFIG for OpenSearch tests.
 */
@TestConfiguration
public class OpenSearchLineageFixtureConfiguration extends SearchLineageFixtureConfiguration {

  /** Provide OpenSearch configuration which has PIT enabled for graph queries. */
  @Override
  protected ElasticSearchConfiguration getElasticSearchConfiguration() {
    return TEST_OS_SEARCH_CONFIG;
  }
}
