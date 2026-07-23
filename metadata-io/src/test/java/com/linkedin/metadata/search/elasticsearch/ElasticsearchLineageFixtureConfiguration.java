package com.linkedin.metadata.search.elasticsearch;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import io.datahubproject.test.fixtures.search.SearchLineageFixtureConfiguration;
import org.springframework.boot.test.context.TestConfiguration;

/** Elasticsearch-specific configuration for lineage fixture tests. */
@TestConfiguration
public class ElasticsearchLineageFixtureConfiguration extends SearchLineageFixtureConfiguration {

  /** Provide Elasticsearch configuration. */
  @Override
  protected ElasticSearchConfiguration getElasticSearchConfiguration() {
    return TEST_ES_SEARCH_CONFIG;
  }
}
