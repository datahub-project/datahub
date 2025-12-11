/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
