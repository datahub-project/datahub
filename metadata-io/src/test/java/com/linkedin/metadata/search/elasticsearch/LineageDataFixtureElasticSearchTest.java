/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.search.elasticsearch;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.fixtures.LineageDataFixtureTestBase;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Getter
@Import({
  ElasticSearchSuite.class,
  ElasticsearchLineageFixtureConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class LineageDataFixtureElasticSearchTest extends LineageDataFixtureTestBase {

  @Autowired
  @Qualifier("searchLineageSearchService")
  protected SearchService searchService;

  @Autowired
  @Qualifier("searchLineageLineageSearchService")
  protected LineageSearchService lineageService;

  @Autowired
  @Qualifier("searchLineageOperationContext")
  protected OperationContext operationContext;

  @Test
  public void initTest() {
    assertNotNull(lineageService);
  }
}
