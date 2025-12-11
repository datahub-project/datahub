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

import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.update.WriteDAOTestBase;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.fixtures.search.SampleDataFixtureConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Slf4j
@Getter
@Import({
  ElasticSearchSuite.class,
  SampleDataFixtureConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class WriteDAOElasticSearchTest extends WriteDAOTestBase {

  @Autowired private SearchClientShim<?> searchClient;

  @Autowired private ESWriteDAO esWriteDAO;

  @Autowired
  @Qualifier("sampleDataOperationContext")
  protected OperationContext operationContext;

  @Autowired
  @Qualifier("sampleDataEntitySearchService")
  protected ElasticSearchService entitySearchService;

  @Autowired
  @Qualifier("sampleDataGraphService")
  protected ElasticSearchGraphService graphService;

  @Test
  public void initTest() {
    assertNotNull(searchClient);
    assertNotNull(esWriteDAO);
  }
}
