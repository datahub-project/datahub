package com.linkedin.metadata.search.opensearch;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.update.WriteDAOTestBase;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.fixtures.search.SampleDataFixtureConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Slf4j
@Getter
@Import({
  OpenSearchSuite.class,
  SampleDataFixtureConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class WriteDAOOpenSearchTest extends WriteDAOTestBase {

  @Autowired private RestHighLevelClient searchClient;

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
