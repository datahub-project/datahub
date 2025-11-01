package com.linkedin.metadata.graph.search.elasticsearch;

import static com.linkedin.metadata.Constants.ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.graph.search.SearchGraphServiceTestBase;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchSuite;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({ElasticSearchSuite.class, SearchTestContainerConfiguration.class})
public class SearchGraphServiceElasticSearchTest extends SearchGraphServiceTestBase {

  @Autowired private SearchClientShim<?> _searchClient;
  @Autowired private ESBulkProcessor _bulkProcessor;
  @Autowired private ESIndexBuilder _esIndexBuilder;

  @NotNull
  @Override
  protected SearchClientShim<?> getSearchClient() {
    return _searchClient;
  }

  @NotNull
  @Override
  protected ESBulkProcessor getBulkProcessor() {
    return _bulkProcessor;
  }

  @NotNull
  @Override
  protected ESIndexBuilder getIndexBuilder() {
    return _esIndexBuilder;
  }

  @NotNull
  @Override
  protected String getElasticSearchImplementation() {
    return ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH;
  }

  @NotNull
  @Override
  protected ElasticSearchConfiguration getElasticSearchConfiguration() {
    return TEST_ES_SEARCH_CONFIG;
  }

  @Test
  public void initTest() {
    assertNotNull(_searchClient);
  }
}
