package com.linkedin.metadata.search.elasticsearch;

import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.search.TestEntityTestBase;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import org.jetbrains.annotations.NotNull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Import({
  ElasticSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class TestEntityElasticSearchTest extends TestEntityTestBase {

  @Autowired private RestHighLevelClient searchClient;
  @Autowired private ESBulkProcessor bulkProcessor;
  @Autowired private ESIndexBuilder esIndexBuilder;
  @Autowired private SearchConfiguration searchConfiguration;
  @Autowired private CustomSearchConfiguration customSearchConfiguration;

  @NotNull
  @Override
  protected RestHighLevelClient getSearchClient() {
    return searchClient;
  }

  @NotNull
  @Override
  protected ESBulkProcessor getBulkProcessor() {
    return bulkProcessor;
  }

  @NotNull
  @Override
  protected ESIndexBuilder getIndexBuilder() {
    return esIndexBuilder;
  }

  @NotNull
  @Override
  protected SearchConfiguration getSearchConfiguration() {
    return searchConfiguration;
  }

  @NotNull
  @Override
  protected CustomSearchConfiguration getCustomSearchConfiguration() {
    return customSearchConfiguration;
  }

  @Test
  public void initTest() {
    AssertJUnit.assertNotNull(searchClient);
  }
}
