package com.linkedin.metadata.search.elasticsearch;

import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.search.LineageServiceTestBase;
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
public class LineageServiceElasticSearchTest extends LineageServiceTestBase {

  @Autowired private RestHighLevelClient _searchClient;
  @Autowired private ESBulkProcessor _bulkProcessor;
  @Autowired private ESIndexBuilder _esIndexBuilder;
  @Autowired private SearchConfiguration _searchConfiguration;
  @Autowired private CustomSearchConfiguration _customSearchConfiguration;

  @NotNull
  @Override
  protected RestHighLevelClient getSearchClient() {
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
  protected SearchConfiguration getSearchConfiguration() {
    return _searchConfiguration;
  }

  @NotNull
  @Override
  protected CustomSearchConfiguration getCustomSearchConfiguration() {
    return _customSearchConfiguration;
  }

  @Test
  public void initTest() {
    AssertJUnit.assertNotNull(_searchClient);
  }
}
