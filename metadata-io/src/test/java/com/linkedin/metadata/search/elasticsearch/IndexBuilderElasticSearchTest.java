package com.linkedin.metadata.search.elasticsearch;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.indexbuilder.IndexBuilderTestBase;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import org.jetbrains.annotations.NotNull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({ElasticSearchSuite.class, SearchTestContainerConfiguration.class})
public class IndexBuilderElasticSearchTest extends IndexBuilderTestBase {

  @Autowired private RestHighLevelClient _searchClient;

  @NotNull
  @Override
  protected RestHighLevelClient getSearchClient() {
    return _searchClient;
  }

  @Test
  public void initTest() {
    assertNotNull(_searchClient);
  }
}
