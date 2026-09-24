package com.linkedin.metadata.search.elasticsearch;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.datahubusage.DataHubUsageServiceTestBase;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({
  ElasticSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class DataHubUsageServiceElasticSearchTest extends DataHubUsageServiceTestBase {

  @Autowired private SearchClientShim<?> searchClient;

  @Nonnull
  @Override
  protected SearchClientShim<?> getSearchClient() {
    return searchClient;
  }

  @Test
  public void initTest() {
    assertNotNull(searchClient);
  }
}
