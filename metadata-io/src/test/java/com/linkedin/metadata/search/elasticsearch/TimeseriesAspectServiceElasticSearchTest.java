package com.linkedin.metadata.search.elasticsearch;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.timeseries.search.TimeseriesAspectServiceTestBase;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({ElasticSearchSuite.class, SearchTestContainerConfiguration.class})
public class TimeseriesAspectServiceElasticSearchTest extends TimeseriesAspectServiceTestBase {

  @Autowired private SearchClientShim<?> _searchClient;
  @Autowired private ESBulkProcessor _bulkProcessor;

  @Autowired
  @Qualifier("searchIndexBuilder")
  private ESIndexBuilder _esIndexBuilder;

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

  @Test
  public void initTest() {
    assertNotNull(_searchClient);
  }
}
