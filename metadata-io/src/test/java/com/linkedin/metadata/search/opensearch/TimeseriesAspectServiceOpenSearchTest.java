package com.linkedin.metadata.search.opensearch;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.timeseries.search.TimeseriesAspectServiceTestBase;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import org.jetbrains.annotations.NotNull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({OpenSearchSuite.class, SearchTestContainerConfiguration.class})
public class TimeseriesAspectServiceOpenSearchTest extends TimeseriesAspectServiceTestBase {

  @Autowired private RestHighLevelClient _searchClient;
  @Autowired private ESBulkProcessor _bulkProcessor;

  @Autowired
  @Qualifier("searchIndexBuilder")
  private ESIndexBuilder _esIndexBuilder;

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

  @Test
  public void initTest() {
    assertNotNull(_searchClient);
  }
}
