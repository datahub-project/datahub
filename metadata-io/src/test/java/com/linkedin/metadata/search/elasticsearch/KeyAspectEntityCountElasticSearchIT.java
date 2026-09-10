package com.linkedin.metadata.search.elasticsearch;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountIntegrationTestBase;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({ElasticSearchSuite.class, SearchTestContainerConfiguration.class})
public class KeyAspectEntityCountElasticSearchIT extends KeyAspectEntityCountIntegrationTestBase {

  @Autowired private SearchClientShim<?> searchClient;
  @Autowired private ESBulkProcessor bulkProcessor;
  @Autowired private ESIndexBuilder indexBuilder;

  @Nonnull
  @Override
  protected SearchClientShim<?> getSearchClient() {
    return searchClient;
  }

  @Nonnull
  @Override
  protected ESBulkProcessor getBulkProcessor() {
    return bulkProcessor;
  }

  @Nonnull
  @Override
  protected ESIndexBuilder getIndexBuilder() {
    return indexBuilder;
  }

  @Test
  public void initTest() {
    assertNotNull(searchClient);
  }
}
