package com.linkedin.metadata.search.opensearch;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyMappingLookupTestBase;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({
  OpenSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class StructuredPropertyMappingLookupIT extends StructuredPropertyMappingLookupTestBase {

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
