package com.linkedin.metadata.search.opensearch;

import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.fixtures.GoldenTestBase;
import io.datahubproject.test.fixtures.search.SampleDataFixtureConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({
  OpenSearchSuite.class,
  SampleDataFixtureConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class GoldenOpenSearchTest extends GoldenTestBase {

  @Autowired
  @Qualifier("longTailSearchService")
  protected SearchService searchService;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @NotNull
  @Override
  protected EntityRegistry getEntityRegistry() {
    return entityRegistry;
  }

  @NotNull
  @Override
  protected SearchService getSearchService() {
    return searchService;
  }

  @Test
  public void initTest() {
    assertNotNull(searchService);
  }
}
