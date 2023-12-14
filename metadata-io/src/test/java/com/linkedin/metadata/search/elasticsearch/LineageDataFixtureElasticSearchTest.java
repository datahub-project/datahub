package com.linkedin.metadata.search.elasticsearch;

import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.fixtures.LineageDataFixtureTestBase;
import io.datahubproject.test.fixtures.search.SearchLineageFixtureConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Import({
  ElasticSearchSuite.class,
  SearchLineageFixtureConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class LineageDataFixtureElasticSearchTest extends LineageDataFixtureTestBase {

  @Autowired
  @Qualifier("searchLineageSearchService")
  protected SearchService searchService;

  @Autowired
  @Qualifier("searchLineageLineageSearchService")
  protected LineageSearchService lineageService;

  @NotNull
  @Override
  protected LineageSearchService getLineageService() {
    return lineageService;
  }

  @NotNull
  @Override
  protected SearchService getSearchService() {
    return searchService;
  }

  @Test
  public void initTest() {
    AssertJUnit.assertNotNull(lineageService);
  }
}
