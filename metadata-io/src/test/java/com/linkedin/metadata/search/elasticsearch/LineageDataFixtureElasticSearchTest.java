package com.linkedin.metadata.search.elasticsearch;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.fixtures.LineageDataFixtureTestBase;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.fixtures.search.SearchLineageFixtureConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Getter
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

  @Autowired
  @Qualifier("searchLineageOperationContext")
  protected OperationContext operationContext;

  @Test
  public void initTest() {
    assertNotNull(lineageService);
  }
}
