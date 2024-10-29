package com.linkedin.metadata.search.opensearch;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.fixtures.GoldenTestBase;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.fixtures.search.SampleDataFixtureConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Getter
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
  @Qualifier("longTailOperationContext")
  protected OperationContext operationContext;

  @Test
  public void initTest() {
    assertNotNull(searchService);
  }
}
