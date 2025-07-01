package com.linkedin.metadata.search.opensearch;

import static org.testng.Assert.assertNotNull;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.fixtures.SampleDataFixtureTestBase;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.fixtures.search.SampleDataFixtureConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.Getter;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

/** Runs sample data fixture tests for Opensearch test container */
@Getter
@Import({
  OpenSearchSuite.class,
  SampleDataFixtureConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class SampleDataFixtureOpenSearchTest extends SampleDataFixtureTestBase {
  @Autowired private RestHighLevelClient searchClient;

  @Autowired
  @Qualifier("sampleDataSearchService")
  protected SearchService searchService;

  @Autowired
  @Qualifier("sampleDataEntityClient")
  protected EntityClient entityClient;

  @Autowired
  @Qualifier("sampleDataOperationContext")
  protected OperationContext operationContext;

  @Getter
  @Autowired
  @Qualifier("fixtureCustomSearchConfig")
  protected CustomSearchConfiguration customSearchConfiguration;

  @Test
  public void initTest() {
    assertNotNull(searchClient);
  }
}
