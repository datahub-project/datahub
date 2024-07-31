package com.linkedin.metadata.search.opensearch;

import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.search.query.SearchDAOTestBase;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.fixtures.search.SampleDataFixtureConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.Getter;
import org.opensearch.client.RestHighLevelClient;
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
public class SearchDAOOpenSearchTest extends SearchDAOTestBase {
  @Autowired private RestHighLevelClient searchClient;
  @Autowired private SearchConfiguration searchConfiguration;

  @Autowired
  @Qualifier("sampleDataOperationContext")
  protected OperationContext operationContext;

  @Test
  public void initTest() {
    assertNotNull(searchClient);
  }
}
