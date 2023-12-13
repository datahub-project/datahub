package com.linkedin.metadata.search.opensearch;

import io.datahubproject.test.search.OpenSearchTestContainer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.AfterSuite;

@TestConfiguration
public class OpenSearchSuite extends AbstractTestNGSpringContextTests {

  private static final OpenSearchTestContainer OPENSEARCH_TEST_CONTAINER;
  private static GenericContainer<?> container;

  static {
    OPENSEARCH_TEST_CONTAINER = new OpenSearchTestContainer();
  }

  @AfterSuite
  public void after() {
    OPENSEARCH_TEST_CONTAINER.stopContainer();
  }

  @Bean(name = "testSearchContainer")
  public GenericContainer<?> testSearchContainer() {
    if (container == null) {
      container = OPENSEARCH_TEST_CONTAINER.startContainer();
    }
    return container;
  }
}
