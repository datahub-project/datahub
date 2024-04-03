package com.linkedin.metadata.search.elasticsearch;

import io.datahubproject.test.search.ElasticsearchTestContainer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.AfterSuite;

@TestConfiguration
public class ElasticSearchSuite extends AbstractTestNGSpringContextTests {

  private static final ElasticsearchTestContainer ELASTICSEARCH_TEST_CONTAINER;
  private static GenericContainer<?> container;

  static {
    ELASTICSEARCH_TEST_CONTAINER = new ElasticsearchTestContainer();
  }

  @AfterSuite
  public void after() {
    ELASTICSEARCH_TEST_CONTAINER.stopContainer();
  }

  @Bean(name = "testSearchContainer")
  public GenericContainer<?> testSearchContainer() {
    if (container == null) {
      container = ELASTICSEARCH_TEST_CONTAINER.startContainer();
    }
    return container;
  }
}
