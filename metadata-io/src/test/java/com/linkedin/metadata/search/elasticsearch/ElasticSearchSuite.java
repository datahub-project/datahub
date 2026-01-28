package com.linkedin.metadata.search.elasticsearch;

import com.linkedin.metadata.graph.search.elasticsearch.SearchGraphServiceElasticSearchTest;
import io.datahubproject.test.search.ElasticsearchTestContainer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestContextManager;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;

@TestConfiguration
public class ElasticSearchSuite extends AbstractTestNGSpringContextTests {

  private static ElasticsearchTestContainer ELASTICSEARCH_TEST_CONTAINER = null;
  private static GenericContainer<?> container;

  @Parameters({"elasticVersion"})
  @BeforeTest
  public void setUpEnvironment(String elasticVersion) {
    System.setProperty("ELASTIC_VERSION", elasticVersion);
    // Forces ApplicationContext reset so that it is not shared across different version executions,
    // without this it will not reset the container DO NOT REMOVE
    TestContextManager testContextManager =
        new TestContextManager(SearchServiceElasticSearchTest.class);
    testContextManager
        .getTestContext()
        .markApplicationContextDirty(DirtiesContext.HierarchyMode.EXHAUSTIVE);

    TestContextManager graphTestContextManager =
        new TestContextManager(SearchGraphServiceElasticSearchTest.class);
    graphTestContextManager
        .getTestContext()
        .markApplicationContextDirty(DirtiesContext.HierarchyMode.EXHAUSTIVE);

    TestContextManager goldenTestContextManager =
        new TestContextManager(GoldenElasticSearchTest.class);
    goldenTestContextManager
        .getTestContext()
        .markApplicationContextDirty(DirtiesContext.HierarchyMode.EXHAUSTIVE);

    TestContextManager lineageTestContextManager =
        new TestContextManager(LineageDataFixtureElasticSearchTest.class);
    lineageTestContextManager
        .getTestContext()
        .markApplicationContextDirty(DirtiesContext.HierarchyMode.EXHAUSTIVE);
  }

  @AfterSuite
  public void after() {
    if (ELASTICSEARCH_TEST_CONTAINER != null) {
      ELASTICSEARCH_TEST_CONTAINER.stopContainer();
    }
  }

  @Bean(name = "testSearchContainer")
  public GenericContainer<?> testSearchContainer() {
    if (container == null || !container.isRunning()) {
      ELASTICSEARCH_TEST_CONTAINER = new ElasticsearchTestContainer();
      container = ELASTICSEARCH_TEST_CONTAINER.startContainer();
    }
    return container;
  }
}
