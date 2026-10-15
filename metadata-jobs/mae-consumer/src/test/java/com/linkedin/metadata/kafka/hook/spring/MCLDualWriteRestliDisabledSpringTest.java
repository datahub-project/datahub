package com.linkedin.metadata.kafka.hook.spring;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.cassandra.autoconfigure.CassandraAutoConfiguration;
import org.springframework.boot.elasticsearch.autoconfigure.ElasticsearchClientAutoConfiguration;
import org.springframework.boot.elasticsearch.autoconfigure.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.testng.annotations.Test;

/** Separated deployment with rollback dual-write disabled — the bean must not be created at all. */
@SpringBootTest(
    classes = {MCLSpringCommonTestConfiguration.class, ConfigurationProvider.class},
    properties = {
      "spring.main.allow-bean-definition-overriding=true",
      "ingestionScheduler.enabled=false",
      "configEntityRegistry.path=../../metadata-jobs/mae-consumer/src/test/resources/test-entity-registry.yml",
      "kafka.schemaRegistry.type=INTERNAL",
      "entityClient.impl=restli",
      "elasticsearch.buildIndices.rollbackDualWriteEnabled=false"
    })
@TestPropertySource(
    locations = "classpath:/application.yaml",
    properties = {"MCL_CONSUMER_ENABLED=true"})
@EnableAutoConfiguration(
    exclude = {
      CassandraAutoConfiguration.class,
      ElasticsearchClientAutoConfiguration.class,
      ElasticsearchRestClientAutoConfiguration.class
    })
public class MCLDualWriteRestliDisabledSpringTest extends AbstractDualWriteDisabledSpringTest {

  static {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  // Declared here, not inherited: Gradle only detects a test class that carries @Test
  // itself, so an inherited-only test method silently never runs.
  @Test
  public void testNoDualWriteStrategyWhenFlagIsOff() {
    assertNoDualWriteStrategy();
  }
}
