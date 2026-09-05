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

/**
 * Same-process deployment (MAE hook inside GMS, default {@code entityClient.impl=java}) with
 * rollback dual-write enabled. Here {@code SystemEntityClient} resolves in-process to {@code
 * EntityService}, so the same bean must wire up without any restli involvement.
 */
@SpringBootTest(
    classes = {
      MCLSpringCommonTestConfiguration.class,
      MCLSpringGMSTestConfiguration.class,
      ConfigurationProvider.class,
      DualWriteTestSupport.Phase1StateConfiguration.class
    },
    properties = {
      "spring.main.allow-bean-definition-overriding=true",
      "ingestionScheduler.enabled=false",
      "configEntityRegistry.path=../../metadata-jobs/mae-consumer/src/test/resources/test-entity-registry.yml",
      "kafka.schemaRegistry.type=INTERNAL",
      "elasticsearch.buildIndices.rollbackDualWriteEnabled=true"
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
public class MCLDualWriteGmsEnabledSpringTest extends AbstractDualWriteEnabledSpringTest {

  static {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  // Declared here, not inherited: Gradle only detects a test class that carries @Test
  // itself, so an inherited-only test method silently never runs.
  @Test
  public void testDualWriteStrategyResolvesItsTarget() {
    assertDualWriteResolvesItsTarget();
  }
}
