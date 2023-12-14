package com.linkedin.metadata.kafka.hook.spring;

import static org.testng.AssertJUnit.assertEquals;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.kafka.MetadataChangeLogProcessor;
import com.linkedin.metadata.kafka.hook.UpdateIndicesHook;
import com.linkedin.metadata.kafka.hook.assertion.AssertionActionsHook;
import com.linkedin.metadata.kafka.hook.assertion.AssertionsSummaryHook;
import com.linkedin.metadata.kafka.hook.event.EntityChangeEventGeneratorHook;
import com.linkedin.metadata.kafka.hook.incident.IncidentsSummaryHook;
import com.linkedin.metadata.kafka.hook.ingestion.IngestionSchedulerHook;
import com.linkedin.metadata.kafka.hook.notification.NotificationGeneratorHook;
import com.linkedin.metadata.kafka.hook.siblings.SiblingAssociationHook;
import com.linkedin.metadata.kafka.hook.test.MetadataTestHook;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = {MCLSpringTestConfiguration.class, ConfigurationProvider.class},
    properties = {
      "ingestionScheduler.enabled=false",
      "configEntityRegistry.path=../../metadata-jobs/mae-consumer/src/test/resources/test-entity-registry.yml",
      "kafka.schemaRegistry.type=INTERNAL"
    })
@TestPropertySource(
    locations = "classpath:/application.yml",
    properties = {"MCL_CONSUMER_ENABLED=true"})
@EnableAutoConfiguration(exclude = {CassandraAutoConfiguration.class})
public class MCLSpringTest extends AbstractTestNGSpringContextTests {

  static {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testHooks() {
    MetadataChangeLogProcessor metadataChangeLogProcessor =
        applicationContext.getBean(MetadataChangeLogProcessor.class);

    assertEquals(
        0,
        metadataChangeLogProcessor.getHooks().stream()
            .filter(hook -> hook instanceof IngestionSchedulerHook)
            .count());
    assertEquals(
        1,
        metadataChangeLogProcessor.getHooks().stream()
            .filter(hook -> hook instanceof UpdateIndicesHook)
            .count());
    assertEquals(
        1,
        metadataChangeLogProcessor.getHooks().stream()
            .filter(hook -> hook instanceof SiblingAssociationHook)
            .count());
    assertEquals(
        1,
        metadataChangeLogProcessor.getHooks().stream()
            .filter(hook -> hook instanceof EntityChangeEventGeneratorHook)
            .count());
    assertEquals(
        1,
        metadataChangeLogProcessor.getHooks().stream()
            .filter(hook -> hook instanceof NotificationGeneratorHook)
            .count());
    assertEquals(
        1,
        metadataChangeLogProcessor.getHooks().stream()
            .filter(hook -> hook instanceof IncidentsSummaryHook)
            .count());
    assertEquals(
        1,
        metadataChangeLogProcessor.getHooks().stream()
            .filter(hook -> hook instanceof MetadataTestHook)
            .count());
    assertEquals(
        1,
        metadataChangeLogProcessor.getHooks().stream()
            .filter(hook -> hook instanceof AssertionsSummaryHook)
            .count());
    assertEquals(
        1,
        metadataChangeLogProcessor.getHooks().stream()
            .filter(hook -> hook instanceof AssertionActionsHook)
            .count());
  }
}
