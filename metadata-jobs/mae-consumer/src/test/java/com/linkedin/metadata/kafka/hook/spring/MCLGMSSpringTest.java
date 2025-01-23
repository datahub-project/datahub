package com.linkedin.metadata.kafka.hook.spring;

import static org.testng.AssertJUnit.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.kafka.MCLKafkaListenerRegistrar;
import com.linkedin.metadata.kafka.hook.UpdateIndicesHook;
import com.linkedin.metadata.kafka.hook.event.EntityChangeEventGeneratorHook;
import com.linkedin.metadata.kafka.hook.incident.IncidentsSummaryHook;
import com.linkedin.metadata.kafka.hook.ingestion.IngestionSchedulerHook;
import com.linkedin.metadata.kafka.hook.siblings.SiblingAssociationHook;
import com.linkedin.metadata.service.UpdateIndicesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = {
      MCLSpringCommonTestConfiguration.class,
      MCLSpringGMSTestConfiguration.class,
      ConfigurationProvider.class
    },
    properties = {
      "ingestionScheduler.enabled=false",
      "configEntityRegistry.path=../../metadata-jobs/mae-consumer/src/test/resources/test-entity-registry.yml",
      "kafka.schemaRegistry.type=INTERNAL"
    })
@TestPropertySource(
    locations = "classpath:/application.yaml",
    properties = {"MCL_CONSUMER_ENABLED=true"})
@EnableAutoConfiguration(exclude = {CassandraAutoConfiguration.class})
public class MCLGMSSpringTest extends AbstractTestNGSpringContextTests {

  @Autowired private UpdateIndicesService updateIndicesService;

  @Test
  public void testHooks() {
    MCLKafkaListenerRegistrar registrar =
        applicationContext.getBean(MCLKafkaListenerRegistrar.class);
    assertTrue(
        registrar.getMetadataChangeLogHooks().stream()
            .noneMatch(hook -> hook instanceof IngestionSchedulerHook));
    assertTrue(
        registrar.getMetadataChangeLogHooks().stream()
            .anyMatch(hook -> hook instanceof UpdateIndicesHook));
    assertTrue(
        registrar.getMetadataChangeLogHooks().stream()
            .anyMatch(hook -> hook instanceof SiblingAssociationHook));
    assertTrue(
        registrar.getMetadataChangeLogHooks().stream()
            .anyMatch(hook -> hook instanceof EntityChangeEventGeneratorHook));
    assertEquals(
        1,
        registrar.getMetadataChangeLogHooks().stream()
            .filter(hook -> hook instanceof IncidentsSummaryHook)
            .count());
  }

  @Test
  public void testUpdateIndicesServiceInit() {
    assertNotNull(updateIndicesService);
    assertTrue(updateIndicesService.isSearchDiffMode());
    assertTrue(updateIndicesService.isStructuredPropertiesHookEnabled());
    assertTrue(updateIndicesService.isStructuredPropertiesWriteEnabled());

    assertNotNull(updateIndicesService.getUpdateGraphIndicesService());
    assertTrue(updateIndicesService.getUpdateGraphIndicesService().isGraphDiffMode());
    assertTrue(updateIndicesService.getUpdateGraphIndicesService().isGraphStatusEnabled());
  }
}
