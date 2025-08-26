package com.linkedin.metadata.kafka.hook.spring;

import static org.testng.Assert.assertEquals;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.kafka.hook.usage.aws.EventBridgeAuditEventExportHook;
import com.linkedin.metadata.kafka.listener.usage.DataHubUsageEventKafkaListenerRegistrar;
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
      "aws.eventBridge.auditEventExport.enabled=true",
      "configEntityRegistry.path=../../metadata-jobs/mae-consumer/src/test/resources/test-entity-registry.yml",
      "kafka.schemaRegistry.type=INTERNAL",
      "kafka.consumer.mcl.aspectsToDrop={\"*\":[\"status\"],\"dataset\":[\"datasetProperties\"]}"
    })
@TestPropertySource(
    locations = "classpath:/application.yaml",
    properties = {"MCL_CONSUMER_ENABLED=true"})
@EnableAutoConfiguration(exclude = {CassandraAutoConfiguration.class})
public class MCLDataHubUsageSpringTest extends AbstractTestNGSpringContextTests {

  static {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testHooks() {
    DataHubUsageEventKafkaListenerRegistrar registrar =
        applicationContext.getBean(DataHubUsageEventKafkaListenerRegistrar.class);
    assertEquals(
        1,
        registrar.getEnabledHooks().stream()
            .filter(hook -> hook instanceof EventBridgeAuditEventExportHook)
            .count());
  }
}
