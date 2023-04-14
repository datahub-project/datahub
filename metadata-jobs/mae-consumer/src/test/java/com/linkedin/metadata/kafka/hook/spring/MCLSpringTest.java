package com.linkedin.metadata.kafka.hook.spring;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.kafka.MetadataChangeLogProcessor;
import com.linkedin.metadata.kafka.hook.UpdateIndicesHook;
import com.linkedin.metadata.kafka.hook.event.EntityChangeEventGeneratorHook;
import com.linkedin.metadata.kafka.hook.ingestion.IngestionSchedulerHook;
import com.linkedin.metadata.kafka.hook.siblings.SiblingAssociationHook;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;


@SpringBootTest(classes = {
    MCLSpringTestConfiguration.class
  },
    properties = {
      "ingestionScheduler.enabled=false",
      "configEntityRegistry.path=../../metadata-jobs/mae-consumer/src/test/resources/test-entity-registry.yml"
  })
@TestPropertySource(locations = "classpath:/application.yml", properties = {
    "MCL_CONSUMER_ENABLED=true"
})
@EnableConfigurationProperties(value = ConfigurationProvider.class)
public class MCLSpringTest extends AbstractTestNGSpringContextTests {

  @Test
  public void testHooks() {
    MetadataChangeLogProcessor metadataChangeLogProcessor = applicationContext.getBean(MetadataChangeLogProcessor.class);
    assertTrue(metadataChangeLogProcessor.getHooks().stream().noneMatch(hook -> hook instanceof IngestionSchedulerHook));
    assertTrue(metadataChangeLogProcessor.getHooks().stream().anyMatch(hook -> hook instanceof UpdateIndicesHook));
    assertTrue(metadataChangeLogProcessor.getHooks().stream().anyMatch(hook -> hook instanceof SiblingAssociationHook));
    assertTrue(metadataChangeLogProcessor.getHooks().stream().anyMatch(hook -> hook instanceof EntityChangeEventGeneratorHook));
  }
}
