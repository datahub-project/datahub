package com.linkedin.datahub.upgrade;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.datahub.upgrade.system.SystemUpdateNonBlocking;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.BootstrapMCPStep;
import com.linkedin.metadata.boot.kafka.MockSystemUpdateDeserializer;
import com.linkedin.metadata.boot.kafka.MockSystemUpdateSerializer;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.entity.EntityServiceImpl;
import java.util.stream.Collectors;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    properties = {
      "kafka.schemaRegistry.type=INTERNAL",
    },
    args = {"-u", "SystemUpdateNonBlocking"})
public class DatahubUpgradeNonBlockingTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("systemUpdateNonBlocking")
  private SystemUpdateNonBlocking systemUpdateNonBlocking;

  @Autowired
  @Named("schemaRegistryConfig")
  private KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig;

  @Autowired
  @Named("duheKafkaEventProducer")
  private KafkaEventProducer duheKafkaEventProducer;

  @Autowired
  @Named("kafkaEventProducer")
  private KafkaEventProducer kafkaEventProducer;

  @Autowired private EntityServiceImpl entityService;

  @Test
  public void testSystemUpdateNonBlockingInit() {
    assertNotNull(systemUpdateNonBlocking);

    // Expected system update configuration and producer
    assertEquals(
        schemaRegistryConfig.getValue().getDeserializer(),
        MockSystemUpdateDeserializer.class.getName());
    assertEquals(
        schemaRegistryConfig.getValue().getSerializer(),
        MockSystemUpdateSerializer.class.getName());
    assertEquals(duheKafkaEventProducer, kafkaEventProducer);
    assertEquals(entityService.getProducer(), duheKafkaEventProducer);
  }

  @Test
  public void testNonBlockingBootstrapMCP() {
    java.util.List<BootstrapMCPStep> mcpTemplate =
        systemUpdateNonBlocking.steps().stream()
            .filter(update -> update instanceof BootstrapMCPStep)
            .map(update -> (BootstrapMCPStep) update)
            .toList();

    assertFalse(mcpTemplate.isEmpty());
    assertTrue(
        mcpTemplate.stream().noneMatch(update -> update.getMcpTemplate().isBlocking()),
        String.format(
            "Found blocking step: %s (expected non-blocking only)",
            mcpTemplate.stream()
                .filter(update -> update.getMcpTemplate().isBlocking())
                .map(update -> update.getMcpTemplate().getName())
                .collect(Collectors.toSet())));
  }
}
