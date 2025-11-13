package com.linkedin.datahub.upgrade.system.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.SetupConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KafkaNonBlockingSetupTest {

  private KafkaNonBlockingSetup upgrade;

  @Mock private OperationContext mockOpContext;
  private KafkaConfiguration kafkaConfiguration;
  private KafkaProperties kafkaProperties;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Create real KafkaConfiguration with test data
    kafkaConfiguration = new KafkaConfiguration();
    kafkaConfiguration.setBootstrapServers("localhost:9092");

    SetupConfiguration setupConfig = new SetupConfiguration();
    setupConfig.setPreCreateTopics(true);
    setupConfig.setUseConfluentSchemaRegistry(true);
    kafkaConfiguration.setSetup(setupConfig);

    // Create real KafkaProperties with test data
    kafkaProperties = new KafkaProperties();
    kafkaProperties.setBootstrapServers(Arrays.asList("localhost:9092"));

    upgrade = new KafkaNonBlockingSetup(mockOpContext, kafkaConfiguration, kafkaProperties);
  }

  @Test
  public void testId() {
    assertEquals(upgrade.id(), "KafkaNonBlockingSetup");
  }

  @Test
  public void testSteps() {
    List<UpgradeStep> steps = upgrade.steps();
    assertNotNull(steps);
    assertEquals(steps.size(), 1);
    assertTrue(steps.get(0) instanceof ConfluentSchemaRegistryCleanupPolicyStep);
  }

  @Test
  public void testStepsContainsCorrectStep() {
    List<UpgradeStep> steps = upgrade.steps();
    ConfluentSchemaRegistryCleanupPolicyStep step =
        (ConfluentSchemaRegistryCleanupPolicyStep) steps.get(0);
    assertEquals(step.id(), "ConfluentSchemaRegistryCleanupPolicyStep");
  }
}
