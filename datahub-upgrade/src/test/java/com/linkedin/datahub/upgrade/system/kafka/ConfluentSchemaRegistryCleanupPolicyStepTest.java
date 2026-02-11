package com.linkedin.datahub.upgrade.system.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ProducerConfiguration;
import com.linkedin.metadata.config.kafka.SetupConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConfluentSchemaRegistryCleanupPolicyStepTest {

  private ConfluentSchemaRegistryCleanupPolicyStep step;
  private OperationContext mockOpContext;
  private KafkaConfiguration kafkaConfiguration;
  private KafkaProperties kafkaProperties;

  @BeforeMethod
  public void setUp() {
    mockOpContext = mock(OperationContext.class);

    // Create real KafkaConfiguration with test data
    kafkaConfiguration = new KafkaConfiguration();
    kafkaConfiguration.setProducer(new ProducerConfiguration());
    kafkaConfiguration.setBootstrapServers("localhost:9092");

    SetupConfiguration setupConfig = new SetupConfiguration();
    setupConfig.setPreCreateTopics(true);
    setupConfig.setUseConfluentSchemaRegistry(true);
    kafkaConfiguration.setSetup(setupConfig);

    // Create real KafkaProperties with test data
    kafkaProperties = new KafkaProperties();
    kafkaProperties.setBootstrapServers(Arrays.asList("localhost:9092"));

    step =
        new ConfluentSchemaRegistryCleanupPolicyStep(
            mockOpContext, kafkaConfiguration, kafkaProperties);
  }

  @Test
  public void testId() {
    assertEquals(step.id(), "ConfluentSchemaRegistryCleanupPolicyStep");
  }

  @Test
  public void testSkipWhenSchemaRegistryDisabled() {
    // Set schema registry to disabled
    kafkaConfiguration.getSetup().setUseConfluentSchemaRegistry(false);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSkipWhenSetupConfigIsNull() {
    // Set setup configuration as null
    kafkaConfiguration.setSetup(null);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSkipWhenPreCreateTopicsDisabled() {
    // Set preCreateTopics to false
    kafkaConfiguration.getSetup().setPreCreateTopics(false);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSuccessfulCleanupPolicyConfiguration() throws Exception {
    // Create spy to mock the createAdminClient method
    ConfluentSchemaRegistryCleanupPolicyStep spyStep = spy(step);

    // Mock AdminClient and its dependencies
    AdminClient mockAdminClient = mock(AdminClient.class);
    AlterConfigsResult mockAlterConfigsResult = mock(AlterConfigsResult.class);

    // Mock successful alter configs result
    KafkaFuture<Void> alterConfigsFuture = mock(KafkaFuture.class);
    when(alterConfigsFuture.get()).thenReturn(null); // Success
    when(mockAlterConfigsResult.all()).thenReturn(alterConfigsFuture);
    when(mockAdminClient.incrementalAlterConfigs(any())).thenReturn(mockAlterConfigsResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that incrementalAlterConfigs was called
    verify(mockAdminClient, times(1)).incrementalAlterConfigs(any());
  }

  @Test
  public void testAlterConfigsFailure() throws Exception {
    // Create spy to mock the createAdminClient method
    ConfluentSchemaRegistryCleanupPolicyStep spyStep = spy(step);

    // Mock AdminClient to throw exception when altering configs
    AdminClient mockAdminClient = mock(AdminClient.class);
    when(mockAdminClient.incrementalAlterConfigs(any()))
        .thenThrow(new RuntimeException("Failed to alter configs"));

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testAdminClientException() throws Exception {
    // Create spy to mock the createAdminClient method
    ConfluentSchemaRegistryCleanupPolicyStep spyStep = spy(step);

    // Mock AdminClient to throw exception when calling incrementalAlterConfigs
    AdminClient mockAdminClient = mock(AdminClient.class);
    AlterConfigsResult mockAlterConfigsResult = mock(AlterConfigsResult.class);

    // Mock alter configs result that throws exception
    KafkaFuture<Void> alterConfigsFuture = mock(KafkaFuture.class);
    when(alterConfigsFuture.get()).thenThrow(new RuntimeException("AdminClient operation failed"));
    when(mockAlterConfigsResult.all()).thenReturn(alterConfigsFuture);
    when(mockAdminClient.incrementalAlterConfigs(any())).thenReturn(mockAlterConfigsResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testCreateAdminClientMethod() {
    // Test that the createAdminClient method can be called
    AdminClient adminClient = step.createAdminClient();
    assertNotNull(adminClient);
  }
}
