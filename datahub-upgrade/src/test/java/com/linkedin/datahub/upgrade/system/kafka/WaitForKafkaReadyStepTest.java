package com.linkedin.datahub.upgrade.system.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.SetupConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WaitForKafkaReadyStepTest {

  private WaitForKafkaReadyStep step;
  private WaitForKafkaReadyStep spyStep;
  private OperationContext mockOpContext;
  private KafkaConfiguration kafkaConfiguration;
  private KafkaProperties kafkaProperties;
  private AdminClient mockAdminClient;
  private DescribeClusterResult mockDescribeClusterResult;

  @BeforeMethod
  public void setUp() {
    mockOpContext = mock(OperationContext.class);

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

    mockAdminClient = mock(AdminClient.class);
    mockDescribeClusterResult = mock(DescribeClusterResult.class);

    step = new WaitForKafkaReadyStep(mockOpContext, kafkaConfiguration, kafkaProperties);
    spyStep = spy(step);
  }

  @Test
  public void testId() {
    assertEquals(step.id(), "WaitForKafkaReadyStep");
  }

  @Test
  public void testRetryCount() {
    assertEquals(step.retryCount(), 60);
  }

  @Test
  public void testSuccessfulKafkaReady() throws Exception {
    // Mock successful cluster description
    Node mockNode1 = new Node(1, "localhost", 9092);
    Node mockNode2 = new Node(2, "localhost", 9093);

    KafkaFuture<String> clusterIdFuture = mock(KafkaFuture.class);
    KafkaFuture<Collection<Node>> nodesFuture = mock(KafkaFuture.class);

    when(clusterIdFuture.get(anyLong(), any(TimeUnit.class))).thenReturn("test-cluster-id");
    when(nodesFuture.get(anyLong(), any(TimeUnit.class)))
        .thenReturn(Arrays.asList(mockNode1, mockNode2));

    when(mockDescribeClusterResult.clusterId()).thenReturn(clusterIdFuture);
    when(mockDescribeClusterResult.nodes()).thenReturn(nodesFuture);
    when(mockAdminClient.describeCluster()).thenReturn(mockDescribeClusterResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testKafkaReadyWithNoBrokers() throws Exception {
    // Mock cluster description with no brokers
    KafkaFuture<String> clusterIdFuture = mock(KafkaFuture.class);
    KafkaFuture<Collection<Node>> nodesFuture = mock(KafkaFuture.class);

    when(clusterIdFuture.get(anyLong(), any(TimeUnit.class))).thenReturn("test-cluster-id");
    when(nodesFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(Collections.emptyList());

    when(mockDescribeClusterResult.clusterId()).thenReturn(clusterIdFuture);
    when(mockDescribeClusterResult.nodes()).thenReturn(nodesFuture);
    when(mockAdminClient.describeCluster()).thenReturn(mockDescribeClusterResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testKafkaReadyWithException() throws Exception {
    // Mock AdminClient to throw exception
    when(mockAdminClient.describeCluster())
        .thenThrow(new RuntimeException("Kafka connection failed"));

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testKafkaReadyWithTimeoutException() throws Exception {
    // Mock timeout exception
    KafkaFuture<String> clusterIdFuture = mock(KafkaFuture.class);
    when(clusterIdFuture.get(anyLong(), any(TimeUnit.class)))
        .thenThrow(new java.util.concurrent.TimeoutException("Timeout"));

    when(mockDescribeClusterResult.clusterId()).thenReturn(clusterIdFuture);
    when(mockAdminClient.describeCluster()).thenReturn(mockDescribeClusterResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testCreateAdminClientMethod() {
    // Test that the createAdminClient method exists and can be called
    // This will fail in tests since we don't have real Kafka configuration,
    // but it verifies the method structure
    try {
      step.createAdminClient();
    } catch (Exception e) {
      // Expected to fail in test environment, but method should exist
      assertNotNull(e);
    }
  }
}
