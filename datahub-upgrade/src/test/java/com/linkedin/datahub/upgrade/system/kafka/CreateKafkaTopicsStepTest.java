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
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateKafkaTopicsStepTest {

  private CreateKafkaTopicsStep step;
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

    step = new CreateKafkaTopicsStep(mockOpContext, kafkaConfiguration, kafkaProperties);
  }

  @Test
  public void testId() {
    assertEquals(step.id(), "CreateKafkaTopicsStep");
  }

  @Test
  public void testSkipTopicCreationWhenDisabled() {
    // Set preCreateTopics to false
    kafkaConfiguration.getSetup().setPreCreateTopics(false);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSkipTopicCreationWhenSetupConfigIsNull() {
    // Set setup configuration as null
    kafkaConfiguration.setSetup(null);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSkipTopicCreationWhenTopicsConfigIsNull() {
    // Set topics configuration as null
    kafkaConfiguration.setTopics(null);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    // When topics is null, getTopics() still returns a TopicsConfiguration with empty topics map
    // which causes the step to fail because no topics are configured
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testFailWhenNoTopicsConfigured() {
    // Set empty topics map directly on KafkaConfiguration
    kafkaConfiguration.setTopics(Collections.emptyMap());

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testSuccessfulTopicCreation() throws Exception {
    // Create spy to mock the createAdminClient method
    CreateKafkaTopicsStep spyStep = spy(step);

    // Mock AdminClient and its dependencies
    AdminClient mockAdminClient = mock(AdminClient.class);
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
    CreateTopicsResult mockCreateTopicsResult = mock(CreateTopicsResult.class);

    // Mock existing topics (empty set - no existing topics)
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(new HashSet<>());
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    // Mock topic creation result
    KafkaFuture<Void> createTopicsFuture = mock(KafkaFuture.class);
    when(createTopicsFuture.get()).thenReturn(null); // Success
    when(mockCreateTopicsResult.all()).thenReturn(createTopicsFuture);
    when(mockAdminClient.createTopics(any())).thenReturn(mockCreateTopicsResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    // Set up topics configuration with test topics
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setPartitions(3);
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(true);
    topics.put("testTopic", topicConfig);
    kafkaConfiguration.setTopics(topics);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that createTopics was called
    verify(mockAdminClient, times(1)).createTopics(any());
  }

  @Test
  public void testSkipExistingTopics() throws Exception {
    // Create spy to mock the createAdminClient method
    CreateKafkaTopicsStep spyStep = spy(step);

    // Mock AdminClient and its dependencies
    AdminClient mockAdminClient = mock(AdminClient.class);
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);

    // Mock existing topics (topic already exists)
    Set<String> existingTopics = new HashSet<>();
    existingTopics.add("test-topic");
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(existingTopics);
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    // Set up topics configuration with test topics
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setPartitions(3);
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(true);
    topics.put("testTopic", topicConfig);
    kafkaConfiguration.setTopics(topics);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that createTopics was NOT called since topic already exists
    verify(mockAdminClient, times(0)).createTopics(any());
  }

  @Test
  public void testSkipTopicWhenCreateFlagIsFalse() throws Exception {
    // Create spy to mock the createAdminClient method
    CreateKafkaTopicsStep spyStep = spy(step);

    // Mock AdminClient and its dependencies
    AdminClient mockAdminClient = mock(AdminClient.class);
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);

    // Mock existing topics (empty set)
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(new HashSet<>());
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    // Set up topics configuration with create flag set to false
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setPartitions(3);
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(false); // Don't create this topic
    topics.put("testTopic", topicConfig);
    kafkaConfiguration.setTopics(topics);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that createTopics was NOT called since create flag is false
    verify(mockAdminClient, times(0)).createTopics(any());
  }

  @Test
  public void testTopicCreationWithConfigProperties() throws Exception {
    // Create spy to mock the createAdminClient method
    CreateKafkaTopicsStep spyStep = spy(step);

    // Mock AdminClient and its dependencies
    AdminClient mockAdminClient = mock(AdminClient.class);
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
    CreateTopicsResult mockCreateTopicsResult = mock(CreateTopicsResult.class);

    // Mock existing topics (empty set)
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(new HashSet<>());
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    // Mock topic creation result
    KafkaFuture<Void> createTopicsFuture = mock(KafkaFuture.class);
    when(createTopicsFuture.get()).thenReturn(null); // Success
    when(mockCreateTopicsResult.all()).thenReturn(createTopicsFuture);
    when(mockAdminClient.createTopics(any())).thenReturn(mockCreateTopicsResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    // Set up topics configuration with config properties
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setPartitions(3);
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(true);

    // Add config properties
    Map<String, String> configProperties = new HashMap<>();
    configProperties.put("retention.ms", "86400000");
    configProperties.put("cleanup.policy", "delete");
    topicConfig.setConfigProperties(configProperties);

    topics.put("testTopic", topicConfig);
    kafkaConfiguration.setTopics(topics);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that createTopics was called
    verify(mockAdminClient, times(1)).createTopics(any());
  }

  @Test
  public void testTopicCreationFailure() throws Exception {
    // Create spy to mock the createAdminClient method
    CreateKafkaTopicsStep spyStep = spy(step);

    // Mock AdminClient and its dependencies
    AdminClient mockAdminClient = mock(AdminClient.class);
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);

    // Mock existing topics (empty set)
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(new HashSet<>());
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    // Mock topic creation to throw exception
    when(mockAdminClient.createTopics(any()))
        .thenThrow(new RuntimeException("Topic creation failed"));

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    // Set up topics configuration
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setPartitions(3);
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(true);
    topics.put("testTopic", topicConfig);
    kafkaConfiguration.setTopics(topics);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testListTopicsFailure() throws Exception {
    // Create spy to mock the createAdminClient method
    CreateKafkaTopicsStep spyStep = spy(step);

    // Mock AdminClient to throw exception when listing topics
    AdminClient mockAdminClient = mock(AdminClient.class);
    when(mockAdminClient.listTopics()).thenThrow(new RuntimeException("Failed to list topics"));

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    // Set up topics configuration
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setPartitions(3);
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(true);
    topics.put("testTopic", topicConfig);
    kafkaConfiguration.setTopics(topics);

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
