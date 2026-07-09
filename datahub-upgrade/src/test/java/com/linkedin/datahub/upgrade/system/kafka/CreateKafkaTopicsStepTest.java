package com.linkedin.datahub.upgrade.system.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
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
    setupConfig.setAutoIncreasePartitions(true);
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
  public void testSkipExistingTopicsWithSamePartitionCount() throws Exception {
    // Create spy to mock the createAdminClient method
    CreateKafkaTopicsStep spyStep = spy(step);

    // Mock AdminClient and its dependencies
    AdminClient mockAdminClient = mock(AdminClient.class);
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
    DescribeTopicsResult mockDescribeTopicsResult = mock(DescribeTopicsResult.class);

    // Mock existing topics (topic already exists)
    Set<String> existingTopics = new HashSet<>();
    existingTopics.add("test-topic");
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(existingTopics);
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    // Mock topic description with 3 partitions (same as config)
    TopicDescription mockTopicDescription = mock(TopicDescription.class);
    TopicPartitionInfo partition1 = mock(TopicPartitionInfo.class);
    TopicPartitionInfo partition2 = mock(TopicPartitionInfo.class);
    TopicPartitionInfo partition3 = mock(TopicPartitionInfo.class);
    when(mockTopicDescription.partitions())
        .thenReturn(Arrays.asList(partition1, partition2, partition3));

    Map<String, TopicDescription> topicDescriptionMap = new HashMap<>();
    topicDescriptionMap.put("test-topic", mockTopicDescription);
    KafkaFuture<Map<String, TopicDescription>> describeTopicsFuture = mock(KafkaFuture.class);
    when(describeTopicsFuture.get()).thenReturn(topicDescriptionMap);
    when(mockDescribeTopicsResult.allTopicNames()).thenReturn(describeTopicsFuture);
    when(mockAdminClient.describeTopics(anyList())).thenReturn(mockDescribeTopicsResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    // Set up topics configuration with test topics (3 partitions)
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

    // Verify that createTopics was NOT called since topic already exists with correct partition
    // count
    verify(mockAdminClient, times(0)).createTopics(any());
    // Verify that createPartitions was NOT called since partition count matches
    verify(mockAdminClient, times(0)).createPartitions(any());
  }

  @Test
  public void testIncreasePartitionsForExistingTopic() throws Exception {
    // Create spy to mock the createAdminClient method
    CreateKafkaTopicsStep spyStep = spy(step);

    // Mock AdminClient and its dependencies
    AdminClient mockAdminClient = mock(AdminClient.class);
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
    DescribeTopicsResult mockDescribeTopicsResult = mock(DescribeTopicsResult.class);
    CreatePartitionsResult mockCreatePartitionsResult = mock(CreatePartitionsResult.class);

    // Mock existing topics (topic already exists)
    Set<String> existingTopics = new HashSet<>();
    existingTopics.add("test-topic");
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(existingTopics);
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    // Mock topic description with 3 partitions (less than config's 5)
    TopicDescription mockTopicDescription = mock(TopicDescription.class);
    TopicPartitionInfo partition1 = mock(TopicPartitionInfo.class);
    TopicPartitionInfo partition2 = mock(TopicPartitionInfo.class);
    TopicPartitionInfo partition3 = mock(TopicPartitionInfo.class);
    when(mockTopicDescription.partitions())
        .thenReturn(Arrays.asList(partition1, partition2, partition3));

    Map<String, TopicDescription> topicDescriptionMap = new HashMap<>();
    topicDescriptionMap.put("test-topic", mockTopicDescription);
    KafkaFuture<Map<String, TopicDescription>> describeTopicsFuture = mock(KafkaFuture.class);
    when(describeTopicsFuture.get()).thenReturn(topicDescriptionMap);
    when(mockDescribeTopicsResult.allTopicNames()).thenReturn(describeTopicsFuture);
    when(mockAdminClient.describeTopics(anyList())).thenReturn(mockDescribeTopicsResult);

    // Mock partition increase result
    KafkaFuture<Void> createPartitionsFuture = mock(KafkaFuture.class);
    when(createPartitionsFuture.get()).thenReturn(null); // Success
    when(mockCreatePartitionsResult.all()).thenReturn(createPartitionsFuture);
    when(mockAdminClient.createPartitions(any())).thenReturn(mockCreatePartitionsResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    // Set up topics configuration with test topics (5 partitions desired)
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setPartitions(5); // Want to increase from 3 to 5
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
    // Verify that createPartitions WAS called to increase partition count
    verify(mockAdminClient, times(1)).createPartitions(any());
  }

  @Test
  public void testWarnWhenTopicHasMorePartitionsThanConfig() throws Exception {
    // Create spy to mock the createAdminClient method
    CreateKafkaTopicsStep spyStep = spy(step);

    // Mock AdminClient and its dependencies
    AdminClient mockAdminClient = mock(AdminClient.class);
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
    DescribeTopicsResult mockDescribeTopicsResult = mock(DescribeTopicsResult.class);

    // Mock existing topics (topic already exists)
    Set<String> existingTopics = new HashSet<>();
    existingTopics.add("test-topic");
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(existingTopics);
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    // Mock topic description with 5 partitions (more than config's 3)
    TopicDescription mockTopicDescription = mock(TopicDescription.class);
    TopicPartitionInfo partition1 = mock(TopicPartitionInfo.class);
    TopicPartitionInfo partition2 = mock(TopicPartitionInfo.class);
    TopicPartitionInfo partition3 = mock(TopicPartitionInfo.class);
    TopicPartitionInfo partition4 = mock(TopicPartitionInfo.class);
    TopicPartitionInfo partition5 = mock(TopicPartitionInfo.class);
    when(mockTopicDescription.partitions())
        .thenReturn(Arrays.asList(partition1, partition2, partition3, partition4, partition5));

    Map<String, TopicDescription> topicDescriptionMap = new HashMap<>();
    topicDescriptionMap.put("test-topic", mockTopicDescription);
    KafkaFuture<Map<String, TopicDescription>> describeTopicsFuture = mock(KafkaFuture.class);
    when(describeTopicsFuture.get()).thenReturn(topicDescriptionMap);
    when(mockDescribeTopicsResult.allTopicNames()).thenReturn(describeTopicsFuture);
    when(mockAdminClient.describeTopics(anyList())).thenReturn(mockDescribeTopicsResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    // Set up topics configuration with test topics (3 partitions desired, but topic has 5)
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setPartitions(3); // Config says 3, but topic has 5
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(true);
    topics.put("testTopic", topicConfig);
    kafkaConfiguration.setTopics(topics);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that createTopics was NOT called
    verify(mockAdminClient, times(0)).createTopics(any());
    // Verify that createPartitions was NOT called (can't reduce partitions)
    verify(mockAdminClient, times(0)).createPartitions(any());
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
  public void testFailWhenPartitionCountCheckFails() throws Exception {
    // Create spy to mock the createAdminClient method
    CreateKafkaTopicsStep spyStep = spy(step);

    // Mock AdminClient and its dependencies
    AdminClient mockAdminClient = mock(AdminClient.class);
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
    DescribeTopicsResult mockDescribeTopicsResult = mock(DescribeTopicsResult.class);

    // Mock existing topics (topic already exists)
    Set<String> existingTopics = new HashSet<>();
    existingTopics.add("test-topic");
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(existingTopics);
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    // Mock describeTopics to throw an exception
    KafkaFuture<Map<String, TopicDescription>> describeTopicsFuture = mock(KafkaFuture.class);
    when(describeTopicsFuture.get()).thenThrow(new RuntimeException("Failed to describe topic"));
    when(mockDescribeTopicsResult.allTopicNames()).thenReturn(describeTopicsFuture);
    when(mockAdminClient.describeTopics(anyList())).thenReturn(mockDescribeTopicsResult);

    // Mock the createAdminClient method to return our mock AdminClient
    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    // Set up topics configuration with test topics
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setPartitions(5);
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(true);
    topics.put("testTopic", topicConfig);
    kafkaConfiguration.setTopics(topics);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    // Should fail when partition count check fails
    assertEquals(result.result(), DataHubUpgradeState.FAILED);

    // Verify that createTopics was NOT called
    verify(mockAdminClient, times(0)).createTopics(any());
    // Verify that createPartitions was NOT called
    verify(mockAdminClient, times(0)).createPartitions(any());
  }

  @Test
  public void testSkipPartitionIncreaseWhenAutoIncreaseDisabled() throws Exception {
    // Create spy to mock the createAdminClient method
    CreateKafkaTopicsStep spyStep = spy(step);

    // Disable auto-increase partitions
    kafkaConfiguration.getSetup().setAutoIncreasePartitions(false);

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

    // Set up topics configuration with test topics (5 partitions desired, but topic has 3)
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setPartitions(5); // Want to increase from 3 to 5, but auto-increase is disabled
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
    // Verify that createPartitions was NOT called since auto-increase is disabled
    verify(mockAdminClient, times(0)).createPartitions(any());
    // Verify that describeTopics was NOT called since auto-increase is disabled
    verify(mockAdminClient, times(0)).describeTopics(anyList());
  }

  @Test
  public void testCreateAdminClientMethod() {
    // Test that the createAdminClient method can be called
    AdminClient adminClient = step.createAdminClient();
    assertNotNull(adminClient);
  }

  @Test
  public void testReconcileExistingTopicConfigsWhenEnabled() throws Exception {
    kafkaConfiguration.getSetup().setReconcileExistingTopicConfigs(true);

    CreateKafkaTopicsStep spyStep = spy(step);
    AdminClient mockAdminClient = mock(AdminClient.class);

    Set<String> existingTopics = new HashSet<>();
    existingTopics.add("DataHubUpgradeHistory_v1");
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(existingTopics);
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    // Describe returns matching partition count so partition reconciliation is a no-op
    TopicDescription mockDescription = mock(TopicDescription.class);
    TopicPartitionInfo partition = mock(TopicPartitionInfo.class);
    when(mockDescription.partitions()).thenReturn(Arrays.asList(partition));
    Map<String, TopicDescription> descMap = new HashMap<>();
    descMap.put("DataHubUpgradeHistory_v1", mockDescription);
    DescribeTopicsResult mockDescribeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<Map<String, TopicDescription>> descFuture = mock(KafkaFuture.class);
    when(descFuture.get()).thenReturn(descMap);
    when(mockDescribeResult.allTopicNames()).thenReturn(descFuture);
    when(mockAdminClient.describeTopics(anyList())).thenReturn(mockDescribeResult);

    // describeConfigs returns no current retention.ms so the diff sees an "added" entry
    DescribeConfigsResult mockDescribeConfigsResult = mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> describeConfigsFuture = mock(KafkaFuture.class);
    when(describeConfigsFuture.get()).thenReturn(new HashMap<>());
    when(mockDescribeConfigsResult.all()).thenReturn(describeConfigsFuture);
    when(mockAdminClient.describeConfigs(any(Collection.class)))
        .thenReturn(mockDescribeConfigsResult);

    AlterConfigsResult mockAlterResult = mock(AlterConfigsResult.class);
    KafkaFuture<Void> alterFuture = mock(KafkaFuture.class);
    when(alterFuture.get()).thenReturn(null);
    when(mockAlterResult.all()).thenReturn(alterFuture);
    when(mockAdminClient.incrementalAlterConfigs(any(Map.class))).thenReturn(mockAlterResult);

    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("DataHubUpgradeHistory_v1");
    topicConfig.setPartitions(1);
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(true);
    Map<String, String> configProps = new HashMap<>();
    configProps.put("retention.ms", "-1");
    topicConfig.setConfigProperties(configProps);
    topics.put("datahubUpgradeHistory", topicConfig);
    kafkaConfiguration.setTopics(topics);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockAdminClient, times(0)).createTopics(any());
    verify(mockAdminClient, times(0)).createPartitions(any());
    verify(mockAdminClient, times(1)).describeConfigs(any(Collection.class));
    verify(mockAdminClient, times(1)).incrementalAlterConfigs(any(Map.class));
  }

  @Test
  public void testReconcileEmitsChangedDiffWhenBrokerValueDiffers() throws Exception {
    // Broker returns a different value than declared - exercises the
    // "~ key: old -> new" diff branch and the totalChanged accumulator.
    kafkaConfiguration.getSetup().setReconcileExistingTopicConfigs(true);

    CreateKafkaTopicsStep spyStep = spy(step);
    AdminClient mockAdminClient = mock(AdminClient.class);

    Set<String> existingTopics = new HashSet<>();
    existingTopics.add("DataHubUpgradeHistory_v1");
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(existingTopics);
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    TopicDescription mockDescription = mock(TopicDescription.class);
    TopicPartitionInfo partition = mock(TopicPartitionInfo.class);
    when(mockDescription.partitions()).thenReturn(Arrays.asList(partition));
    Map<String, TopicDescription> descMap = new HashMap<>();
    descMap.put("DataHubUpgradeHistory_v1", mockDescription);
    DescribeTopicsResult mockDescribeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<Map<String, TopicDescription>> descFuture = mock(KafkaFuture.class);
    when(descFuture.get()).thenReturn(descMap);
    when(mockDescribeResult.allTopicNames()).thenReturn(descFuture);
    when(mockAdminClient.describeTopics(anyList())).thenReturn(mockDescribeResult);

    // Broker holds retention.ms=604800000 but yaml declares -1
    ConfigResource topicResource =
        new ConfigResource(ConfigResource.Type.TOPIC, "DataHubUpgradeHistory_v1");
    Map<ConfigResource, Config> brokerConfigs = new HashMap<>();
    brokerConfigs.put(
        topicResource, new Config(Arrays.asList(new ConfigEntry("retention.ms", "604800000"))));
    DescribeConfigsResult mockDescribeConfigsResult = mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> describeConfigsFuture = mock(KafkaFuture.class);
    when(describeConfigsFuture.get()).thenReturn(brokerConfigs);
    when(mockDescribeConfigsResult.all()).thenReturn(describeConfigsFuture);
    when(mockAdminClient.describeConfigs(any(Collection.class)))
        .thenReturn(mockDescribeConfigsResult);

    AlterConfigsResult mockAlterResult = mock(AlterConfigsResult.class);
    KafkaFuture<Void> alterFuture = mock(KafkaFuture.class);
    when(alterFuture.get()).thenReturn(null);
    when(mockAlterResult.all()).thenReturn(alterFuture);
    when(mockAdminClient.incrementalAlterConfigs(any(Map.class))).thenReturn(mockAlterResult);

    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("DataHubUpgradeHistory_v1");
    topicConfig.setPartitions(1);
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(true);
    Map<String, String> configProps = new HashMap<>();
    configProps.put("retention.ms", "-1");
    topicConfig.setConfigProperties(configProps);
    topics.put("datahubUpgradeHistory", topicConfig);
    kafkaConfiguration.setTopics(topics);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockAdminClient, times(1)).describeConfigs(any(Collection.class));
    verify(mockAdminClient, times(1)).incrementalAlterConfigs(any(Map.class));
  }

  @Test
  public void testReconcileNoOpWhenBrokerAlreadyAligned() throws Exception {
    // When broker already holds the declared value, describeConfigs is consulted
    // but incrementalAlterConfigs must NOT be invoked.
    kafkaConfiguration.getSetup().setReconcileExistingTopicConfigs(true);

    CreateKafkaTopicsStep spyStep = spy(step);
    AdminClient mockAdminClient = mock(AdminClient.class);

    Set<String> existingTopics = new HashSet<>();
    existingTopics.add("DataHubUpgradeHistory_v1");
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(existingTopics);
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    TopicDescription mockDescription = mock(TopicDescription.class);
    TopicPartitionInfo partition = mock(TopicPartitionInfo.class);
    when(mockDescription.partitions()).thenReturn(Arrays.asList(partition));
    Map<String, TopicDescription> descMap = new HashMap<>();
    descMap.put("DataHubUpgradeHistory_v1", mockDescription);
    DescribeTopicsResult mockDescribeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<Map<String, TopicDescription>> descFuture = mock(KafkaFuture.class);
    when(descFuture.get()).thenReturn(descMap);
    when(mockDescribeResult.allTopicNames()).thenReturn(descFuture);
    when(mockAdminClient.describeTopics(anyList())).thenReturn(mockDescribeResult);

    // Broker already has retention.ms=-1 so the diff is empty
    ConfigResource topicResource =
        new ConfigResource(ConfigResource.Type.TOPIC, "DataHubUpgradeHistory_v1");
    Map<ConfigResource, Config> brokerConfigs = new HashMap<>();
    brokerConfigs.put(
        topicResource, new Config(Arrays.asList(new ConfigEntry("retention.ms", "-1"))));
    DescribeConfigsResult mockDescribeConfigsResult = mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> describeConfigsFuture = mock(KafkaFuture.class);
    when(describeConfigsFuture.get()).thenReturn(brokerConfigs);
    when(mockDescribeConfigsResult.all()).thenReturn(describeConfigsFuture);
    when(mockAdminClient.describeConfigs(any(Collection.class)))
        .thenReturn(mockDescribeConfigsResult);

    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("DataHubUpgradeHistory_v1");
    topicConfig.setPartitions(1);
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(true);
    Map<String, String> configProps = new HashMap<>();
    configProps.put("retention.ms", "-1");
    topicConfig.setConfigProperties(configProps);
    topics.put("datahubUpgradeHistory", topicConfig);
    kafkaConfiguration.setTopics(topics);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockAdminClient, times(1)).describeConfigs(any(Collection.class));
    verify(mockAdminClient, times(0)).incrementalAlterConfigs(any(Map.class));
  }

  @Test
  public void testNoReconcileWhenFlagDisabled() throws Exception {
    // Flag defaults to false; existing topic with declared configProperties
    // must NOT trigger incrementalAlterConfigs.
    CreateKafkaTopicsStep spyStep = spy(step);
    AdminClient mockAdminClient = mock(AdminClient.class);

    Set<String> existingTopics = new HashSet<>();
    existingTopics.add("DataHubUpgradeHistory_v1");
    ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> listTopicsFuture = mock(KafkaFuture.class);
    when(listTopicsFuture.get()).thenReturn(existingTopics);
    when(mockListTopicsResult.names()).thenReturn(listTopicsFuture);
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    TopicDescription mockDescription = mock(TopicDescription.class);
    TopicPartitionInfo partition = mock(TopicPartitionInfo.class);
    when(mockDescription.partitions()).thenReturn(Arrays.asList(partition));
    Map<String, TopicDescription> descMap = new HashMap<>();
    descMap.put("DataHubUpgradeHistory_v1", mockDescription);
    DescribeTopicsResult mockDescribeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<Map<String, TopicDescription>> descFuture = mock(KafkaFuture.class);
    when(descFuture.get()).thenReturn(descMap);
    when(mockDescribeResult.allTopicNames()).thenReturn(descFuture);
    when(mockAdminClient.describeTopics(anyList())).thenReturn(mockDescribeResult);

    doReturn(mockAdminClient).when(spyStep).createAdminClient();

    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("DataHubUpgradeHistory_v1");
    topicConfig.setPartitions(1);
    topicConfig.setReplicationFactor(1);
    topicConfig.setEnabled(true);
    Map<String, String> configProps = new HashMap<>();
    configProps.put("retention.ms", "-1");
    topicConfig.setConfigProperties(configProps);
    topics.put("datahubUpgradeHistory", topicConfig);
    kafkaConfiguration.setTopics(topics);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = spyStep.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockAdminClient, times(0)).incrementalAlterConfigs(any(Map.class));
  }
}
