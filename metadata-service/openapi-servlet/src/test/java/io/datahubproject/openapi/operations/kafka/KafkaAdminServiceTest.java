package io.datahubproject.openapi.operations.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.mxe.TopicConventionImpl;
import io.datahubproject.openapi.operations.kafka.models.KafkaClusterResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaConsumerGroupResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaMessageResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaTopicResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KafkaAdminServiceTest {

  private static final String MCP_TOPIC = "MetadataChangeProposal_v1";
  private static final String MCP_FAILED_TOPIC = "FailedMetadataChangeProposal_v1";
  private static final String MCL_VERSIONED_TOPIC = "MetadataChangeLog_Versioned_v1";
  private static final String TEST_TOPIC = "test-topic";
  private static final String TEST_GROUP = "test-group";
  private static final String DATAHUB_MCP_CONSUMER_GROUP = "datahub-mcp-consumer";
  private static final String DATAHUB_MCL_CONSUMER_GROUP = "datahub-mcl-consumer";

  @Mock private AdminClient adminClient;
  @Mock private Consumer<String, GenericRecord> consumer;
  @Mock private KafkaEventProducer kafkaEventProducer;

  private DataHubTopicConvention topicConvention;
  private KafkaAdminService kafkaAdminService;
  private Supplier<Consumer<String, GenericRecord>> consumerSupplier;

  @BeforeMethod(alwaysRun = true)
  public void setup() {
    MockitoAnnotations.openMocks(this);
    // Create topic convention with known consumer group IDs
    Set<String> knownConsumerGroups =
        Set.of(DATAHUB_MCP_CONSUMER_GROUP, DATAHUB_MCL_CONSUMER_GROUP);
    topicConvention = new DataHubTopicConvention(new TopicConventionImpl(), knownConsumerGroups);
    consumerSupplier = () -> consumer;

    kafkaAdminService =
        KafkaAdminService.builder()
            .adminClient(adminClient)
            .topicConvention(topicConvention)
            .kafkaEventProducer(kafkaEventProducer)
            .consumerSupplier(consumerSupplier)
            .build();
  }

  // ============================================================================
  // Cluster Operations Tests
  // ============================================================================

  @Test
  public void testGetClusterInfo() throws Exception {
    // Arrange
    Node controller = new Node(0, "controller-host", 9092, "rack-0");
    Node broker1 = new Node(1, "broker1-host", 9092, "rack-1");
    Node broker2 = new Node(2, "broker2-host", 9092, "rack-2");
    List<Node> nodes = List.of(controller, broker1, broker2);

    DescribeClusterResult clusterResult = mock(DescribeClusterResult.class);
    when(clusterResult.clusterId()).thenReturn(KafkaFuture.completedFuture("test-cluster-id"));
    when(clusterResult.controller()).thenReturn(KafkaFuture.completedFuture(controller));
    when(clusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(nodes));
    when(adminClient.describeCluster()).thenReturn(clusterResult);

    // Act
    KafkaClusterResponse result = kafkaAdminService.getClusterInfo();

    // Assert
    assertNotNull(result);
    assertEquals(result.getClusterId(), "test-cluster-id");
    assertEquals(result.getBrokerCount().intValue(), 3);
    assertNotNull(result.getController());
    assertEquals(result.getController().getId().intValue(), 0);
    assertTrue(result.getController().getIsController());
    assertEquals(result.getBrokers().size(), 3);
  }

  // ============================================================================
  // Topic Operations Tests
  // ============================================================================

  @Test
  public void testListTopics_FiltersToDataHubTopicsOnly() throws Exception {
    // Arrange - mix of DataHub topics, non-DataHub topics, and internal topics
    Set<String> topicNames = Set.of(MCP_TOPIC, MCL_VERSIONED_TOPIC, "random-topic", "__internal");
    ListTopicsResult listResult = mock(ListTopicsResult.class);
    when(listResult.names()).thenReturn(KafkaFuture.completedFuture(topicNames));
    when(adminClient.listTopics()).thenReturn(listResult);

    // Act - without internal topics
    List<String> result = kafkaAdminService.listTopics(false);

    // Assert - only DataHub topics are returned
    assertEquals(result.size(), 2);
    assertTrue(result.contains(MCP_TOPIC));
    assertTrue(result.contains(MCL_VERSIONED_TOPIC));
    assertFalse(result.contains("random-topic"));
    assertFalse(result.contains("__internal"));

    // Act - with internal topics
    List<String> resultWithInternal = kafkaAdminService.listTopics(true);

    // Assert - DataHub topics + internal topics
    assertEquals(resultWithInternal.size(), 3);
    assertTrue(resultWithInternal.contains(MCP_TOPIC));
    assertTrue(resultWithInternal.contains(MCL_VERSIONED_TOPIC));
    assertTrue(resultWithInternal.contains("__internal"));
    assertFalse(resultWithInternal.contains("random-topic"));
  }

  @Test
  public void testDescribeTopic() throws Exception {
    // Arrange - use a DataHub topic (MCP_TOPIC)
    Node leader = new Node(0, "leader-host", 9092);
    TopicPartitionInfo partitionInfo =
        new TopicPartitionInfo(0, leader, List.of(leader), List.of(leader));
    TopicDescription topicDescription =
        new TopicDescription(MCP_TOPIC, false, List.of(partitionInfo));

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    when(describeResult.topicNameValues())
        .thenReturn(Map.of(MCP_TOPIC, KafkaFuture.completedFuture(topicDescription)));
    when(adminClient.describeTopics(anyCollection())).thenReturn(describeResult);

    ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, MCP_TOPIC);
    Config config = new Config(List.of(new ConfigEntry("retention.ms", "604800000")));
    DescribeConfigsResult configsResult = mock(DescribeConfigsResult.class);
    when(configsResult.values())
        .thenReturn(Map.of(configResource, KafkaFuture.completedFuture(config)));
    when(adminClient.describeConfigs(anyCollection())).thenReturn(configsResult);

    // Act
    KafkaTopicResponse result = kafkaAdminService.describeTopic(MCP_TOPIC);

    // Assert
    assertNotNull(result);
    assertEquals(result.getName(), MCP_TOPIC);
    assertEquals(result.getPartitionCount().intValue(), 1);
    assertEquals(result.getReplicationFactor().intValue(), 1);
    assertFalse(result.getInternal());
    assertNotNull(result.getPartitions());
    assertEquals(result.getPartitions().size(), 1);
    assertNotNull(result.getConfigs());
  }

  @Test(expectedExceptions = KafkaAdminException.InvalidTopicException.class)
  public void testDescribeTopic_InvalidTopic() {
    // Act - try to describe a non-DataHub topic
    kafkaAdminService.describeTopic("random-topic");
  }

  @Test(expectedExceptions = KafkaAdminException.TopicNotFoundException.class)
  public void testDescribeTopic_NotFound() throws Exception {
    // Arrange - use a DataHub topic that doesn't exist in Kafka
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(
                new UnknownTopicOrPartitionException("Not found")));
    when(describeResult.topicNameValues()).thenReturn(Map.of(MCP_TOPIC, failedFuture));
    when(adminClient.describeTopics(anyCollection())).thenReturn(describeResult);

    // Act - use a valid DataHub topic that doesn't exist in Kafka
    kafkaAdminService.describeTopic(MCP_TOPIC);
  }

  @Test
  public void testCreateDataHubTopic() throws Exception {
    // Arrange
    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    when(createResult.all()).thenReturn(KafkaFuture.completedFuture(null));
    when(adminClient.createTopics(anyCollection())).thenReturn(createResult);

    // Setup describe for validation after create
    Node leader = new Node(0, "leader-host", 9092);
    TopicPartitionInfo partitionInfo =
        new TopicPartitionInfo(0, leader, List.of(leader), List.of(leader));
    TopicDescription topicDescription =
        new TopicDescription(MCP_TOPIC, false, List.of(partitionInfo));

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    when(describeResult.topicNameValues())
        .thenReturn(Map.of(MCP_TOPIC, KafkaFuture.completedFuture(topicDescription)));
    when(adminClient.describeTopics(anyCollection())).thenReturn(describeResult);

    ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, MCP_TOPIC);
    Config config = new Config(Collections.emptyList());
    DescribeConfigsResult configsResult = mock(DescribeConfigsResult.class);
    when(configsResult.values())
        .thenReturn(Map.of(configResource, KafkaFuture.completedFuture(config)));
    when(adminClient.describeConfigs(anyCollection())).thenReturn(configsResult);

    // Act
    KafkaTopicResponse result = kafkaAdminService.createDataHubTopic("mcp", 3, (short) 2, null);

    // Assert
    assertNotNull(result);
    verify(adminClient).createTopics(anyCollection());
  }

  @Test(expectedExceptions = KafkaAdminException.InvalidAliasException.class)
  public void testCreateDataHubTopic_InvalidAlias() {
    // Act
    kafkaAdminService.createDataHubTopic("invalid-alias", 1, null, null);
  }

  @Test(expectedExceptions = KafkaAdminException.TopicAlreadyExistsException.class)
  public void testCreateDataHubTopic_AlreadyExists() throws Exception {
    // Arrange
    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(new java.util.concurrent.ExecutionException(new TopicExistsException("Exists")));
    when(createResult.all()).thenReturn(failedFuture);
    when(adminClient.createTopics(anyCollection())).thenReturn(createResult);

    // Act
    kafkaAdminService.createDataHubTopic("mcp", 1, null, null);
  }

  @Test
  public void testDeleteDataHubTopic() throws Exception {
    // Arrange
    DeleteTopicsResult deleteResult = mock(DeleteTopicsResult.class);
    when(deleteResult.all()).thenReturn(KafkaFuture.completedFuture(null));
    when(adminClient.deleteTopics(anyCollection())).thenReturn(deleteResult);

    // Act
    kafkaAdminService.deleteDataHubTopic("mcp");

    // Assert
    verify(adminClient).deleteTopics(anyCollection());
  }

  // ============================================================================
  // Consumer Group Operations Tests
  // ============================================================================

  @Test
  public void testListConsumerGroups_FiltersToKnownGroupsOnly() throws Exception {
    // Arrange - mix of known DataHub groups and unknown groups
    ConsumerGroupListing group1 =
        new ConsumerGroupListing(TEST_GROUP, true, Optional.of(ConsumerGroupState.STABLE));
    ConsumerGroupListing group2 =
        new ConsumerGroupListing(
            DATAHUB_MCP_CONSUMER_GROUP, false, Optional.of(ConsumerGroupState.STABLE));
    ConsumerGroupListing group3 =
        new ConsumerGroupListing(
            DATAHUB_MCL_CONSUMER_GROUP, false, Optional.of(ConsumerGroupState.STABLE));

    ListConsumerGroupsResult listResult = mock(ListConsumerGroupsResult.class);
    when(listResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(group1, group2, group3)));
    when(adminClient.listConsumerGroups()).thenReturn(listResult);

    // Act
    List<KafkaConsumerGroupResponse.ConsumerGroupSummary> result =
        kafkaAdminService.listConsumerGroups();

    // Assert - only known DataHub consumer groups are returned
    assertEquals(result.size(), 2);
    assertTrue(result.stream().anyMatch(g -> g.getGroupId().equals(DATAHUB_MCP_CONSUMER_GROUP)));
    assertTrue(result.stream().anyMatch(g -> g.getGroupId().equals(DATAHUB_MCL_CONSUMER_GROUP)));
    assertFalse(result.stream().anyMatch(g -> g.getGroupId().equals(TEST_GROUP)));
  }

  @Test
  public void testDescribeConsumerGroup() throws Exception {
    // Arrange - use a known DataHub consumer group
    Node coordinator = new Node(0, "coordinator-host", 9092);
    TopicPartition tp = new TopicPartition(MCP_TOPIC, 0);

    MemberAssignment assignment = new MemberAssignment(Set.of(tp));
    MemberDescription member =
        new MemberDescription("member-1", Optional.empty(), "client-1", "host-1", assignment);

    ConsumerGroupDescription groupDescription =
        new ConsumerGroupDescription(
            DATAHUB_MCP_CONSUMER_GROUP,
            true,
            List.of(member),
            "range",
            ConsumerGroupState.STABLE,
            coordinator);

    DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
    when(describeResult.describedGroups())
        .thenReturn(
            Map.of(DATAHUB_MCP_CONSUMER_GROUP, KafkaFuture.completedFuture(groupDescription)));
    when(adminClient.describeConsumerGroups(anyCollection())).thenReturn(describeResult);

    // Mock offsets
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));
    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Mock end offsets
    ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
    ListOffsetsResult.ListOffsetsResultInfo offsetInfo =
        mock(ListOffsetsResult.ListOffsetsResultInfo.class);
    when(offsetInfo.offset()).thenReturn(150L);
    when(listOffsetsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(tp, offsetInfo)));
    when(adminClient.listOffsets(anyMap())).thenReturn(listOffsetsResult);

    // Act
    KafkaConsumerGroupResponse result =
        kafkaAdminService.describeConsumerGroup(DATAHUB_MCP_CONSUMER_GROUP);

    // Assert
    assertNotNull(result);
    assertEquals(result.getGroupId(), DATAHUB_MCP_CONSUMER_GROUP);
    assertEquals(result.getState(), "Stable");
    assertNotNull(result.getCoordinator());
    assertEquals(result.getMembers().size(), 1);
  }

  @Test(expectedExceptions = KafkaAdminException.InvalidConsumerGroupException.class)
  public void testDescribeConsumerGroup_InvalidGroup() {
    // Act - try to describe an unknown consumer group
    kafkaAdminService.describeConsumerGroup(TEST_GROUP);
  }

  @Test
  public void testDeleteConsumerGroup() throws Exception {
    // Arrange - use a known DataHub consumer group
    DeleteConsumerGroupsResult deleteResult = mock(DeleteConsumerGroupsResult.class);
    when(deleteResult.all()).thenReturn(KafkaFuture.completedFuture(null));
    when(adminClient.deleteConsumerGroups(anyCollection())).thenReturn(deleteResult);

    // Act
    kafkaAdminService.deleteConsumerGroup(DATAHUB_MCP_CONSUMER_GROUP);

    // Assert
    verify(adminClient).deleteConsumerGroups(anyCollection());
  }

  @Test(expectedExceptions = KafkaAdminException.InvalidConsumerGroupException.class)
  public void testDeleteConsumerGroup_InvalidGroup() {
    // Act - try to delete an unknown consumer group
    kafkaAdminService.deleteConsumerGroup(TEST_GROUP);
  }

  @Test(expectedExceptions = KafkaAdminException.ConsumerGroupNotEmptyException.class)
  public void testDeleteConsumerGroup_NotEmpty() throws Exception {
    // Arrange - use a known DataHub consumer group that is not empty
    DeleteConsumerGroupsResult deleteResult = mock(DeleteConsumerGroupsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new GroupNotEmptyException("Not empty")));
    when(deleteResult.all()).thenReturn(failedFuture);
    when(adminClient.deleteConsumerGroups(anyCollection())).thenReturn(deleteResult);

    // Act
    kafkaAdminService.deleteConsumerGroup(DATAHUB_MCP_CONSUMER_GROUP);
  }

  // ============================================================================
  // Message Operations Tests
  // ============================================================================

  @Test
  public void testGetMessages() throws Exception {
    // Arrange
    ConsumerRecords<String, GenericRecord> emptyRecords = mock(ConsumerRecords.class);
    when(emptyRecords.isEmpty()).thenReturn(true);
    when(consumer.poll(any(Duration.class))).thenReturn(emptyRecords);

    // Act
    KafkaMessageResponse result = kafkaAdminService.getMessages("mcp", 0, 0L, 10);

    // Assert
    assertNotNull(result);
    assertEquals(
        result.getTopic(),
        topicConvention.getTopicConvention().getMetadataChangeProposalTopicName());
    assertEquals(result.getDataHubAlias(), "mcp");
    assertNotNull(result.getMessages());
    verify(consumer).assign(anyList());
    verify(consumer).seek(any(TopicPartition.class), eq(0L));
  }

  @Test(expectedExceptions = KafkaAdminException.InvalidAliasException.class)
  public void testGetMessages_InvalidAlias() {
    // Act
    kafkaAdminService.getMessages("invalid-alias", 0, 0L, 10);
  }

  // ============================================================================
  // TopicConvention Integration Tests
  // ============================================================================

  @Test
  public void testTopicConventionAliases() {
    // Verify all aliases resolve correctly
    assertTrue(topicConvention.getTopicNameForAlias("mcp").isPresent());
    assertTrue(topicConvention.getTopicNameForAlias("mcp-failed").isPresent());
    assertTrue(topicConvention.getTopicNameForAlias("mcl-versioned").isPresent());
    assertTrue(topicConvention.getTopicNameForAlias("mcl-timeseries").isPresent());
    assertTrue(topicConvention.getTopicNameForAlias("platform-event").isPresent());
    assertTrue(topicConvention.getTopicNameForAlias("upgrade-history").isPresent());

    // Verify case insensitivity
    assertTrue(topicConvention.getTopicNameForAlias("MCP").isPresent());
    assertTrue(topicConvention.getTopicNameForAlias("MCP-FAILED").isPresent());

    // Verify invalid alias
    assertFalse(topicConvention.getTopicNameForAlias("invalid").isPresent());
    assertFalse(topicConvention.getTopicNameForAlias(null).isPresent());
  }

  @Test
  public void testTopicConventionReverseLoookup() {
    // Get the actual topic name
    String mcpTopic = topicConvention.getTopicConvention().getMetadataChangeProposalTopicName();

    // Verify reverse lookup
    Optional<String> alias = topicConvention.getAliasForTopicName(mcpTopic);
    assertTrue(alias.isPresent());
    assertEquals(alias.get(), "mcp");

    // Verify unknown topic
    assertFalse(topicConvention.getAliasForTopicName("unknown-topic").isPresent());
  }

  @Test
  public void testIsDataHubTopic() {
    assertTrue(
        topicConvention.isDataHubTopic(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName()));
    assertTrue(
        topicConvention.isDataHubTopic(
            topicConvention.getTopicConvention().getFailedMetadataChangeProposalTopicName()));
    assertTrue(
        topicConvention.isDataHubTopic(
            topicConvention.getTopicConvention().getMetadataChangeLogVersionedTopicName()));
    assertTrue(
        topicConvention.isDataHubTopic(
            topicConvention.getTopicConvention().getMetadataChangeLogTimeseriesTopicName()));

    assertFalse(topicConvention.isDataHubTopic("random-topic"));
    assertFalse(topicConvention.isDataHubTopic(null));
  }

  @Test
  public void testGetAllAliases() {
    Set<String> aliases = topicConvention.getAllAliases();
    assertEquals(aliases.size(), 6);
    assertTrue(aliases.contains("mcp"));
    assertTrue(aliases.contains("mcp-failed"));
    assertTrue(aliases.contains("mcl-versioned"));
    assertTrue(aliases.contains("mcl-timeseries"));
    assertTrue(aliases.contains("platform-event"));
    assertTrue(aliases.contains("upgrade-history"));
  }

  // ============================================================================
  // Alter Topic Config Tests
  // ============================================================================

  @Test
  public void testAlterDataHubTopicConfig() throws Exception {
    // Arrange - setup alter configs to succeed
    org.apache.kafka.clients.admin.AlterConfigsResult alterResult =
        mock(org.apache.kafka.clients.admin.AlterConfigsResult.class);
    when(alterResult.all()).thenReturn(KafkaFuture.completedFuture(null));
    when(adminClient.incrementalAlterConfigs(anyMap())).thenReturn(alterResult);

    // Setup describe for validation after alter
    Node leader = new Node(0, "leader-host", 9092);
    TopicPartitionInfo partitionInfo =
        new TopicPartitionInfo(0, leader, List.of(leader), List.of(leader));
    TopicDescription topicDescription =
        new TopicDescription(MCP_TOPIC, false, List.of(partitionInfo));

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    when(describeResult.topicNameValues())
        .thenReturn(Map.of(MCP_TOPIC, KafkaFuture.completedFuture(topicDescription)));
    when(adminClient.describeTopics(anyCollection())).thenReturn(describeResult);

    ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, MCP_TOPIC);
    Config config =
        new Config(List.of(new ConfigEntry("retention.ms", "86400000"))); // 1 day retention
    DescribeConfigsResult configsResult = mock(DescribeConfigsResult.class);
    when(configsResult.values())
        .thenReturn(Map.of(configResource, KafkaFuture.completedFuture(config)));
    when(adminClient.describeConfigs(anyCollection())).thenReturn(configsResult);

    // Act
    Map<String, String> newConfigs = new HashMap<>();
    newConfigs.put("retention.ms", "86400000");
    KafkaTopicResponse result = kafkaAdminService.alterDataHubTopicConfig("mcp", newConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(result.getName(), MCP_TOPIC);
    verify(adminClient).incrementalAlterConfigs(anyMap());
  }

  @Test(expectedExceptions = KafkaAdminException.InvalidAliasException.class)
  public void testAlterDataHubTopicConfig_InvalidAlias() {
    // Act
    kafkaAdminService.alterDataHubTopicConfig("invalid-alias", Map.of("retention.ms", "86400000"));
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testAlterDataHubTopicConfig_EmptyConfigs() {
    // Act
    kafkaAdminService.alterDataHubTopicConfig("mcp", Map.of());
  }

  // ============================================================================
  // Log Dirs Tests
  // ============================================================================

  @Test
  public void testGetLogDirs() throws Exception {
    // Arrange
    Node broker = new Node(0, "broker-host", 9092);
    DescribeClusterResult clusterResult = mock(DescribeClusterResult.class);
    when(clusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(broker)));
    when(adminClient.describeCluster()).thenReturn(clusterResult);

    // Mock log dir description
    TopicPartition tp = new TopicPartition(TEST_TOPIC, 0);
    org.apache.kafka.clients.admin.ReplicaInfo replicaInfo =
        new org.apache.kafka.clients.admin.ReplicaInfo(1024L, 0L, false);
    org.apache.kafka.clients.admin.LogDirDescription logDirDescription =
        new org.apache.kafka.clients.admin.LogDirDescription(null, Map.of(tp, replicaInfo));

    DescribeLogDirsResult logDirsResult = mock(DescribeLogDirsResult.class);
    when(logDirsResult.allDescriptions())
        .thenReturn(
            KafkaFuture.completedFuture(Map.of(0, Map.of("/var/kafka-logs", logDirDescription))));
    when(adminClient.describeLogDirs(anyCollection())).thenReturn(logDirsResult);

    // Act
    var result = kafkaAdminService.getLogDirs(null);

    // Assert
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getBrokerId().intValue(), 0);
    assertNotNull(result.get(0).getLogDirs());
    assertEquals(result.get(0).getLogDirs().size(), 1);
    assertEquals(result.get(0).getLogDirs().get(0).getPath(), "/var/kafka-logs");
  }

  @Test
  public void testGetLogDirs_SpecificBroker() throws Exception {
    // Arrange - mock for specific broker
    TopicPartition tp = new TopicPartition(TEST_TOPIC, 0);
    org.apache.kafka.clients.admin.ReplicaInfo replicaInfo =
        new org.apache.kafka.clients.admin.ReplicaInfo(2048L, 10L, false);
    org.apache.kafka.clients.admin.LogDirDescription logDirDescription =
        new org.apache.kafka.clients.admin.LogDirDescription(null, Map.of(tp, replicaInfo));

    DescribeLogDirsResult logDirsResult = mock(DescribeLogDirsResult.class);
    when(logDirsResult.allDescriptions())
        .thenReturn(
            KafkaFuture.completedFuture(Map.of(1, Map.of("/data/kafka", logDirDescription))));
    when(adminClient.describeLogDirs(anyCollection())).thenReturn(logDirsResult);

    // Act
    var result = kafkaAdminService.getLogDirs(Set.of(1));

    // Assert
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getBrokerId().intValue(), 1);
    assertEquals(result.get(0).getTotalSizeBytes().longValue(), 2048L);
  }

  // ============================================================================
  // Delete Topic Tests
  // ============================================================================

  @Test(expectedExceptions = KafkaAdminException.TopicNotFoundException.class)
  public void testDeleteDataHubTopic_NotFound() throws Exception {
    // Arrange
    DeleteTopicsResult deleteResult = mock(DeleteTopicsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(
                new UnknownTopicOrPartitionException("Not found")));
    when(deleteResult.all()).thenReturn(failedFuture);
    when(adminClient.deleteTopics(anyCollection())).thenReturn(deleteResult);

    // Act
    kafkaAdminService.deleteDataHubTopic("mcp");
  }

  @Test(expectedExceptions = KafkaAdminException.InvalidAliasException.class)
  public void testDeleteDataHubTopic_InvalidAlias() {
    // Act
    kafkaAdminService.deleteDataHubTopic("invalid-alias");
  }

  // ============================================================================
  // Consumer Group Not Found Tests
  // ============================================================================

  @Test(expectedExceptions = KafkaAdminException.ConsumerGroupNotFoundException.class)
  public void testDescribeConsumerGroup_NotFound() throws Exception {
    // Arrange - use a known DataHub consumer group that doesn't exist in Kafka
    DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
    KafkaFuture<ConsumerGroupDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(
                new org.apache.kafka.common.errors.GroupIdNotFoundException("Not found")));
    when(describeResult.describedGroups())
        .thenReturn(Map.of(DATAHUB_MCP_CONSUMER_GROUP, failedFuture));
    when(adminClient.describeConsumerGroups(anyCollection())).thenReturn(describeResult);

    // Act
    kafkaAdminService.describeConsumerGroup(DATAHUB_MCP_CONSUMER_GROUP);
  }

  // ============================================================================
  // Reset Offsets Tests
  // ============================================================================

  @Test
  public void testResetConsumerGroupOffsets_DryRun() throws Exception {
    // Arrange - use a known DataHub consumer group
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Mock earliest offset lookup
    ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
    ListOffsetsResult.ListOffsetsResultInfo offsetInfo =
        mock(ListOffsetsResult.ListOffsetsResultInfo.class);
    when(offsetInfo.offset()).thenReturn(0L);
    when(listOffsetsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(tp, offsetInfo)));
    when(adminClient.listOffsets(anyMap())).thenReturn(listOffsetsResult);

    // Act - reset offsets for mcp topic
    List<KafkaConsumerGroupResponse.PartitionResetInfo> result =
        kafkaAdminService.resetConsumerGroupOffsets(
            DATAHUB_MCP_CONSUMER_GROUP, "mcp", "earliest", null, null, true);

    // Assert
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getPreviousOffset().longValue(), 100L);
    assertEquals(result.get(0).getNewOffset().longValue(), 0L);

    // Verify consumer.commitSync was NOT called (dry run)
    verify(consumer, never()).commitSync(anyMap());
  }

  @Test(expectedExceptions = KafkaAdminException.InvalidConsumerGroupException.class)
  public void testResetConsumerGroupOffsets_InvalidGroup() {
    // Act - try to reset offsets for an unknown consumer group
    kafkaAdminService.resetConsumerGroupOffsets(TEST_GROUP, "mcp", "earliest", null, null, true);
  }

  @Test(expectedExceptions = KafkaAdminException.InvalidAliasException.class)
  public void testResetConsumerGroupOffsets_InvalidTopic() {
    // Act - try to reset offsets with an invalid topic alias
    kafkaAdminService.resetConsumerGroupOffsets(
        DATAHUB_MCP_CONSUMER_GROUP, "invalid-topic", "earliest", null, null, true);
  }

  @Test
  public void testResetConsumerGroupOffsets_LatestStrategy() throws Exception {
    // Arrange
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Mock latest offset lookup
    ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
    ListOffsetsResult.ListOffsetsResultInfo offsetInfo =
        mock(ListOffsetsResult.ListOffsetsResultInfo.class);
    when(offsetInfo.offset()).thenReturn(500L);
    when(listOffsetsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(tp, offsetInfo)));
    when(adminClient.listOffsets(anyMap())).thenReturn(listOffsetsResult);

    // Act
    List<KafkaConsumerGroupResponse.PartitionResetInfo> result =
        kafkaAdminService.resetConsumerGroupOffsets(
            DATAHUB_MCP_CONSUMER_GROUP, "mcp", "latest", null, null, true);

    // Assert
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getPreviousOffset().longValue(), 100L);
    assertEquals(result.get(0).getNewOffset().longValue(), 500L);
  }

  @Test
  public void testResetConsumerGroupOffsets_ToOffsetStrategy() throws Exception {
    // Arrange
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Act - reset to specific offset 250
    List<KafkaConsumerGroupResponse.PartitionResetInfo> result =
        kafkaAdminService.resetConsumerGroupOffsets(
            DATAHUB_MCP_CONSUMER_GROUP, "mcp", "to-offset", "250", null, true);

    // Assert
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getPreviousOffset().longValue(), 100L);
    assertEquals(result.get(0).getNewOffset().longValue(), 250L);
  }

  @Test
  public void testResetConsumerGroupOffsets_ShiftByPositive() throws Exception {
    // Arrange
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Act - shift forward by 50
    List<KafkaConsumerGroupResponse.PartitionResetInfo> result =
        kafkaAdminService.resetConsumerGroupOffsets(
            DATAHUB_MCP_CONSUMER_GROUP, "mcp", "shift-by", "50", null, true);

    // Assert
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getPreviousOffset().longValue(), 100L);
    assertEquals(result.get(0).getNewOffset().longValue(), 150L);
  }

  @Test
  public void testResetConsumerGroupOffsets_ShiftByNegative() throws Exception {
    // Arrange
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Act - shift backward by 50
    List<KafkaConsumerGroupResponse.PartitionResetInfo> result =
        kafkaAdminService.resetConsumerGroupOffsets(
            DATAHUB_MCP_CONSUMER_GROUP, "mcp", "shift-by", "-50", null, true);

    // Assert
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getPreviousOffset().longValue(), 100L);
    assertEquals(result.get(0).getNewOffset().longValue(), 50L);
  }

  @Test
  public void testResetConsumerGroupOffsets_ShiftByNegativeClampsToZero() throws Exception {
    // Arrange
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(50L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Act - shift backward by 100 (more than current offset)
    List<KafkaConsumerGroupResponse.PartitionResetInfo> result =
        kafkaAdminService.resetConsumerGroupOffsets(
            DATAHUB_MCP_CONSUMER_GROUP, "mcp", "shift-by", "-100", null, true);

    // Assert - should clamp to 0
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getPreviousOffset().longValue(), 50L);
    assertEquals(result.get(0).getNewOffset().longValue(), 0L);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testResetConsumerGroupOffsets_InvalidStrategy() throws Exception {
    // Arrange
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Act - use invalid strategy (wrapped in KafkaAdminException)
    kafkaAdminService.resetConsumerGroupOffsets(
        DATAHUB_MCP_CONSUMER_GROUP, "mcp", "invalid-strategy", null, null, true);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testResetConsumerGroupOffsets_ToOffsetMissingValue() throws Exception {
    // Arrange
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Act - to-offset without value (wrapped in KafkaAdminException)
    kafkaAdminService.resetConsumerGroupOffsets(
        DATAHUB_MCP_CONSUMER_GROUP, "mcp", "to-offset", null, null, true);
  }

  // ============================================================================
  // Replay Failed MCPs Tests
  // ============================================================================

  @Test
  public void testReplayFailedMCPs_DryRun() throws Exception {
    // Arrange
    String mcpFailedTopic =
        topicConvention.getTopicConvention().getFailedMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpFailedTopic, 0);

    // Create a mock failed MCP record
    GenericRecord mockRecord = mock(GenericRecord.class);
    when(mockRecord.toString()).thenReturn("{\"mock\": \"record\"}");

    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>(mcpFailedTopic, 0, 100L, "key1", mockRecord);
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());

    // Act
    KafkaMessageResponse.ReplayResponse result =
        kafkaAdminService.replayFailedMCPs(0, 100L, 100L, 10, true);

    // Assert
    assertNotNull(result);
    assertTrue(result.getDryRun());
    assertEquals(result.getPartition().intValue(), 0);
    assertEquals(result.getStartOffset().longValue(), 100L);
    assertEquals(result.getSuccessCount().intValue(), 1);
    assertEquals(result.getFailureCount().intValue(), 0);
    assertEquals(result.getMessages().size(), 1);
    assertTrue(result.getMessages().get(0).getSuccess());

    // Verify producer was NOT called (dry run)
    verify(kafkaEventProducer, never()).produceMetadataChangeProposal(any(), any());
  }

  @Test
  public void testReplayFailedMCPs_NoMessagesInRange() throws Exception {
    // Arrange - return empty records
    when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

    // Act
    KafkaMessageResponse.ReplayResponse result =
        kafkaAdminService.replayFailedMCPs(0, 100L, 100L, 10, true);

    // Assert
    assertNotNull(result);
    assertEquals(result.getSuccessCount().intValue(), 0);
    assertEquals(result.getFailureCount().intValue(), 0);
    assertTrue(result.getMessages().isEmpty());
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testReplayFailedMCPs_NoProducerConfigured() {
    // Arrange - create a service without a producer
    KafkaAdminService serviceWithoutProducer =
        KafkaAdminService.builder()
            .adminClient(adminClient)
            .topicConvention(topicConvention)
            .kafkaEventProducer(null) // No producer
            .consumerSupplier(consumerSupplier)
            .build();

    // Act - should throw because producer is not configured
    serviceWithoutProducer.replayFailedMCPs(0, 100L, 100L, 10, false);
  }

  // ============================================================================
  // Get Messages Tests
  // ============================================================================

  @Test
  public void testGetMessages_WithMessages() throws Exception {
    // Arrange
    String mcpTopic = topicConvention.getTopicConvention().getMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpTopic, 0);

    GenericRecord mockRecord = mock(GenericRecord.class);
    when(mockRecord.toString()).thenReturn("{\"test\": \"value\"}");

    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>(mcpTopic, 0, 50L, "testKey", mockRecord);
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());
    when(consumer.position(any(TopicPartition.class))).thenReturn(51L);

    // Act
    KafkaMessageResponse result = kafkaAdminService.getMessages("mcp", 0, 50L, 10);

    // Assert
    assertNotNull(result);
    assertEquals(result.getTopic(), mcpTopic);
    assertEquals(result.getDataHubAlias(), "mcp");
    assertEquals(result.getCount().intValue(), 1);
    assertFalse(result.getHasMore());
    assertEquals(result.getMessages().size(), 1);
    assertEquals(result.getMessages().get(0).getKey(), "testKey");
    assertEquals(result.getMessages().get(0).getOffset().longValue(), 50L);
  }

  @Test
  public void testGetMessages_EmptyPartition() throws Exception {
    // Arrange - return empty records
    when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
    when(consumer.position(any(TopicPartition.class))).thenReturn(0L);

    // Act
    KafkaMessageResponse result = kafkaAdminService.getMessages("mcp", 0, 0L, 10);

    // Assert
    assertNotNull(result);
    assertEquals(result.getCount().intValue(), 0);
    assertFalse(result.getHasMore());
    assertTrue(result.getMessages().isEmpty());
  }

  @Test
  public void testGetMessages_ReplayableForMcpFailed() throws Exception {
    // Arrange
    String mcpFailedTopic =
        topicConvention.getTopicConvention().getFailedMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpFailedTopic, 0);

    GenericRecord mockRecord = mock(GenericRecord.class);
    when(mockRecord.toString()).thenReturn("{\"failed\": \"mcp\"}");

    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>(mcpFailedTopic, 0, 100L, "key1", mockRecord);
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());
    when(consumer.position(any(TopicPartition.class))).thenReturn(101L);

    // Act
    KafkaMessageResponse result = kafkaAdminService.getMessages("mcp-failed", 0, 100L, 10);

    // Assert
    assertNotNull(result);
    assertEquals(result.getMessages().size(), 1);
    assertTrue(result.getMessages().get(0).getReplayable()); // mcp-failed messages are replayable
  }

  @Test
  public void testGetMessages_JsonFormat() throws Exception {
    // Arrange
    String mcpTopic = topicConvention.getTopicConvention().getMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpTopic, 0);

    GenericRecord mockRecord = mock(GenericRecord.class);
    when(mockRecord.toString()).thenReturn("{\"test\": \"value\"}");

    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>(mcpTopic, 0, 50L, "testKey", mockRecord);
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());
    when(consumer.position(any(TopicPartition.class))).thenReturn(51L);

    // Act
    KafkaMessageResponse result = kafkaAdminService.getMessages("mcp", 0, 50L, 10, "json", false);

    // Assert
    assertNotNull(result);
    assertEquals(result.getMessages().size(), 1);
    assertEquals(result.getMessages().get(0).getFormat(), "json");
    assertEquals(result.getMessages().get(0).getValue(), "{\"test\": \"value\"}");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetMessages_AvroFormat() throws Exception {
    // Arrange
    String mcpTopic = topicConvention.getTopicConvention().getMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpTopic, 0);

    GenericRecord mockRecord = mock(GenericRecord.class);
    org.apache.avro.Schema mockSchema = mock(org.apache.avro.Schema.class);
    when(mockRecord.toString()).thenReturn("{\"test\": \"value\"}");
    when(mockRecord.getSchema()).thenReturn(mockSchema);
    when(mockSchema.toString(true)).thenReturn("{\"type\":\"record\",\"name\":\"Test\"}");

    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>(mcpTopic, 0, 50L, "testKey", mockRecord);
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());
    when(consumer.position(any(TopicPartition.class))).thenReturn(51L);

    // Act
    KafkaMessageResponse result = kafkaAdminService.getMessages("mcp", 0, 50L, 10, "avro", false);

    // Assert
    assertNotNull(result);
    assertEquals(result.getMessages().size(), 1);
    assertEquals(result.getMessages().get(0).getFormat(), "avro");
    // Value should be a Map with schema and data
    Object value = result.getMessages().get(0).getValue();
    assertTrue(value instanceof Map);
    Map<String, Object> avroValue = (Map<String, Object>) value;
    assertNotNull(avroValue.get("schema"));
    assertNotNull(avroValue.get("data"));
  }

  @Test
  public void testGetMessages_RawFormat() throws Exception {
    // Arrange
    String mcpTopic = topicConvention.getTopicConvention().getMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpTopic, 0);

    GenericRecord mockRecord = mock(GenericRecord.class);
    when(mockRecord.toString()).thenReturn("{\"test\": \"value\"}");
    // For raw format, when encoding fails it falls back to toString
    // We can't easily mock the Avro binary encoding, so we test the fallback path

    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>(mcpTopic, 0, 50L, "testKey", mockRecord);
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());
    when(consumer.position(any(TopicPartition.class))).thenReturn(51L);

    // Act
    KafkaMessageResponse result = kafkaAdminService.getMessages("mcp", 0, 50L, 10, "raw", false);

    // Assert
    assertNotNull(result);
    assertEquals(result.getMessages().size(), 1);
    assertEquals(result.getMessages().get(0).getFormat(), "raw");
    // Value should be either base64 encoded or fallback string
    assertNotNull(result.getMessages().get(0).getValue());
  }

  @Test
  public void testGetMessages_NullValue() throws Exception {
    // Arrange
    String mcpTopic = topicConvention.getTopicConvention().getMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpTopic, 0);

    // Create record with null value
    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>(mcpTopic, 0, 50L, "testKey", null);
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());
    when(consumer.position(any(TopicPartition.class))).thenReturn(51L);

    // Act
    KafkaMessageResponse result = kafkaAdminService.getMessages("mcp", 0, 50L, 10, "json", false);

    // Assert
    assertNotNull(result);
    assertEquals(result.getMessages().size(), 1);
    assertTrue(result.getMessages().get(0).getValue() == null);
  }

  @Test
  public void testGetMessages_WithHeaders() throws Exception {
    // Arrange
    String mcpTopic = topicConvention.getTopicConvention().getMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpTopic, 0);

    GenericRecord mockRecord = mock(GenericRecord.class);
    when(mockRecord.toString()).thenReturn("{\"test\": \"value\"}");

    // Create record with headers
    org.apache.kafka.common.header.Headers headers =
        new org.apache.kafka.common.header.internals.RecordHeaders();
    headers.add("correlation-id", "abc123".getBytes());
    headers.add("source", "test".getBytes());

    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>(
            mcpTopic,
            0,
            50L,
            1234567890L,
            org.apache.kafka.common.record.TimestampType.CREATE_TIME,
            0,
            0,
            "testKey",
            mockRecord,
            headers,
            Optional.empty());
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());
    when(consumer.position(any(TopicPartition.class))).thenReturn(51L);

    // Act
    KafkaMessageResponse result = kafkaAdminService.getMessages("mcp", 0, 50L, 10, "json", true);

    // Assert
    assertNotNull(result);
    assertEquals(result.getMessages().size(), 1);
    assertNotNull(result.getMessages().get(0).getHeaders());
    assertEquals(result.getMessages().get(0).getHeaders().size(), 2);
    assertEquals(result.getMessages().get(0).getHeaders().get(0).getKey(), "correlation-id");
    assertEquals(result.getMessages().get(0).getHeaders().get(0).getValue(), "abc123");
  }

  @Test
  public void testGetMessages_WithoutHeaders() throws Exception {
    // Arrange
    String mcpTopic = topicConvention.getTopicConvention().getMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpTopic, 0);

    GenericRecord mockRecord = mock(GenericRecord.class);
    when(mockRecord.toString()).thenReturn("{\"test\": \"value\"}");

    // Create record with headers but don't request them
    org.apache.kafka.common.header.Headers headers =
        new org.apache.kafka.common.header.internals.RecordHeaders();
    headers.add("correlation-id", "abc123".getBytes());

    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>(
            mcpTopic,
            0,
            50L,
            1234567890L,
            org.apache.kafka.common.record.TimestampType.CREATE_TIME,
            0,
            0,
            "testKey",
            mockRecord,
            headers,
            Optional.empty());
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());
    when(consumer.position(any(TopicPartition.class))).thenReturn(51L);

    // Act - includeHeaders = false
    KafkaMessageResponse result = kafkaAdminService.getMessages("mcp", 0, 50L, 10, "json", false);

    // Assert - headers should be null when not requested
    assertNotNull(result);
    assertEquals(result.getMessages().size(), 1);
    assertTrue(
        result.getMessages().get(0).getHeaders() == null
            || result.getMessages().get(0).getHeaders().isEmpty());
  }

  // ============================================================================
  // Additional Coverage Tests
  // ============================================================================

  @Test
  public void testResetConsumerGroupOffsets_ActualCommit() throws Exception {
    // Arrange
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Mock alterConsumerGroupOffsets for actual commit
    org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult alterResult =
        mock(org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult.class);
    when(alterResult.all()).thenReturn(KafkaFuture.completedFuture(null));
    when(adminClient.alterConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP), anyMap()))
        .thenReturn(alterResult);

    // Act - dryRun = false (actual commit)
    List<KafkaConsumerGroupResponse.PartitionResetInfo> result =
        kafkaAdminService.resetConsumerGroupOffsets(
            DATAHUB_MCP_CONSUMER_GROUP, "mcp", "to-offset", "200", null, false);

    // Assert
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getPreviousOffset().longValue(), 100L);
    assertEquals(result.get(0).getNewOffset().longValue(), 200L);

    // Verify alterConsumerGroupOffsets was called
    verify(adminClient).alterConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP), anyMap());
  }

  @Test
  public void testResetConsumerGroupOffsets_WithPartitionFilter() throws Exception {
    // Arrange - multiple partitions, filter to just partition 0
    TopicPartition tp0 =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    TopicPartition tp1 =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 1);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp0, new OffsetAndMetadata(100L));
    offsets.put(tp1, new OffsetAndMetadata(200L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Act - filter to only partition 0
    List<KafkaConsumerGroupResponse.PartitionResetInfo> result =
        kafkaAdminService.resetConsumerGroupOffsets(
            DATAHUB_MCP_CONSUMER_GROUP, "mcp", "to-offset", "50", List.of(0), true);

    // Assert - only partition 0 should be in the result
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getPartition().intValue(), 0);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testResetConsumerGroupOffsets_NoPartitionsForTopic() throws Exception {
    // Arrange - consumer group has offsets for a different topic
    TopicPartition tp = new TopicPartition("other-topic", 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Act - should throw because no partitions match the requested topic
    kafkaAdminService.resetConsumerGroupOffsets(
        DATAHUB_MCP_CONSUMER_GROUP, "mcp", "earliest", null, null, true);
  }

  @Test
  public void testDeleteConsumerGroup_Success() throws Exception {
    // Arrange
    DeleteConsumerGroupsResult deleteResult = mock(DeleteConsumerGroupsResult.class);
    when(deleteResult.all()).thenReturn(KafkaFuture.completedFuture(null));
    when(adminClient.deleteConsumerGroups(anyCollection())).thenReturn(deleteResult);

    // Act
    kafkaAdminService.deleteConsumerGroup(DATAHUB_MCP_CONSUMER_GROUP);

    // Assert
    verify(adminClient).deleteConsumerGroups(anyCollection());
  }

  @Test
  public void testListTopics_WithInternalTopics() throws Exception {
    // Arrange - include internal topics
    ListTopicsResult listResult = mock(ListTopicsResult.class);
    Set<String> topicNames =
        Set.of(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(),
            "__consumer_offsets");
    when(listResult.names()).thenReturn(KafkaFuture.completedFuture(topicNames));
    when(adminClient.listTopics()).thenReturn(listResult);

    // Act - include internal topics
    List<String> result = kafkaAdminService.listTopics(true);

    // Assert - should include internal topic when requested
    assertNotNull(result);
    assertTrue(result.contains("__consumer_offsets"));
  }

  @Test
  public void testListTopics_WithoutInternalTopics() throws Exception {
    // Arrange
    ListTopicsResult listResult = mock(ListTopicsResult.class);
    Set<String> topicNames =
        Set.of(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(),
            "__consumer_offsets");
    when(listResult.names()).thenReturn(KafkaFuture.completedFuture(topicNames));
    when(adminClient.listTopics()).thenReturn(listResult);

    // Act - exclude internal topics
    List<String> result = kafkaAdminService.listTopics(false);

    // Assert - should not include internal topic
    assertNotNull(result);
    assertFalse(result.contains("__consumer_offsets"));
  }

  @Test
  public void testCreateDataHubTopic_TopicAlreadyExists() throws Exception {
    // Arrange
    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(anyLong(), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(
                new TopicExistsException("Topic already exists")));
    when(createResult.all()).thenReturn(failedFuture);
    when(adminClient.createTopics(anyCollection())).thenReturn(createResult);

    // Act & Assert - should throw
    try {
      kafkaAdminService.createDataHubTopic("mcp", 3, (short) 1, Collections.emptyMap());
      assertTrue(false, "Expected KafkaAdminException");
    } catch (KafkaAdminException e) {
      assertTrue(e.getMessage().contains("already exists"));
    }
  }

  @Test
  public void testDeleteDataHubTopic_Success() throws Exception {
    // Arrange
    DeleteTopicsResult deleteResult = mock(DeleteTopicsResult.class);
    when(deleteResult.all()).thenReturn(KafkaFuture.completedFuture(null));
    when(adminClient.deleteTopics(anyCollection())).thenReturn(deleteResult);

    // Act
    kafkaAdminService.deleteDataHubTopic("mcp");

    // Assert
    verify(adminClient).deleteTopics(anyCollection());
  }

  @Test
  public void testDeleteDataHubTopic_UnknownTopic() throws Exception {
    // Arrange
    DeleteTopicsResult deleteResult = mock(DeleteTopicsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(anyLong(), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(
                new UnknownTopicOrPartitionException("Topic not found")));
    when(deleteResult.all()).thenReturn(failedFuture);
    when(adminClient.deleteTopics(anyCollection())).thenReturn(deleteResult);

    // Act & Assert
    try {
      kafkaAdminService.deleteDataHubTopic("mcp");
      assertTrue(false, "Expected KafkaAdminException");
    } catch (KafkaAdminException e) {
      assertTrue(e.getMessage().contains("not found") || e.getMessage().contains("Failed"));
    }
  }

  @Test
  public void testListConsumerGroups_FiltersNonDataHubGroups() throws Exception {
    // Arrange - mix of DataHub and non-DataHub groups
    ListConsumerGroupsResult listResult = mock(ListConsumerGroupsResult.class);
    ConsumerGroupListing datahubGroup = new ConsumerGroupListing(DATAHUB_MCP_CONSUMER_GROUP, false);
    ConsumerGroupListing otherGroup = new ConsumerGroupListing("other-consumer-group", false);
    when(listResult.all())
        .thenReturn(KafkaFuture.completedFuture(List.of(datahubGroup, otherGroup)));
    when(adminClient.listConsumerGroups()).thenReturn(listResult);

    // Act
    List<KafkaConsumerGroupResponse.ConsumerGroupSummary> result =
        kafkaAdminService.listConsumerGroups();

    // Assert - should only include DataHub consumer group
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getGroupId(), DATAHUB_MCP_CONSUMER_GROUP);
  }

  @Test
  public void testReplayFailedMCPs_OffsetOutOfRange() throws Exception {
    // Arrange - record with offset outside the requested range
    String mcpFailedTopic =
        topicConvention.getTopicConvention().getFailedMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpFailedTopic, 0);

    GenericRecord mockRecord = mock(GenericRecord.class);
    when(mockRecord.toString()).thenReturn("{\"mock\": \"record\"}");

    // Record at offset 50, but we request range 100-110
    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>(mcpFailedTopic, 0, 50L, "key1", mockRecord);
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());

    // Act - request offset range 100-110, but record is at 50
    KafkaMessageResponse.ReplayResponse result =
        kafkaAdminService.replayFailedMCPs(0, 100L, 110L, 10, true);

    // Assert - no messages should be replayed (offset 50 is outside range 100-110)
    assertNotNull(result);
    assertEquals(result.getSuccessCount().intValue(), 0);
    assertEquals(result.getMessages().size(), 0);
  }

  @Test
  public void testReplayFailedMCPs_MultipleMessages() throws Exception {
    // Arrange - multiple records
    String mcpFailedTopic =
        topicConvention.getTopicConvention().getFailedMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpFailedTopic, 0);

    GenericRecord mockRecord1 = mock(GenericRecord.class);
    GenericRecord mockRecord2 = mock(GenericRecord.class);
    when(mockRecord1.toString()).thenReturn("{\"msg\": \"1\"}");
    when(mockRecord2.toString()).thenReturn("{\"msg\": \"2\"}");

    ConsumerRecord<String, GenericRecord> record1 =
        new ConsumerRecord<>(mcpFailedTopic, 0, 100L, "key1", mockRecord1);
    ConsumerRecord<String, GenericRecord> record2 =
        new ConsumerRecord<>(mcpFailedTopic, 0, 101L, "key2", mockRecord2);
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record1, record2)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());

    // Act
    KafkaMessageResponse.ReplayResponse result =
        kafkaAdminService.replayFailedMCPs(0, 100L, 105L, 10, true);

    // Assert
    assertNotNull(result);
    assertEquals(result.getSuccessCount().intValue(), 2);
    assertEquals(result.getMessages().size(), 2);
  }

  @Test
  public void testReplayFailedMCPs_MaxCountLimit() throws Exception {
    // Arrange - multiple records but limited by maxCount
    String mcpFailedTopic =
        topicConvention.getTopicConvention().getFailedMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpFailedTopic, 0);

    GenericRecord mockRecord1 = mock(GenericRecord.class);
    GenericRecord mockRecord2 = mock(GenericRecord.class);
    when(mockRecord1.toString()).thenReturn("{\"msg\": \"1\"}");
    when(mockRecord2.toString()).thenReturn("{\"msg\": \"2\"}");

    ConsumerRecord<String, GenericRecord> record1 =
        new ConsumerRecord<>(mcpFailedTopic, 0, 100L, "key1", mockRecord1);
    ConsumerRecord<String, GenericRecord> record2 =
        new ConsumerRecord<>(mcpFailedTopic, 0, 101L, "key2", mockRecord2);
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record1, record2)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());

    // Act - maxCount = 1
    KafkaMessageResponse.ReplayResponse result =
        kafkaAdminService.replayFailedMCPs(0, 100L, 105L, 1, true);

    // Assert - only 1 message due to maxCount
    assertNotNull(result);
    assertEquals(result.getSuccessCount().intValue(), 1);
    assertEquals(result.getMessages().size(), 1);
  }

  @Test
  public void testReplayFailedMCPs_WrongPartition() throws Exception {
    // Arrange - record from wrong partition
    String mcpFailedTopic =
        topicConvention.getTopicConvention().getFailedMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpFailedTopic, 1); // partition 1

    GenericRecord mockRecord = mock(GenericRecord.class);
    when(mockRecord.toString()).thenReturn("{\"mock\": \"record\"}");

    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>(mcpFailedTopic, 1, 100L, "key1", mockRecord); // partition 1
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());

    // Act - request partition 0
    KafkaMessageResponse.ReplayResponse result =
        kafkaAdminService.replayFailedMCPs(0, 100L, 100L, 10, true);

    // Assert - no messages (wrong partition filtered out)
    assertNotNull(result);
    assertEquals(result.getSuccessCount().intValue(), 0);
  }

  @Test
  public void testGetMessages_HasMoreFlag() throws Exception {
    // Arrange - return more records than count to trigger hasMore
    String mcpTopic = topicConvention.getTopicConvention().getMetadataChangeProposalTopicName();
    TopicPartition tp = new TopicPartition(mcpTopic, 0);

    GenericRecord mockRecord1 = mock(GenericRecord.class);
    GenericRecord mockRecord2 = mock(GenericRecord.class);
    when(mockRecord1.toString()).thenReturn("{\"test\": \"value1\"}");
    when(mockRecord2.toString()).thenReturn("{\"test\": \"value2\"}");

    ConsumerRecord<String, GenericRecord> record1 =
        new ConsumerRecord<>(mcpTopic, 0, 50L, "key1", mockRecord1);
    ConsumerRecord<String, GenericRecord> record2 =
        new ConsumerRecord<>(mcpTopic, 0, 51L, "key2", mockRecord2);
    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record1, record2)));

    when(consumer.poll(any(Duration.class)))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());
    when(consumer.position(any(TopicPartition.class))).thenReturn(52L);

    // Act - count = 1, but 2 records available - should trigger hasMore
    KafkaMessageResponse result = kafkaAdminService.getMessages("mcp", 0, 50L, 1);

    // Assert - hasMore should be true since there's more data after count=1
    assertNotNull(result);
    assertEquals(result.getCount().intValue(), 1);
    assertTrue(result.getHasMore());
    assertEquals(result.getNextOffset().longValue(), 51L);
  }

  // ============================================================================
  // Error and Exception Handling Tests
  // ============================================================================

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testGetClusterInfo_Failure() throws Exception {
    // Arrange - cluster operation fails with timeout
    DescribeClusterResult clusterResult = mock(DescribeClusterResult.class);
    KafkaFuture<String> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(new java.util.concurrent.TimeoutException("Connection timed out"));
    when(clusterResult.clusterId()).thenReturn(failedFuture);
    when(adminClient.describeCluster()).thenReturn(clusterResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.getClusterInfo();
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testListTopics_Failure() throws Exception {
    // Arrange - list topics fails
    ListTopicsResult listResult = mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new RuntimeException("Kafka error")));
    when(listResult.names()).thenReturn(failedFuture);
    when(adminClient.listTopics()).thenReturn(listResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.listTopics(false);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testDescribeTopic_GenericFailure() throws Exception {
    // Arrange - describe topic fails with a non-UnknownTopicOrPartitionException
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new RuntimeException("Kafka error")));
    when(describeResult.topicNameValues()).thenReturn(Map.of(MCP_TOPIC, failedFuture));
    when(adminClient.describeTopics(anyCollection())).thenReturn(describeResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.describeTopic(MCP_TOPIC);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testDescribeTopic_InterruptedException() throws Exception {
    // Arrange - describe topic fails with InterruptedException
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(new InterruptedException("Thread interrupted"));
    when(describeResult.topicNameValues()).thenReturn(Map.of(MCP_TOPIC, failedFuture));
    when(adminClient.describeTopics(anyCollection())).thenReturn(describeResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.describeTopic(MCP_TOPIC);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testCreateDataHubTopic_GenericFailure() throws Exception {
    // Arrange - create topic fails with a non-TopicExistsException
    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new RuntimeException("Kafka error")));
    when(createResult.all()).thenReturn(failedFuture);
    when(adminClient.createTopics(anyCollection())).thenReturn(createResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.createDataHubTopic("mcp", 1, null, null);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testCreateDataHubTopic_TimeoutException() throws Exception {
    // Arrange - create topic times out
    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(new java.util.concurrent.TimeoutException("Operation timed out"));
    when(createResult.all()).thenReturn(failedFuture);
    when(adminClient.createTopics(anyCollection())).thenReturn(createResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.createDataHubTopic("mcp", 1, null, null);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testDeleteDataHubTopic_GenericFailure() throws Exception {
    // Arrange - delete topic fails with a non-UnknownTopicOrPartitionException
    DeleteTopicsResult deleteResult = mock(DeleteTopicsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new RuntimeException("Kafka error")));
    when(deleteResult.all()).thenReturn(failedFuture);
    when(adminClient.deleteTopics(anyCollection())).thenReturn(deleteResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.deleteDataHubTopic("mcp");
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testDeleteDataHubTopic_TimeoutException() throws Exception {
    // Arrange - delete topic times out
    DeleteTopicsResult deleteResult = mock(DeleteTopicsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(new java.util.concurrent.TimeoutException("Operation timed out"));
    when(deleteResult.all()).thenReturn(failedFuture);
    when(adminClient.deleteTopics(anyCollection())).thenReturn(deleteResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.deleteDataHubTopic("mcp");
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testAlterDataHubTopicConfig_Failure() throws Exception {
    // Arrange - alter config fails
    org.apache.kafka.clients.admin.AlterConfigsResult alterResult =
        mock(org.apache.kafka.clients.admin.AlterConfigsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new RuntimeException("Kafka error")));
    when(alterResult.all()).thenReturn(failedFuture);
    when(adminClient.incrementalAlterConfigs(anyMap())).thenReturn(alterResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.alterDataHubTopicConfig("mcp", Map.of("retention.ms", "86400000"));
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testGetLogDirs_Failure() throws Exception {
    // Arrange - cluster description succeeds but log dirs fails
    Node broker = new Node(0, "broker-host", 9092);
    DescribeClusterResult clusterResult = mock(DescribeClusterResult.class);
    when(clusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(broker)));
    when(adminClient.describeCluster()).thenReturn(clusterResult);

    DescribeLogDirsResult logDirsResult = mock(DescribeLogDirsResult.class);
    KafkaFuture<Map<Integer, Map<String, org.apache.kafka.clients.admin.LogDirDescription>>>
        failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new RuntimeException("Kafka error")));
    when(logDirsResult.allDescriptions()).thenReturn(failedFuture);
    when(adminClient.describeLogDirs(anyCollection())).thenReturn(logDirsResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.getLogDirs(null);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testListConsumerGroups_Failure() throws Exception {
    // Arrange - list consumer groups fails
    ListConsumerGroupsResult listResult = mock(ListConsumerGroupsResult.class);
    KafkaFuture<java.util.Collection<ConsumerGroupListing>> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new RuntimeException("Kafka error")));
    when(listResult.all()).thenReturn(failedFuture);
    when(adminClient.listConsumerGroups()).thenReturn(listResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.listConsumerGroups();
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testDescribeConsumerGroup_GenericFailure() throws Exception {
    // Arrange - describe consumer group fails with a non-GroupIdNotFoundException
    DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
    KafkaFuture<ConsumerGroupDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new RuntimeException("Kafka error")));
    when(describeResult.describedGroups())
        .thenReturn(Map.of(DATAHUB_MCP_CONSUMER_GROUP, failedFuture));
    when(adminClient.describeConsumerGroups(anyCollection())).thenReturn(describeResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.describeConsumerGroup(DATAHUB_MCP_CONSUMER_GROUP);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testDescribeConsumerGroup_TimeoutException() throws Exception {
    // Arrange - describe consumer group times out
    DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
    KafkaFuture<ConsumerGroupDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(new java.util.concurrent.TimeoutException("Operation timed out"));
    when(describeResult.describedGroups())
        .thenReturn(Map.of(DATAHUB_MCP_CONSUMER_GROUP, failedFuture));
    when(adminClient.describeConsumerGroups(anyCollection())).thenReturn(describeResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.describeConsumerGroup(DATAHUB_MCP_CONSUMER_GROUP);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testDeleteConsumerGroup_GenericFailure() throws Exception {
    // Arrange - delete consumer group fails with a non-GroupNotEmptyException
    DeleteConsumerGroupsResult deleteResult = mock(DeleteConsumerGroupsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new RuntimeException("Kafka error")));
    when(deleteResult.all()).thenReturn(failedFuture);
    when(adminClient.deleteConsumerGroups(anyCollection())).thenReturn(deleteResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.deleteConsumerGroup(DATAHUB_MCP_CONSUMER_GROUP);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testDeleteConsumerGroup_TimeoutException() throws Exception {
    // Arrange - delete consumer group times out
    DeleteConsumerGroupsResult deleteResult = mock(DeleteConsumerGroupsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(new java.util.concurrent.TimeoutException("Operation timed out"));
    when(deleteResult.all()).thenReturn(failedFuture);
    when(adminClient.deleteConsumerGroups(anyCollection())).thenReturn(deleteResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.deleteConsumerGroup(DATAHUB_MCP_CONSUMER_GROUP);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testResetConsumerGroupOffsets_ShiftByMissingValue() throws Exception {
    // Arrange
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Act - shift-by without value (wrapped in KafkaAdminException)
    kafkaAdminService.resetConsumerGroupOffsets(
        DATAHUB_MCP_CONSUMER_GROUP, "mcp", "shift-by", null, null, true);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testResetConsumerGroupOffsets_OffsetLookupFailure() throws Exception {
    // Arrange
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Mock offset lookup to fail
    ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
    KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> failedFuture =
        mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new RuntimeException("Kafka error")));
    when(listOffsetsResult.all()).thenReturn(failedFuture);
    when(adminClient.listOffsets(anyMap())).thenReturn(listOffsetsResult);

    // Act - should throw KafkaAdminException
    kafkaAdminService.resetConsumerGroupOffsets(
        DATAHUB_MCP_CONSUMER_GROUP, "mcp", "earliest", null, null, true);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testResetConsumerGroupOffsets_CommitFailure() throws Exception {
    // Arrange
    TopicPartition tp =
        new TopicPartition(
            topicConvention.getTopicConvention().getMetadataChangeProposalTopicName(), 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(100L));

    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsets));
    when(adminClient.listConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP)))
        .thenReturn(offsetsResult);

    // Mock alterConsumerGroupOffsets to fail
    org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult alterResult =
        mock(org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult.class);
    KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get(any(Long.class), any(TimeUnit.class)))
        .thenThrow(
            new java.util.concurrent.ExecutionException(new RuntimeException("Kafka error")));
    when(alterResult.all()).thenReturn(failedFuture);
    when(adminClient.alterConsumerGroupOffsets(eq(DATAHUB_MCP_CONSUMER_GROUP), anyMap()))
        .thenReturn(alterResult);

    // Act - dryRun = false, should fail on commit
    kafkaAdminService.resetConsumerGroupOffsets(
        DATAHUB_MCP_CONSUMER_GROUP, "mcp", "to-offset", "50", null, false);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testGetMessages_ConsumerFailure() {
    // Arrange - consumer throws exception on assign
    org.apache.kafka.common.KafkaException kafkaError =
        new org.apache.kafka.common.KafkaException("Consumer error");
    doThrow(kafkaError).when(consumer).assign(anyList());

    // Act - should throw KafkaAdminException
    kafkaAdminService.getMessages("mcp", 0, 0L, 10);
  }

  @Test(expectedExceptions = KafkaAdminException.class)
  public void testReplayFailedMCPs_ConsumerFailure() {
    // Arrange - consumer throws exception on assign
    org.apache.kafka.common.KafkaException kafkaError =
        new org.apache.kafka.common.KafkaException("Consumer error");
    doThrow(kafkaError).when(consumer).assign(anyList());

    // Act - should throw KafkaAdminException
    kafkaAdminService.replayFailedMCPs(0, 100L, 100L, 10, true);
  }

  @Test
  public void testGetLogDirs_WithLogDirError() throws Exception {
    // Arrange - log dir has an error
    Node broker = new Node(0, "broker-host", 9092);
    DescribeClusterResult clusterResult = mock(DescribeClusterResult.class);
    when(clusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(broker)));
    when(adminClient.describeCluster()).thenReturn(clusterResult);

    // Mock log dir description with error
    org.apache.kafka.common.errors.KafkaStorageException storageError =
        new org.apache.kafka.common.errors.KafkaStorageException("Disk error");
    org.apache.kafka.clients.admin.LogDirDescription logDirDescription =
        new org.apache.kafka.clients.admin.LogDirDescription(storageError, Collections.emptyMap());

    DescribeLogDirsResult logDirsResult = mock(DescribeLogDirsResult.class);
    when(logDirsResult.allDescriptions())
        .thenReturn(
            KafkaFuture.completedFuture(Map.of(0, Map.of("/var/kafka-logs", logDirDescription))));
    when(adminClient.describeLogDirs(anyCollection())).thenReturn(logDirsResult);

    // Act
    var result = kafkaAdminService.getLogDirs(null);

    // Assert - should include error information
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getLogDirs().size(), 1);
    assertNotNull(result.get(0).getLogDirs().get(0).getError());
    assertEquals(result.get(0).getLogDirs().get(0).getError(), "Disk error");
  }
}
