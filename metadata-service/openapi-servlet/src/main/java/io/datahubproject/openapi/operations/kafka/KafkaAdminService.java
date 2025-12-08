package io.datahubproject.openapi.operations.kafka;

import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openapi.operations.kafka.models.KafkaClusterResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaConsumerGroupResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaLogDirResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaMessageResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaTopicResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * Service for Kafka admin operations.
 *
 * <p>This service provides operations for managing Kafka topics, consumer groups, and cluster
 * metadata. Write operations (create/delete topics, alter configs, message retrieval/replay) are
 * restricted to DataHub topics only, while read operations work on any topic in the cluster.
 */
@Slf4j
@Builder
public class KafkaAdminService {

  public static final int DEFAULT_TIMEOUT_SECONDS = 30;
  public static final int DEFAULT_MESSAGE_COUNT = 100;

  @Nonnull private final AdminClient adminClient;
  @Nonnull private final DataHubTopicConvention topicConvention;
  @Nullable private final KafkaEventProducer kafkaEventProducer;
  @Nonnull private final Supplier<Consumer<String, GenericRecord>> consumerSupplier;

  @Builder.Default private final int timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
  @Builder.Default private final int defaultMessageCount = DEFAULT_MESSAGE_COUNT;

  // ============================================================================
  // Cluster Operations
  // ============================================================================

  /**
   * Gets cluster metadata including brokers, controller, and cluster ID.
   *
   * @return cluster information
   * @throws KafkaAdminException if the operation fails
   */
  public KafkaClusterResponse getClusterInfo() {
    try {
      DescribeClusterResult clusterResult = adminClient.describeCluster();
      String clusterId = clusterResult.clusterId().get(timeoutSeconds, TimeUnit.SECONDS);
      Node controller = clusterResult.controller().get(timeoutSeconds, TimeUnit.SECONDS);
      List<Node> nodes =
          new ArrayList<>(clusterResult.nodes().get(timeoutSeconds, TimeUnit.SECONDS));

      List<KafkaClusterResponse.BrokerInfo> brokerInfos =
          nodes.stream()
              .map(
                  node ->
                      KafkaClusterResponse.BrokerInfo.builder()
                          .id(node.id())
                          .host(node.host())
                          .port(node.port())
                          .rack(node.rack())
                          .isController(node.id() == controller.id())
                          .build())
              .collect(Collectors.toList());

      KafkaClusterResponse.BrokerInfo controllerInfo =
          KafkaClusterResponse.BrokerInfo.builder()
              .id(controller.id())
              .host(controller.host())
              .port(controller.port())
              .rack(controller.rack())
              .isController(true)
              .build();

      return KafkaClusterResponse.builder()
          .clusterId(clusterId)
          .controller(controllerInfo)
          .brokers(brokerInfos)
          .brokerCount(brokerInfos.size())
          .build();
    } catch (Exception e) {
      log.error("Failed to get cluster information", e);
      throw new KafkaAdminException("Failed to get cluster information: " + e.getMessage(), e);
    }
  }

  // ============================================================================
  // Topic Operations (Read-only - DataHub topics only)
  // ============================================================================

  /**
   * Lists DataHub topics in the cluster.
   *
   * @param includeInternal whether to include internal Kafka topics (starting with __)
   * @return list of topic names (only DataHub topics, plus internal if requested)
   * @throws KafkaAdminException if the operation fails
   */
  public List<String> listTopics(boolean includeInternal) {
    try {
      ListTopicsResult result = adminClient.listTopics();
      Set<String> topicNames = result.names().get(timeoutSeconds, TimeUnit.SECONDS);
      Set<String> dataHubTopics = topicConvention.getAllDataHubTopicNames();

      return topicNames.stream()
          .filter(
              topic -> dataHubTopics.contains(topic) || (includeInternal && topic.startsWith("__")))
          .sorted()
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Failed to list topics", e);
      throw new KafkaAdminException("Failed to list topics: " + e.getMessage(), e);
    }
  }

  /**
   * Describes a DataHub topic including partitions, configs, and ISR.
   *
   * @param topicName the topic name (must be a DataHub topic)
   * @return topic information
   * @throws KafkaAdminException.InvalidTopicException if the topic is not a DataHub topic
   * @throws KafkaAdminException.TopicNotFoundException if the topic is not found
   * @throws KafkaAdminException if the operation fails
   */
  public KafkaTopicResponse describeTopic(@Nonnull String topicName) {
    // Validate the topic is a DataHub topic
    if (!topicConvention.isDataHubTopic(topicName)) {
      throw new KafkaAdminException.InvalidTopicException(
          topicName, topicConvention.getAllDataHubTopicNames());
    }

    try {
      TopicDescription topicDescription =
          adminClient
              .describeTopics(List.of(topicName))
              .topicNameValues()
              .get(topicName)
              .get(timeoutSeconds, TimeUnit.SECONDS);

      ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
      Config config =
          adminClient
              .describeConfigs(List.of(configResource))
              .values()
              .get(configResource)
              .get(timeoutSeconds, TimeUnit.SECONDS);

      Map<String, String> configMap =
          config.entries().stream()
              .filter(entry -> !entry.isDefault() || !entry.isSensitive())
              .collect(
                  Collectors.toMap(
                      ConfigEntry::name, entry -> entry.isSensitive() ? "***" : entry.value()));

      Optional<String> dataHubAlias = topicConvention.getAliasForTopicName(topicName);

      List<KafkaTopicResponse.PartitionInfo> partitionInfos =
          topicDescription.partitions().stream()
              .map(
                  partition ->
                      KafkaTopicResponse.PartitionInfo.builder()
                          .partition(partition.partition())
                          .leader(partition.leader() != null ? partition.leader().id() : null)
                          .replicas(
                              partition.replicas().stream()
                                  .map(Node::id)
                                  .collect(Collectors.toList()))
                          .isr(partition.isr().stream().map(Node::id).collect(Collectors.toList()))
                          .build())
              .collect(Collectors.toList());

      return KafkaTopicResponse.builder()
          .name(topicName)
          .partitionCount(topicDescription.partitions().size())
          .replicationFactor(
              topicDescription.partitions().isEmpty()
                  ? null
                  : topicDescription.partitions().get(0).replicas().size())
          .internal(topicDescription.isInternal())
          .dataHubAlias(dataHubAlias.orElse(null))
          .partitions(partitionInfos)
          .configs(configMap)
          .build();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        throw new KafkaAdminException.TopicNotFoundException(topicName);
      }
      log.error("Failed to describe topic: " + topicName, e);
      throw new KafkaAdminException("Failed to describe topic: " + e.getMessage(), e);
    } catch (Exception e) {
      log.error("Failed to describe topic: " + topicName, e);
      throw new KafkaAdminException("Failed to describe topic: " + e.getMessage(), e);
    }
  }

  // ============================================================================
  // Topic Operations (Write - DataHub topics only)
  // ============================================================================

  /**
   * Creates a DataHub topic.
   *
   * @param alias the topic alias (mcp, mcl-versioned, etc.)
   * @param numPartitions number of partitions (defaults to 1)
   * @param replicationFactor replication factor (defaults to cluster default)
   * @param configs topic configuration
   * @return the created topic info
   * @throws KafkaAdminException if the alias is invalid or the operation fails
   */
  public KafkaTopicResponse createDataHubTopic(
      @Nonnull String alias,
      @Nullable Integer numPartitions,
      @Nullable Short replicationFactor,
      @Nullable Map<String, String> configs) {

    String topicName = resolveTopicName(alias);

    try {
      int partitions = numPartitions != null ? numPartitions : 1;

      NewTopic newTopic =
          new NewTopic(
              topicName,
              Optional.of(partitions),
              replicationFactor != null ? Optional.of(replicationFactor) : Optional.empty());
      if (configs != null) {
        newTopic.configs(configs);
      }

      CreateTopicsResult result = adminClient.createTopics(List.of(newTopic));
      result.all().get(timeoutSeconds, TimeUnit.SECONDS);

      return describeTopic(topicName);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        throw new KafkaAdminException.TopicAlreadyExistsException(topicName);
      }
      log.error("Failed to create topic: " + topicName, e);
      throw new KafkaAdminException("Failed to create topic: " + e.getMessage(), e);
    } catch (Exception e) {
      log.error("Failed to create topic: " + topicName, e);
      throw new KafkaAdminException("Failed to create topic: " + e.getMessage(), e);
    }
  }

  /**
   * Deletes a DataHub topic.
   *
   * @param alias the topic alias (mcp, mcl-versioned, etc.)
   * @throws KafkaAdminException if the alias is invalid or the operation fails
   */
  public void deleteDataHubTopic(@Nonnull String alias) {
    String topicName = resolveTopicName(alias);

    try {
      DeleteTopicsResult result = adminClient.deleteTopics(List.of(topicName));
      result.all().get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        throw new KafkaAdminException.TopicNotFoundException(topicName);
      }
      log.error("Failed to delete topic: " + topicName, e);
      throw new KafkaAdminException("Failed to delete topic: " + e.getMessage(), e);
    } catch (Exception e) {
      log.error("Failed to delete topic: " + topicName, e);
      throw new KafkaAdminException("Failed to delete topic: " + e.getMessage(), e);
    }
  }

  /**
   * Alters configuration for a DataHub topic.
   *
   * @param alias the topic alias (mcp, mcl-versioned, etc.)
   * @param configs configuration changes (null value deletes the config)
   * @return the updated topic info
   * @throws KafkaAdminException if the alias is invalid or the operation fails
   */
  public KafkaTopicResponse alterDataHubTopicConfig(
      @Nonnull String alias, @Nonnull Map<String, String> configs) {
    String topicName = resolveTopicName(alias);

    if (configs.isEmpty()) {
      throw new KafkaAdminException("No configuration changes provided");
    }

    try {
      ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
      List<AlterConfigOp> configOps =
          configs.entrySet().stream()
              .map(
                  entry ->
                      new AlterConfigOp(
                          new ConfigEntry(entry.getKey(), entry.getValue()),
                          entry.getValue() == null
                              ? AlterConfigOp.OpType.DELETE
                              : AlterConfigOp.OpType.SET))
              .collect(Collectors.toList());

      Map<ConfigResource, Collection<AlterConfigOp>> configUpdates = new HashMap<>();
      configUpdates.put(configResource, configOps);

      adminClient
          .incrementalAlterConfigs(configUpdates)
          .all()
          .get(timeoutSeconds, TimeUnit.SECONDS);

      return describeTopic(topicName);
    } catch (Exception e) {
      log.error("Failed to alter topic config: " + topicName, e);
      throw new KafkaAdminException("Failed to alter topic config: " + e.getMessage(), e);
    }
  }

  // ============================================================================
  // Log Directory Operations
  // ============================================================================

  /**
   * Gets log directory information for specified brokers.
   *
   * @param brokerIds set of broker IDs (null or empty for all brokers)
   * @return list of broker log directory information
   * @throws KafkaAdminException if the operation fails
   */
  public List<KafkaLogDirResponse.BrokerLogDirInfo> getLogDirs(@Nullable Set<Integer> brokerIds) {
    try {
      Set<Integer> targetBrokerIds = brokerIds;
      if (targetBrokerIds == null || targetBrokerIds.isEmpty()) {
        DescribeClusterResult clusterResult = adminClient.describeCluster();
        List<Node> nodes =
            new ArrayList<>(clusterResult.nodes().get(timeoutSeconds, TimeUnit.SECONDS));
        targetBrokerIds = nodes.stream().map(Node::id).collect(Collectors.toSet());
      }

      DescribeLogDirsResult result = adminClient.describeLogDirs(targetBrokerIds);
      Map<Integer, Map<String, LogDirDescription>> logDirsByBroker =
          result.allDescriptions().get(timeoutSeconds, TimeUnit.SECONDS);

      List<KafkaLogDirResponse.BrokerLogDirInfo> brokerInfos = new ArrayList<>();

      for (Map.Entry<Integer, Map<String, LogDirDescription>> brokerEntry :
          logDirsByBroker.entrySet()) {
        Integer brokerId = brokerEntry.getKey();
        Map<String, LogDirDescription> logDirs = brokerEntry.getValue();

        List<KafkaLogDirResponse.LogDirInfo> logDirDetails = new ArrayList<>();
        long totalSize = 0;

        for (Map.Entry<String, LogDirDescription> logDirEntry : logDirs.entrySet()) {
          String path = logDirEntry.getKey();
          LogDirDescription logDirDescription = logDirEntry.getValue();

          // Check for errors
          if (logDirDescription.error() != null) {
            logDirDetails.add(
                KafkaLogDirResponse.LogDirInfo.builder()
                    .path(path)
                    .error(logDirDescription.error().getMessage())
                    .build());
            continue;
          }

          Map<String, KafkaLogDirResponse.ReplicaInfo> replicas = new HashMap<>();
          long dirSize = 0;

          for (Map.Entry<TopicPartition, ReplicaInfo> replicaEntry :
              logDirDescription.replicaInfos().entrySet()) {
            TopicPartition tp = replicaEntry.getKey();
            ReplicaInfo replicaInfo = replicaEntry.getValue();

            String topicPartition = tp.topic() + "-" + tp.partition();
            replicas.put(
                topicPartition,
                KafkaLogDirResponse.ReplicaInfo.builder()
                    .topic(tp.topic())
                    .partition(tp.partition())
                    .sizeBytes(replicaInfo.size())
                    .offsetLag(replicaInfo.offsetLag())
                    .isFuture(replicaInfo.isFuture())
                    .build());
            dirSize += replicaInfo.size();
          }

          totalSize += dirSize;

          logDirDetails.add(
              KafkaLogDirResponse.LogDirInfo.builder()
                  .path(path)
                  .sizeBytes(dirSize)
                  .replicas(replicas)
                  .build());
        }

        brokerInfos.add(
            KafkaLogDirResponse.BrokerLogDirInfo.builder()
                .brokerId(brokerId)
                .logDirs(logDirDetails)
                .totalSizeBytes(totalSize)
                .build());
      }

      return brokerInfos;
    } catch (Exception e) {
      log.error("Failed to get log directories", e);
      throw new KafkaAdminException("Failed to get log directories: " + e.getMessage(), e);
    }
  }

  // ============================================================================
  // Consumer Group Operations (Read-only - DataHub consumer groups only)
  // ============================================================================

  /**
   * Lists known DataHub consumer groups in the cluster.
   *
   * @return list of consumer group summaries (only known DataHub consumer groups)
   * @throws KafkaAdminException if the operation fails
   */
  public List<KafkaConsumerGroupResponse.ConsumerGroupSummary> listConsumerGroups() {
    try {
      ListConsumerGroupsResult result = adminClient.listConsumerGroups();
      List<ConsumerGroupListing> groups =
          new ArrayList<>(result.all().get(timeoutSeconds, TimeUnit.SECONDS));

      return groups.stream()
          .filter(group -> topicConvention.isDataHubConsumerGroup(group.groupId()))
          .map(
              group ->
                  KafkaConsumerGroupResponse.ConsumerGroupSummary.builder()
                      .groupId(group.groupId())
                      .state(group.state().orElse(ConsumerGroupState.UNKNOWN).toString())
                      .isDataHubGroup(true)
                      .isSimpleConsumerGroup(group.isSimpleConsumerGroup())
                      .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Failed to list consumer groups", e);
      throw new KafkaAdminException("Failed to list consumer groups: " + e.getMessage(), e);
    }
  }

  /**
   * Describes a DataHub consumer group including state, members, and offsets.
   *
   * @param groupId the consumer group ID (must be a known DataHub consumer group)
   * @return consumer group information
   * @throws KafkaAdminException.InvalidConsumerGroupException if the group is not a DataHub group
   * @throws KafkaAdminException.ConsumerGroupNotFoundException if the group is not found
   * @throws KafkaAdminException if the operation fails
   */
  public KafkaConsumerGroupResponse describeConsumerGroup(@Nonnull String groupId) {
    // Validate the consumer group is a known DataHub consumer group
    if (!topicConvention.isDataHubConsumerGroup(groupId)) {
      throw new KafkaAdminException.InvalidConsumerGroupException(
          groupId, topicConvention.getKnownConsumerGroupIds());
    }

    try {
      DescribeConsumerGroupsResult describeResult =
          adminClient.describeConsumerGroups(List.of(groupId));
      ConsumerGroupDescription groupDescription =
          describeResult.describedGroups().get(groupId).get(timeoutSeconds, TimeUnit.SECONDS);

      ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupId);
      Map<TopicPartition, OffsetAndMetadata> offsets =
          offsetsResult.partitionsToOffsetAndMetadata().get(timeoutSeconds, TimeUnit.SECONDS);

      Map<String, KafkaConsumerGroupResponse.TopicOffsets> topicOffsetsMap = new HashMap<>();
      long totalLag = 0;

      // Batch fetch all end offsets in a single call (instead of N calls per partition)
      Map<TopicPartition, Long> endOffsets = fetchOffsets(offsets.keySet(), OffsetSpec.latest());

      for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
        TopicPartition tp = entry.getKey();
        OffsetAndMetadata offsetAndMetadata = entry.getValue();

        Long endOffset = endOffsets.get(tp);
        Long currentOffset = offsetAndMetadata.offset();
        Long lag = endOffset != null ? Math.max(0, endOffset - currentOffset) : null;
        if (lag != null) {
          totalLag += lag;
        }

        KafkaConsumerGroupResponse.PartitionOffsetInfo partitionOffsetInfo =
            KafkaConsumerGroupResponse.PartitionOffsetInfo.builder()
                .currentOffset(currentOffset)
                .endOffset(endOffset)
                .lag(lag)
                .metadata(offsetAndMetadata.metadata())
                .build();

        topicOffsetsMap
            .computeIfAbsent(
                tp.topic(),
                k ->
                    KafkaConsumerGroupResponse.TopicOffsets.builder()
                        .partitions(new HashMap<>())
                        .build())
            .getPartitions()
            .put(tp.partition(), partitionOffsetInfo);
      }

      for (KafkaConsumerGroupResponse.TopicOffsets topicOffsets : topicOffsetsMap.values()) {
        long topicLag =
            topicOffsets.getPartitions().values().stream()
                .mapToLong(poi -> poi.getLag() != null ? poi.getLag() : 0)
                .sum();
        topicOffsets.setTopicLag(topicLag);
      }

      Node coordinator = groupDescription.coordinator();
      KafkaConsumerGroupResponse.CoordinatorInfo coordinatorInfo =
          KafkaConsumerGroupResponse.CoordinatorInfo.builder()
              .id(coordinator.id())
              .host(coordinator.host())
              .port(coordinator.port())
              .build();

      List<KafkaConsumerGroupResponse.MemberInfo> memberInfos =
          groupDescription.members().stream()
              .map(
                  member ->
                      KafkaConsumerGroupResponse.MemberInfo.builder()
                          .memberId(member.consumerId())
                          .groupInstanceId(member.groupInstanceId().orElse(null))
                          .clientId(member.clientId())
                          .clientHost(member.host())
                          .assignments(
                              member.assignment().topicPartitions().stream()
                                  .map(
                                      tp ->
                                          KafkaConsumerGroupResponse.TopicPartitionInfo.builder()
                                              .topic(tp.topic())
                                              .partition(tp.partition())
                                              .build())
                                  .collect(Collectors.toList()))
                          .build())
              .collect(Collectors.toList());

      return KafkaConsumerGroupResponse.builder()
          .groupId(groupId)
          .state(groupDescription.state().toString())
          .isSimpleConsumerGroup(groupDescription.isSimpleConsumerGroup())
          .coordinator(coordinatorInfo)
          .partitionAssignor(groupDescription.partitionAssignor())
          .members(memberInfos)
          .offsets(topicOffsetsMap)
          .totalLag(totalLag)
          .build();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
        throw new KafkaAdminException.ConsumerGroupNotFoundException(groupId);
      }
      log.error("Failed to describe consumer group: " + groupId, e);
      throw new KafkaAdminException("Failed to describe consumer group: " + e.getMessage(), e);
    } catch (Exception e) {
      log.error("Failed to describe consumer group: " + groupId, e);
      throw new KafkaAdminException("Failed to describe consumer group: " + e.getMessage(), e);
    }
  }

  // ============================================================================
  // Consumer Group Operations (Write - DataHub consumer groups only)
  // ============================================================================

  /**
   * Deletes an inactive DataHub consumer group.
   *
   * @param groupId the consumer group ID (must be a known DataHub consumer group)
   * @throws KafkaAdminException.InvalidConsumerGroupException if the group is not a DataHub group
   * @throws KafkaAdminException.ConsumerGroupNotEmptyException if the group is active
   * @throws KafkaAdminException if the operation fails
   */
  public void deleteConsumerGroup(@Nonnull String groupId) {
    // Validate the consumer group is a known DataHub consumer group
    if (!topicConvention.isDataHubConsumerGroup(groupId)) {
      throw new KafkaAdminException.InvalidConsumerGroupException(
          groupId, topicConvention.getKnownConsumerGroupIds());
    }

    try {
      DeleteConsumerGroupsResult result = adminClient.deleteConsumerGroups(List.of(groupId));
      result.all().get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof GroupNotEmptyException) {
        throw new KafkaAdminException.ConsumerGroupNotEmptyException(groupId);
      }
      log.error("Failed to delete consumer group: " + groupId, e);
      throw new KafkaAdminException("Failed to delete consumer group: " + e.getMessage(), e);
    } catch (Exception e) {
      log.error("Failed to delete consumer group: " + groupId, e);
      throw new KafkaAdminException("Failed to delete consumer group: " + e.getMessage(), e);
    }
  }

  /**
   * Resets consumer group offsets for DataHub topic partitions.
   *
   * @param groupId the consumer group ID (must be a known DataHub consumer group)
   * @param topicAlias the topic alias (mcp, mcl-versioned, etc.)
   * @param strategy the reset strategy (earliest, latest, to-offset, shift-by)
   * @param value the target value for to-offset or shift-by strategies
   * @param partitions specific partitions to reset (null for all)
   * @param dryRun if true, only shows what would be changed
   * @return list of partition reset information
   * @throws KafkaAdminException.InvalidAliasException if the topic alias is invalid
   * @throws KafkaAdminException.InvalidConsumerGroupException if the group is not a DataHub group
   * @throws KafkaAdminException if the operation fails
   */
  public List<KafkaConsumerGroupResponse.PartitionResetInfo> resetConsumerGroupOffsets(
      @Nonnull String groupId,
      @Nonnull String topicAlias,
      @Nonnull String strategy,
      @Nullable String value,
      @Nullable List<Integer> partitions,
      boolean dryRun) {

    // Validate the consumer group is a known DataHub consumer group
    if (!topicConvention.isDataHubConsumerGroup(groupId)) {
      throw new KafkaAdminException.InvalidConsumerGroupException(
          groupId, topicConvention.getKnownConsumerGroupIds());
    }

    // Resolve the topic alias to the actual topic name
    String topicName = resolveTopicName(topicAlias);

    try {
      ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupId);
      Map<TopicPartition, OffsetAndMetadata> currentOffsets =
          offsetsResult.partitionsToOffsetAndMetadata().get(timeoutSeconds, TimeUnit.SECONDS);

      // Filter to only the specified topic and optionally specific partitions
      Map<TopicPartition, OffsetAndMetadata> filteredOffsets =
          currentOffsets.entrySet().stream()
              .filter(entry -> entry.getKey().topic().equals(topicName))
              .filter(
                  entry -> partitions == null || partitions.contains(entry.getKey().partition()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      if (filteredOffsets.isEmpty()) {
        throw new KafkaAdminException(
            "No partitions found for topic '"
                + topicAlias
                + "' in consumer group '"
                + groupId
                + "'");
      }

      Map<TopicPartition, Long> targetOffsets =
          calculateTargetOffsets(filteredOffsets, strategy, value);

      List<KafkaConsumerGroupResponse.PartitionResetInfo> resetInfos =
          filteredOffsets.entrySet().stream()
              .map(
                  entry -> {
                    TopicPartition tp = entry.getKey();
                    Long currentOffset = entry.getValue().offset();
                    Long newOffset = targetOffsets.get(tp);
                    return KafkaConsumerGroupResponse.PartitionResetInfo.builder()
                        .partition(tp.partition())
                        .previousOffset(currentOffset)
                        .newOffset(newOffset)
                        .offsetChange(
                            newOffset != null && currentOffset != null
                                ? newOffset - currentOffset
                                : null)
                        .build();
                  })
              .collect(Collectors.toList());

      if (!dryRun) {
        Map<TopicPartition, OffsetAndMetadata> newOffsets =
            targetOffsets.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry ->
                            new OffsetAndMetadata(
                                entry.getValue(), filteredOffsets.get(entry.getKey()).metadata())));

        // Use AdminClient.alterConsumerGroupOffsets to reset offsets for the target consumer group
        adminClient
            .alterConsumerGroupOffsets(groupId, newOffsets)
            .all()
            .get(timeoutSeconds, TimeUnit.SECONDS);
      }

      return resetInfos;
    } catch (KafkaAdminException e) {
      throw e;
    } catch (Exception e) {
      log.error("Failed to reset consumer group offsets: " + groupId, e);
      throw new KafkaAdminException("Failed to reset consumer group offsets: " + e.getMessage(), e);
    }
  }

  // ============================================================================
  // Message Operations (DataHub topics only)
  // ============================================================================

  /**
   * Retrieves messages from a DataHub topic with default JSON format.
   *
   * @param alias the topic alias (mcp, mcl-versioned, etc.)
   * @param partition the partition to read from
   * @param offset the offset to start reading from
   * @param count maximum number of messages to retrieve
   * @return message batch with retrieved messages
   * @throws KafkaAdminException if the alias is invalid or the operation fails
   */
  public KafkaMessageResponse getMessages(
      @Nonnull String alias, int partition, long offset, int count) {
    return getMessages(alias, partition, offset, count, "json", false);
  }

  /**
   * Retrieves messages from a DataHub topic with configurable output format.
   *
   * @param alias the topic alias (mcp, mcl-versioned, etc.)
   * @param partition the partition to read from
   * @param offset the offset to start reading from
   * @param count maximum number of messages to retrieve
   * @param format output format: "json" (default), "avro" (with schema), or "raw" (base64)
   * @param includeHeaders whether to include message headers
   * @return message batch with retrieved messages
   * @throws KafkaAdminException if the alias is invalid or the operation fails
   */
  public KafkaMessageResponse getMessages(
      @Nonnull String alias,
      int partition,
      long offset,
      int count,
      @Nonnull String format,
      boolean includeHeaders) {

    String topicName = resolveTopicName(alias);

    try (Consumer<String, GenericRecord> consumer = consumerSupplier.get()) {
      TopicPartition tp = new TopicPartition(topicName, partition);
      consumer.assign(List.of(tp));
      consumer.seek(tp, offset);

      List<KafkaMessageResponse.MessageInfo> messages = new ArrayList<>();
      int messagesRead = 0;
      Long nextOffset = null;

      while (messagesRead < count) {
        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(1));
        if (records.isEmpty()) {
          break;
        }

        for (ConsumerRecord<String, GenericRecord> record : records) {
          if (!record.topic().equals(topicName) || record.partition() != partition) {
            continue;
          }

          if (record.offset() < offset) {
            continue;
          }

          if (messagesRead >= count) {
            nextOffset = record.offset();
            break;
          }

          // Format the value based on requested format
          Object formattedValue = formatMessageValue(record.value(), format);

          KafkaMessageResponse.MessageInfo.MessageInfoBuilder messageBuilder =
              KafkaMessageResponse.MessageInfo.builder()
                  .partition(record.partition())
                  .offset(record.offset())
                  .timestamp(record.timestamp())
                  .timestampType(record.timestampType().name())
                  .key(record.key())
                  .value(formattedValue)
                  .format(format)
                  .replayable(DataHubTopicConvention.ALIAS_MCP_FAILED.equals(alias));

          // Include headers if requested
          if (includeHeaders && record.headers() != null) {
            List<KafkaMessageResponse.HeaderInfo> headerList = new ArrayList<>();
            for (org.apache.kafka.common.header.Header header : record.headers()) {
              headerList.add(
                  KafkaMessageResponse.HeaderInfo.builder()
                      .key(header.key())
                      .value(header.value() != null ? new String(header.value()) : null)
                      .build());
            }
            messageBuilder.headers(headerList);
          }

          messages.add(messageBuilder.build());
          messagesRead++;
        }
      }

      if (messagesRead < count) {
        nextOffset = consumer.position(tp);
      }

      return KafkaMessageResponse.builder()
          .topic(topicName)
          .dataHubAlias(alias)
          .messages(messages)
          .count(messages.size())
          .hasMore(nextOffset != null && messages.size() == count)
          .nextOffset(nextOffset)
          .build();
    } catch (Exception e) {
      log.error("Failed to retrieve messages from topic: " + topicName, e);
      throw new KafkaAdminException("Failed to retrieve messages: " + e.getMessage(), e);
    }
  }

  /**
   * Formats a message value according to the specified format.
   *
   * @param value the Avro GenericRecord value
   * @param format the desired output format
   * @return formatted value as Object (String for json/raw, Map for avro)
   */
  private Object formatMessageValue(GenericRecord value, String format) {
    if (value == null) {
      return null;
    }

    switch (format.toLowerCase()) {
      case "avro":
        // Return full Avro representation with schema information
        Map<String, Object> avroOutput = new LinkedHashMap<>();
        avroOutput.put("schema", value.getSchema().toString(true));
        avroOutput.put("data", value.toString());
        return avroOutput;

      case "raw":
        // Return base64 encoded binary
        try {
          org.apache.avro.io.DatumWriter<GenericRecord> writer =
              new org.apache.avro.generic.GenericDatumWriter<>(value.getSchema());
          java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
          org.apache.avro.io.BinaryEncoder encoder =
              org.apache.avro.io.EncoderFactory.get().binaryEncoder(baos, null);
          writer.write(value, encoder);
          encoder.flush();
          return java.util.Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (Exception e) {
          log.warn("Failed to encode message as base64, falling back to string", e);
          return value.toString();
        }

      case "json":
      default:
        // Default: JSON string representation
        return value.toString();
    }
  }

  /**
   * Replays failed MCPs from the mcp-failed topic to the mcp topic.
   *
   * @param partition the partition to replay from
   * @param startOffset the starting offset (inclusive)
   * @param endOffset the ending offset (inclusive, null for single message)
   * @param maxCount maximum number of messages to replay
   * @param dryRun if true, only shows what would be replayed
   * @return replay result with details of each message
   * @throws KafkaAdminException if the operation fails
   */
  public KafkaMessageResponse.ReplayResponse replayFailedMCPs(
      int partition,
      long startOffset,
      @Nullable Long endOffset,
      @Nullable Integer maxCount,
      boolean dryRun) {

    if (kafkaEventProducer == null) {
      throw new KafkaAdminException("Kafka event producer is not configured for replay operations");
    }

    String mcpFailedTopicName =
        topicConvention
            .getTopicNameForAlias(DataHubTopicConvention.ALIAS_MCP_FAILED)
            .orElseThrow(() -> new KafkaAdminException("MCP failed topic not configured"));

    String mcpTopicName =
        topicConvention
            .getTopicNameForAlias(DataHubTopicConvention.ALIAS_MCP)
            .orElseThrow(() -> new KafkaAdminException("MCP topic not configured"));

    Long effectiveEndOffset = endOffset != null ? endOffset : startOffset;
    int effectiveMaxCount = maxCount != null ? maxCount : defaultMessageCount;

    try (Consumer<String, GenericRecord> consumer = consumerSupplier.get()) {
      TopicPartition tp = new TopicPartition(mcpFailedTopicName, partition);
      consumer.assign(List.of(tp));
      consumer.seek(tp, startOffset);

      List<KafkaMessageResponse.ReplayedMessageInfo> replayedMessages = new ArrayList<>();
      List<String> errors = new ArrayList<>();
      int successCount = 0;
      int failureCount = 0;
      int messagesRead = 0;

      while (messagesRead < effectiveMaxCount) {
        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(1));
        if (records.isEmpty()) {
          break;
        }

        for (ConsumerRecord<String, GenericRecord> record : records) {
          if (!record.topic().equals(mcpFailedTopicName) || record.partition() != partition) {
            continue;
          }

          if (record.offset() < startOffset || record.offset() > effectiveEndOffset) {
            continue;
          }

          if (messagesRead >= effectiveMaxCount) {
            break;
          }

          messagesRead++;

          if (dryRun) {
            replayedMessages.add(
                KafkaMessageResponse.ReplayedMessageInfo.builder()
                    .sourceOffset(record.offset())
                    .key(record.key())
                    .success(true)
                    .build());
            successCount++;
            continue;
          }

          try {
            GenericRecord failedMCPRecord = record.value();
            if (failedMCPRecord == null) {
              throw new IllegalArgumentException("Message value is null");
            }

            FailedMetadataChangeProposal failedMCP =
                EventUtils.avroToPegasusFailedMCP(failedMCPRecord);
            MetadataChangeProposal mcp = failedMCP.getMetadataChangeProposal();

            com.linkedin.common.urn.Urn urn = mcp.getEntityUrn();
            if (urn == null) {
              throw new IllegalArgumentException("MCP missing entityUrn");
            }

            kafkaEventProducer.produceMetadataChangeProposal(urn, mcp).get();

            replayedMessages.add(
                KafkaMessageResponse.ReplayedMessageInfo.builder()
                    .sourceOffset(record.offset())
                    .key(record.key())
                    .success(true)
                    .build());
            successCount++;
          } catch (Exception e) {
            log.error("Failed to replay message at offset: " + record.offset(), e);
            replayedMessages.add(
                KafkaMessageResponse.ReplayedMessageInfo.builder()
                    .sourceOffset(record.offset())
                    .key(record.key())
                    .success(false)
                    .error(e.getMessage())
                    .build());
            errors.add("Offset " + record.offset() + ": " + e.getMessage());
            failureCount++;
          }
        }
      }

      return KafkaMessageResponse.ReplayResponse.builder()
          .dryRun(dryRun)
          .sourceTopic(mcpFailedTopicName)
          .destinationTopic(mcpTopicName)
          .partition(partition)
          .startOffset(startOffset)
          .endOffset(effectiveEndOffset)
          .successCount(successCount)
          .failureCount(failureCount)
          .messages(replayedMessages)
          .errors(errors.isEmpty() ? null : errors)
          .build();
    } catch (KafkaAdminException e) {
      throw e;
    } catch (Exception e) {
      log.error("Failed to replay failed MCPs", e);
      throw new KafkaAdminException("Failed to replay failed MCPs: " + e.getMessage(), e);
    }
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  /**
   * Resolves a topic alias to its actual topic name.
   *
   * @param alias the topic alias
   * @return the topic name
   * @throws KafkaAdminException.InvalidAliasException if the alias is invalid
   */
  private String resolveTopicName(String alias) {
    return topicConvention
        .getTopicNameForAlias(alias)
        .orElseThrow(
            () ->
                new KafkaAdminException.InvalidAliasException(
                    alias, topicConvention.getAllAliases()));
  }

  /**
   * Fetches offsets for the given partitions using a single batched admin call.
   *
   * @param partitions the set of topic partitions to fetch offsets for
   * @param spec the offset specification (earliest, latest, etc.)
   * @return map of topic partition to offset
   * @throws InterruptedException if the operation is interrupted
   * @throws ExecutionException if the operation fails
   * @throws TimeoutException if the operation times out
   */
  private Map<TopicPartition, Long> fetchOffsets(Set<TopicPartition> partitions, OffsetSpec spec)
      throws InterruptedException, ExecutionException, TimeoutException {
    Map<TopicPartition, OffsetSpec> offsetSpecMap =
        partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> spec));
    return adminClient
        .listOffsets(offsetSpecMap)
        .all()
        .get(timeoutSeconds, TimeUnit.SECONDS)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
  }

  private Map<TopicPartition, Long> calculateTargetOffsets(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets, String strategy, String value) {
    Map<TopicPartition, Long> targetOffsets = new HashMap<>();

    try {
      if ("earliest".equals(strategy)) {
        targetOffsets.putAll(fetchOffsets(currentOffsets.keySet(), OffsetSpec.earliest()));
      } else if ("latest".equals(strategy)) {
        targetOffsets.putAll(fetchOffsets(currentOffsets.keySet(), OffsetSpec.latest()));
      } else if ("to-offset".equals(strategy)) {
        if (value == null) {
          throw new IllegalArgumentException("Value is required for to-offset strategy");
        }
        long targetOffset = Long.parseLong(value);
        for (TopicPartition tp : currentOffsets.keySet()) {
          targetOffsets.put(tp, targetOffset);
        }
      } else if ("shift-by".equals(strategy)) {
        if (value == null) {
          throw new IllegalArgumentException("Value is required for shift-by strategy");
        }
        long shift = Long.parseLong(value);
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
          long newOffset = entry.getValue().offset() + shift;
          targetOffsets.put(entry.getKey(), Math.max(0, newOffset));
        }
      } else {
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.error("Failed to calculate target offsets", e);
      throw new KafkaAdminException("Failed to calculate target offsets: " + e.getMessage(), e);
    }

    return targetOffsets;
  }
}
