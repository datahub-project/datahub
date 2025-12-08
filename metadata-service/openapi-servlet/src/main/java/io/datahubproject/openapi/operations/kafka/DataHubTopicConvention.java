package io.datahubproject.openapi.operations.kafka;

import com.linkedin.mxe.TopicConvention;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Wrapper around {@link TopicConvention} that provides additional helper methods for topic
 * validation and alias resolution specific to DataHub topics and consumer groups.
 *
 * <p>This class provides human-readable aliases for DataHub topics (e.g., "mcp" for the
 * MetadataChangeProposal topic) and methods to validate whether a topic or consumer group is a
 * known DataHub resource.
 */
public class DataHubTopicConvention {

  @Nonnull private final TopicConvention topicConvention;

  /** Set of known DataHub consumer group IDs. */
  @Getter @Nonnull private final Set<String> knownConsumerGroupIds;

  /**
   * Creates a new DataHubTopicConvention with topic convention only (no consumer groups).
   *
   * @param topicConvention the topic convention
   */
  public DataHubTopicConvention(@Nonnull TopicConvention topicConvention) {
    this(topicConvention, Set.of());
  }

  /**
   * Creates a new DataHubTopicConvention with topic convention and known consumer group IDs.
   *
   * @param topicConvention the topic convention
   * @param consumerGroupIds the set of known consumer group IDs
   */
  public DataHubTopicConvention(
      @Nonnull TopicConvention topicConvention, @Nonnull Set<String> consumerGroupIds) {
    this.topicConvention = topicConvention;
    this.knownConsumerGroupIds = consumerGroupIds != null ? Set.copyOf(consumerGroupIds) : Set.of();
  }

  // ============================================================================
  // Topic Aliases
  // ============================================================================

  /** Human-readable alias for MCP topic */
  public static final String ALIAS_MCP = "mcp";

  /** Human-readable alias for failed MCP topic */
  public static final String ALIAS_MCP_FAILED = "mcp-failed";

  /** Human-readable alias for MCL versioned topic */
  public static final String ALIAS_MCL_VERSIONED = "mcl-versioned";

  /** Human-readable alias for MCL timeseries topic */
  public static final String ALIAS_MCL_TIMESERIES = "mcl-timeseries";

  /** Human-readable alias for platform event topic */
  public static final String ALIAS_PLATFORM_EVENT = "platform-event";

  /** Human-readable alias for upgrade history topic */
  public static final String ALIAS_UPGRADE_HISTORY = "upgrade-history";

  // ============================================================================
  // Delegate Methods
  // ============================================================================

  /** Returns the underlying TopicConvention. */
  @Nonnull
  public TopicConvention getTopicConvention() {
    return topicConvention;
  }

  // ============================================================================
  // Topic Validation and Lookup Methods
  // ============================================================================

  /**
   * Returns a map of alias to topic name for all DataHub topics. This is the single source of truth
   * for all topic alias mappings.
   *
   * @return map of alias to topic name
   */
  public Map<String, String> getAliasToTopicNameMap() {
    return Map.of(
        ALIAS_MCP, topicConvention.getMetadataChangeProposalTopicName(),
        ALIAS_MCP_FAILED, topicConvention.getFailedMetadataChangeProposalTopicName(),
        ALIAS_MCL_VERSIONED, topicConvention.getMetadataChangeLogVersionedTopicName(),
        ALIAS_MCL_TIMESERIES, topicConvention.getMetadataChangeLogTimeseriesTopicName(),
        ALIAS_PLATFORM_EVENT, topicConvention.getPlatformEventTopicName(),
        ALIAS_UPGRADE_HISTORY, topicConvention.getDataHubUpgradeHistoryTopicName());
  }

  /**
   * Returns all known DataHub topic names as a set.
   *
   * @return set of all DataHub topic names
   */
  public Set<String> getAllDataHubTopicNames() {
    return Set.copyOf(getAliasToTopicNameMap().values());
  }

  /**
   * Checks if a topic name is a known DataHub topic.
   *
   * @param topicName the topic name to check
   * @return true if the topic is a known DataHub topic
   */
  public boolean isDataHubTopic(String topicName) {
    return topicName != null && getAliasToTopicNameMap().containsValue(topicName);
  }

  /**
   * Returns the topic name for a given alias (mcp, mcl-versioned, etc.).
   *
   * @param alias the alias to look up (case-insensitive)
   * @return the topic name, or empty if the alias is unknown
   */
  public Optional<String> getTopicNameForAlias(String alias) {
    if (alias == null) {
      return Optional.empty();
    }
    String normalizedAlias = alias.toLowerCase().trim();
    return Optional.ofNullable(getAliasToTopicNameMap().get(normalizedAlias));
  }

  /**
   * Returns the alias for a given topic name.
   *
   * @param topicName the topic name to look up
   * @return the alias, or empty if the topic is not a known DataHub topic
   */
  public Optional<String> getAliasForTopicName(String topicName) {
    if (topicName == null) {
      return Optional.empty();
    }
    return getAliasToTopicNameMap().entrySet().stream()
        .filter(entry -> entry.getValue().equals(topicName))
        .map(Map.Entry::getKey)
        .findFirst();
  }

  /**
   * Returns all valid topic aliases.
   *
   * @return set of all valid aliases
   */
  public Set<String> getAllAliases() {
    return getAliasToTopicNameMap().keySet();
  }

  // ============================================================================
  // Consumer Group Validation Methods
  // ============================================================================

  /**
   * Checks if a consumer group ID is a known DataHub consumer group.
   *
   * @param groupId the consumer group ID to check
   * @return true if the consumer group is a known DataHub consumer group
   */
  public boolean isDataHubConsumerGroup(String groupId) {
    return groupId != null && knownConsumerGroupIds.contains(groupId);
  }
}
