package com.linkedin.datahub.upgrade.system.elasticsearch;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.shared.ElasticSearchUpgradeUtils;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.IncrementalReindexAliasSwapStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.IncrementalReindexCatchUpStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.IncrementalReindexPreAliasSwapLagStep;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Ordered post–Phase-1 work for incremental reindex: catch-up, optional MCL lag gate, optional
 * automatic alias swap. Exposed as a single non-blocking upgrade so steps always run in sequence.
 */
public class IncrementalReindex implements NonBlockingSystemUpgrade {

  public static final String ID = "IncrementalReindex";

  private final List<UpgradeStep> steps;

  public IncrementalReindex(
      SystemMetadataService systemMetadataService,
      TimeseriesAspectService timeseriesAspectService,
      EntitySearchService entitySearchService,
      GraphService graphService,
      ConfigurationProvider configurationProvider,
      AspectDao aspectDao,
      OperationContext opContext,
      EntityService<?> entityService,
      String upgradeVersion,
      KafkaProperties kafkaProperties,
      String metadataChangeLogKafkaConsumerGroupId) {

    BuildIndicesConfiguration buildIndices =
        configurationProvider.getElasticSearch().getBuildIndices();
    boolean autoAliasSwap = buildIndices.isAutoAliasSwapEnabled();

    Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
    if (configurationProvider.getStructuredProperties().isSystemUpdateEnabled()) {
      structuredProperties =
          ElasticSearchUpgradeUtils.getActiveStructuredPropertiesDefinitions(aspectDao);
    } else {
      structuredProperties = Set.of();
    }

    List<ElasticSearchIndexed> indexedServices =
        ElasticSearchUpgradeUtils.createElasticSearchIndexedServices(
            graphService, entitySearchService, systemMetadataService, timeseriesAspectService);

    TopicsConfiguration topicsConfiguration = configurationProvider.getKafka().getTopics();
    Map<String, TopicsConfiguration.TopicConfiguration> topicMap = topicsConfiguration.getTopics();
    if (topicMap == null) {
      throw new IllegalStateException("Kafka topics configuration is not initialized");
    }
    KafkaConfiguration kafkaConfiguration = configurationProvider.getKafka();

    steps = new ArrayList<>();
    steps.add(
        new IncrementalReindexCatchUpStep(opContext, entityService, aspectDao, upgradeVersion));

    if (autoAliasSwap) {
      steps.add(
          new IncrementalReindexPreAliasSwapLagStep(
              upgradeVersion,
              buildIndices.getPreAliasSwapMaxMclLagTotal(),
              buildIndices.getPreAliasSwapLagStepRetries(),
              buildIndices.getPreAliasSwapLagRetryInitialBackoffMs(),
              buildIndices.getPreAliasSwapLagRetryMaxBackoffMs(),
              metadataChangeLogKafkaConsumerGroupId,
              kafkaConfiguration,
              kafkaProperties,
              topicMap));
      steps.add(
          new IncrementalReindexAliasSwapStep(
              opContext, entityService, indexedServices, structuredProperties, upgradeVersion));
    }
  }

  @Override
  public String id() {
    return ID;
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
