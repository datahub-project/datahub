package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.SystemUpdate;
import com.linkedin.datahub.upgrade.system.SystemUpdateBlocking;
import com.linkedin.datahub.upgrade.system.SystemUpdateNonBlocking;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.BootstrapMCP;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.DataHubStartupStep;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.DataHubKafkaProducerFactory;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.metadata.client.SystemJavaEntityClient;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.TopicConvention;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;

@Slf4j
@Configuration
@Conditional(SystemUpdateCondition.class)
public class SystemUpdateConfig {

  @Bean(name = "systemUpdate")
  public SystemUpdate systemUpdate(
      final List<BlockingSystemUpgrade> blockingSystemUpgrades,
      final List<NonBlockingSystemUpgrade> nonBlockingSystemUpgrades,
      final DataHubStartupStep dataHubStartupStep,
      @Qualifier("bootstrapMCPBlocking") @NonNull final BootstrapMCP bootstrapMCPBlocking,
      @Qualifier("bootstrapMCPNonBlocking") @NonNull final BootstrapMCP bootstrapMCPNonBlocking) {
    return new SystemUpdate(
        blockingSystemUpgrades,
        nonBlockingSystemUpgrades,
        dataHubStartupStep,
        bootstrapMCPBlocking,
        bootstrapMCPNonBlocking);
  }

  @Bean(name = "systemUpdateBlocking")
  public SystemUpdateBlocking systemUpdateBlocking(
      final List<BlockingSystemUpgrade> blockingSystemUpgrades,
      final DataHubStartupStep dataHubStartupStep,
      @Qualifier("bootstrapMCPBlocking") @NonNull final BootstrapMCP bootstrapMCPBlocking) {
    return new SystemUpdateBlocking(
        blockingSystemUpgrades, dataHubStartupStep, bootstrapMCPBlocking);
  }

  @Bean(name = "systemUpdateNonBlocking")
  public SystemUpdateNonBlocking systemUpdateNonBlocking(
      final List<NonBlockingSystemUpgrade> nonBlockingSystemUpgrades,
      @Qualifier("bootstrapMCPNonBlocking") @NonNull final BootstrapMCP bootstrapMCPNonBlocking,
      final org.springframework.boot.ApplicationArguments applicationArguments) {

    List<NonBlockingSystemUpgrade> filteredUpgrades = nonBlockingSystemUpgrades;

    // Filter by classname(s) if specified via -n or --nonblocking-classname (comma-delimited)
    List<String> classnameValues = applicationArguments.getOptionValues("n");
    if (classnameValues == null || classnameValues.isEmpty()) {
      classnameValues = applicationArguments.getOptionValues("nonblocking-classname");
    }

    if (classnameValues != null && !classnameValues.isEmpty()) {
      Set<String> targetClassnames =
          Arrays.stream(classnameValues.get(0).split(","))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .collect(Collectors.toSet());

      log.info("Filtering non-blocking upgrades to run only: {}", targetClassnames);

      filteredUpgrades =
          nonBlockingSystemUpgrades.stream()
              .filter(upgrade -> targetClassnames.contains(upgrade.id()))
              .toList();

      if (filteredUpgrades.isEmpty()) {
        log.warn(
            "No non-blocking upgrades found matching '{}'. Available upgrades: {}",
            targetClassnames,
            nonBlockingSystemUpgrades.stream().map(NonBlockingSystemUpgrade::id).toList());
      } else if (filteredUpgrades.size() < targetClassnames.size()) {
        Set<String> foundIds =
            filteredUpgrades.stream().map(NonBlockingSystemUpgrade::id).collect(Collectors.toSet());
        Set<String> notFound = new HashSet<>(targetClassnames);
        notFound.removeAll(foundIds);
        log.warn("Some requested upgrades were not found: {}", notFound);
      }
    }

    return new SystemUpdateNonBlocking(filteredUpgrades, bootstrapMCPNonBlocking);
  }

  @Value("#{systemEnvironment['DATAHUB_REVISION'] ?: '0'}")
  private String revision;

  @Bean(name = "revision")
  public String getRevision() {
    return revision;
  }

  @Bean
  public DataHubStartupStep dataHubStartupStep(
      @Qualifier("duheKafkaEventProducer") final KafkaEventProducer kafkaEventProducer,
      final GitVersion gitVersion,
      @Qualifier("revision") String revision) {
    return new DataHubStartupStep(
        kafkaEventProducer, String.format("%s-%s", gitVersion.getVersion(), revision));
  }

  @Autowired
  @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN)
  private TopicConvention topicConvention;

  @Autowired private KafkaHealthChecker kafkaHealthChecker;

  @Bean(name = "duheKafkaEventProducer")
  protected KafkaEventProducer duheKafkaEventProducer(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      KafkaProperties properties,
      @Qualifier("duheSchemaRegistryConfig")
          KafkaConfiguration.SerDeKeyValueConfig duheSchemaRegistryConfig,
      MetricUtils metricUtils) {
    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    Producer<String, IndexedRecord> producer =
        new KafkaProducer<>(
            DataHubKafkaProducerFactory.buildProducerProperties(
                duheSchemaRegistryConfig, kafkaConfiguration, properties));
    return new KafkaEventProducer(producer, topicConvention, kafkaHealthChecker, metricUtils);
  }

  /**
   * The ReindexDataJobViaNodesCLLConfig step requires publishing to MCL. Overriding the default
   * producer with this special producer which doesn't require an active registry.
   *
   * <p>Use when INTERNAL registry and is SYSTEM_UPDATE
   *
   * <p>This forces this producer into the EntityService
   */
  @Primary
  @Bean(name = "kafkaEventProducer")
  @ConditionalOnProperty(
      name = "kafka.schemaRegistry.type",
      havingValue = InternalSchemaRegistryFactory.TYPE)
  protected KafkaEventProducer kafkaEventProducer(
      @Qualifier("duheKafkaEventProducer") KafkaEventProducer kafkaEventProducer) {
    return kafkaEventProducer;
  }

  @Primary
  @Bean(name = "schemaRegistryConfig")
  @ConditionalOnProperty(
      name = "kafka.schemaRegistry.type",
      havingValue = InternalSchemaRegistryFactory.TYPE)
  protected KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig(
      @Qualifier("duheSchemaRegistryConfig")
          KafkaConfiguration.SerDeKeyValueConfig duheSchemaRegistryConfig) {
    return duheSchemaRegistryConfig;
  }

  /**
   * Override EntityService bean in the datahub-upgrade context to use system update CDC mode
   * configuration. Only active when system update is running blocking mode operations.
   */
  @Primary
  @Bean(name = "entityService")
  @Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
  @Nonnull
  protected EntityService<ChangeItemImpl> createEntityServiceWithSystemUpdateCDCMode(
      @Qualifier("kafkaEventProducer") final KafkaEventProducer eventProducer,
      @Qualifier("entityAspectDao") final AspectDao aspectDao,
      @Qualifier("configurationProvider") ConfigurationProvider configurationProvider,
      @Value("${featureFlags.showBrowseV2}") final boolean enableBrowsePathV2,
      @Value("${EBEAN_MAX_TRANSACTION_RETRY:#{null}}") final Integer ebeanMaxTransactionRetry,
      final List<ThrottleSensor> throttleSensors) {

    FeatureFlags featureFlags = configurationProvider.getFeatureFlags();
    boolean systemUpdateCDCMode = configurationProvider.getSystemUpdate().isCdcMode();

    log.info(
        "Creating EntityService with system update CDC mode override: {}", systemUpdateCDCMode);

    EntityServiceImpl entityService =
        new EntityServiceImpl(
            aspectDao,
            eventProducer,
            featureFlags.isAlwaysEmitChangeLog(),
            systemUpdateCDCMode, // Use system update CDC mode
            featureFlags.getPreProcessHooks(),
            ebeanMaxTransactionRetry,
            enableBrowsePathV2);

    if (throttleSensors != null
        && !throttleSensors.isEmpty()
        && configurationProvider
            .getMetadataChangeProposal()
            .getThrottle()
            .getComponents()
            .getApiRequests()
            .isEnabled()) {
      log.info("API Requests Throttle Enabled");
      throttleSensors.forEach(sensor -> sensor.addCallback(entityService::handleThrottleEvent));
    } else {
      log.info("API Requests Throttle Disabled");
    }

    return entityService;
  }

  /**
   * Override SystemEntityClient bean in the datahub-upgrade context to use the Java implementation.
   * Only active when system update is running blocking mode operations.
   */
  @Primary
  @Bean(name = "systemEntityClient")
  @Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
  @Nonnull
  protected SystemEntityClient systemEntityClient(
      @Lazy final @Qualifier("entityService") EntityService<?> entityService,
      @Lazy final @Qualifier("deleteEntityService") DeleteEntityService deleteEntityService,
      final @Qualifier("searchService") SearchService searchService,
      final @Qualifier("entitySearchService") EntitySearchService entitySearchService,
      final @Qualifier("cachingEntitySearchService") CachingEntitySearchService
              cachingEntitySearchService,
      final @Qualifier("timeseriesAspectService") TimeseriesAspectService timeseriesAspectService,
      final @Qualifier("relationshipSearchService") LineageSearchService lineageSearchService,
      @Lazy final @Qualifier("kafkaEventProducer") EventProducer eventProducer,
      @Lazy final RollbackService rollbackService,
      final EntityClientCacheConfig entityClientCacheConfig,
      final EntityClientConfig entityClientConfig,
      final MetricUtils metricUtils) {

    log.info("Creating SystemEntityClient with Java implementation for blocking system update");

    return new SystemJavaEntityClient(
        entityService,
        deleteEntityService,
        entitySearchService,
        cachingEntitySearchService,
        searchService,
        lineageSearchService,
        timeseriesAspectService,
        rollbackService,
        eventProducer,
        entityClientCacheConfig,
        entityClientConfig,
        metricUtils);
  }
}
