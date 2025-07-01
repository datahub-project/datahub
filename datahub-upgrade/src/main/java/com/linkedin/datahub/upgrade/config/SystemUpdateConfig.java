package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.SystemUpdate;
import com.linkedin.datahub.upgrade.system.SystemUpdateBlocking;
import com.linkedin.datahub.upgrade.system.SystemUpdateNonBlocking;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.BootstrapMCP;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.DataHubStartupStep;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.DataHubKafkaProducerFactory;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.TopicConvention;
import java.util.List;
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
      @Qualifier("bootstrapMCPNonBlocking") @NonNull final BootstrapMCP bootstrapMCPNonBlocking) {
    return new SystemUpdateNonBlocking(nonBlockingSystemUpgrades, bootstrapMCPNonBlocking);
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
          KafkaConfiguration.SerDeKeyValueConfig duheSchemaRegistryConfig) {
    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    Producer<String, IndexedRecord> producer =
        new KafkaProducer<>(
            DataHubKafkaProducerFactory.buildProducerProperties(
                duheSchemaRegistryConfig, kafkaConfiguration, properties));
    return new KafkaEventProducer(producer, topicConvention, kafkaHealthChecker);
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
}
