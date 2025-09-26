package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.cdc.debezium.DebeziumCDCSourceSetup;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CDCSourceConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/**
 * Spring configuration for CDC setup upgrade. Creates the appropriate CDC setup implementation
 * based on the configured type.
 */
@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
@Slf4j
public class CDCSetupConfig {

  @Autowired private OperationContext opContext;

  /**
   * Creates the CDC setup upgrade bean. This runs after KafkaSetup (@Order(1)) to ensure Kafka is
   * ready.
   *
   * @param configurationProvider Provides access to DataHub configuration
   * @param kafkaProperties Kafka connection properties
   * @return CDC setup implementation or null if CDC is disabled or misconfigured
   */
  @Order(Integer.MAX_VALUE)
  @Bean(name = "cdcSetup")
  public BlockingSystemUpgrade cdcSetup(
      final ConfigurationProvider configurationProvider, final KafkaProperties kafkaProperties) {

    // Check if CDC processing configuration exists at all
    if (configurationProvider.getMclProcessing() == null
        || configurationProvider.getMclProcessing().getCdcSource() == null
        || !configurationProvider.getMclProcessing().getCdcSource().isEnabled()
        || !configurationProvider.getMclProcessing().getCdcSource().isConfigureSource()) {
      log.info("CDC processing is not configured - skipping CDC setup");
      return null;
    }

    // Get CDC source configuration - let the setup class validate it
    CDCSourceConfiguration cdcSourceConfig =
        configurationProvider.getMclProcessing().getCdcSource();
    String cdcType = cdcSourceConfig.getType();

    log.info("CDC processing configuration found with type '{}' - attempting setup", cdcType);

    try {
      BlockingSystemUpgrade setup;

      switch (cdcType.toLowerCase()) {
        case "debezium":
        case "debezium-kafka-connector":
          setup =
              new DebeziumCDCSourceSetup(
                  opContext,
                  cdcSourceConfig,
                  configurationProvider.getEbean(),
                  configurationProvider.getKafka(),
                  kafkaProperties);
          break;

        default:
          log.error(
              "Unsupported CDC type '{}' - supported types: debezium, debezium-kafka-connector",
              cdcType);
          return null;
      }

      log.info("Successfully created CDC setup of type '{}': {}", cdcType, setup.id());
      return setup;

    } catch (Exception e) {
      log.error("Failed to create CDC setup for type '{}': {}", cdcType, e.getMessage(), e);
      return null; // Return null to prevent blocking the upgrade
    }
  }
}
