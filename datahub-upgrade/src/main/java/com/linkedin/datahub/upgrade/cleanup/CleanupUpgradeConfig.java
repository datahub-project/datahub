package com.linkedin.datahub.upgrade.cleanup;

import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.config.OpenTelemetryConfig;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetupArgs;
import com.linkedin.datahub.upgrade.sqlsetup.config.SqlSetupConfig;
import com.linkedin.datahub.upgrade.sqlsetup.config.SqlSetupEbeanFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.utils.EnvironmentUtils;
import io.ebean.Database;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

/**
 * Spring configuration for the Cleanup upgrade. Loads the minimal set of beans needed to tear down
 * Elasticsearch indices, Kafka topics, and the SQL database.
 */
@Slf4j
@Configuration
@Import({
  MetricsAutoConfiguration.class,
  OpenTelemetryConfig.class,
  SqlSetupConfig.class,
  SqlSetupEbeanFactory.class
})
@ComponentScan(
    basePackages = {
      "com.linkedin.gms.factory.config",
      "com.linkedin.gms.factory.common",
      "com.linkedin.gms.factory.entity",
      "com.linkedin.gms.factory.entityclient",
      "com.linkedin.gms.factory.plugins",
      "com.linkedin.gms.factory.entityregistry",
      "com.linkedin.gms.factory.search",
      "com.linkedin.gms.factory.timeseries",
      "com.linkedin.gms.factory.context",
      "com.linkedin.gms.factory.system_telemetry"
    },
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {})
    })
public class CleanupUpgradeConfig {

  @Autowired(required = false)
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;

  @Autowired(required = false)
  private ConfigurationProvider configurationProvider;

  @Autowired(required = false)
  private KafkaProperties kafkaProperties;

  @Autowired(required = false)
  @Qualifier("ebeanServer")
  private Database ebeanServer;

  @Autowired(required = false)
  @Qualifier("sqlSetupArgs")
  private SqlSetupArgs sqlSetupArgs;

  @Bean(name = "cleanup")
  @Nonnull
  public Cleanup createCleanup() {
    boolean esEnabled = EnvironmentUtils.getBoolean("CLEANUP_ELASTICSEARCH_ENABLED", true);
    boolean kafkaEnabled = EnvironmentUtils.getBoolean("CLEANUP_KAFKA_ENABLED", true);
    boolean sqlEnabled = EnvironmentUtils.getBoolean("CLEANUP_SQL_ENABLED", true);

    List<UpgradeStep> steps = new ArrayList<>();

    // Order: ES first (so indices aren't queried during DB drop), then Kafka, then SQL
    if (esEnabled && esComponents != null && configurationProvider != null) {
      steps.add(new DeleteElasticsearchIndicesStep(esComponents, configurationProvider));
      log.info("Elasticsearch cleanup step enabled");
    } else if (esEnabled) {
      log.warn("Elasticsearch cleanup requested but ES components not available — skipping");
    }

    if (kafkaEnabled && configurationProvider != null && kafkaProperties != null) {
      KafkaConfiguration kafkaConfig = configurationProvider.getKafka();
      steps.add(new DeleteKafkaTopicsStep(kafkaConfig, kafkaProperties));
      log.info("Kafka cleanup step enabled");
    } else if (kafkaEnabled) {
      log.warn("Kafka cleanup requested but Kafka config not available — skipping");
    }

    if (sqlEnabled && ebeanServer != null && sqlSetupArgs != null) {
      steps.add(new DropDatabaseStep(ebeanServer, sqlSetupArgs));
      log.info("SQL cleanup step enabled");
    } else if (sqlEnabled) {
      log.warn("SQL cleanup requested but database not available — skipping");
    }

    return new Cleanup(steps);
  }
}
