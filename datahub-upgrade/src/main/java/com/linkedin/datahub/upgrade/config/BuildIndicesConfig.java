package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.postgres.PostgresPgSearchEntitySchema;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.Database;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/** Blocking upgrades unrelated to rebuilding Elasticsearch/OpenSearch indices. */
@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
public class BuildIndicesConfig {

  /**
   * After {@link KafkaSetupConfig}; before {@link ElasticsearchBuildIndicesBlockingConfig} when ES
   * is enabled.
   */
  @Order(2)
  @Bean(name = "postgresPgSearchEntitySchema")
  @ConditionalOnProperty(
      prefix = "postgres.pg-search.entity",
      name = "enabled",
      havingValue = "true")
  public BlockingSystemUpgrade postgresPgSearchEntitySchema(
      @Qualifier("ebeanServer") final Database ebeanServer,
      final PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Qualifier("entityRegistry") final EntityRegistry entityRegistry) {
    return new PostgresPgSearchEntitySchema(
        ebeanServer, postgresSqlSetupProperties, entityRegistry);
  }
}
