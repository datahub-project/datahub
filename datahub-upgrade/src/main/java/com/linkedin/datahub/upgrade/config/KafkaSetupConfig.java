package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.kafka.KafkaSetup;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
public class KafkaSetupConfig {

  @Autowired private OperationContext opContext;

  @Order(1) // This ensures it runs before BuildIndices (@Order(2))
  @Bean(name = "kafkaSetup")
  public BlockingSystemUpgrade kafkaSetup(
      final ConfigurationProvider configurationProvider, KafkaProperties properties) {
    return new KafkaSetup(opContext, configurationProvider.getKafka(), properties);
  }
}
