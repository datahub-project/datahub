package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.kafka.KafkaSetup;
import com.linkedin.gms.factory.aws.AwsClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabledCondition;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.Order;

@Configuration
@Conditional(KafkaMessagingEnabledCondition.class)
// KafkaSetup steps build admin clients, which under MSK IAM need the shared AWS credentials.
@Import(AwsClientFactory.class)
public class KafkaSetupConfig {

  @Autowired private OperationContext opContext;

  @Order(1) // Before pgSearch entity schema (when enabled) and BuildIndices
  @Bean(name = "kafkaSetup")
  @Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
  @DependsOn("defaultAwsCredentialsProvider")
  public BlockingSystemUpgrade kafkaSetup(
      final ConfigurationProvider configurationProvider, KafkaProperties properties) {
    return new KafkaSetup(opContext, configurationProvider.getKafka(), properties);
  }
}
