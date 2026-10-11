package com.linkedin.gms.factory.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.messaging.KafkaMessagingDisabled;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.postgres.EbeanPostgresMetadataQueueStore;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@KafkaMessagingDisabled
@ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
public class MetadataQueueStoreFactory {

  @Bean
  public MetadataQueueStore metadataQueueStore(
      @Qualifier("pgQueueEbeanServer") @Nonnull Database database,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      ConfigurationProvider configurationProvider,
      ObjectMapper objectMapper) {
    return new EbeanPostgresMetadataQueueStore(
        database, postgresSqlSetupProperties, configurationProvider.getKafka(), objectMapper);
  }
}
