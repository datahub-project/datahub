package com.linkedin.gms.factory.entity;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class EntityServiceFactory {

  @Value("${EBEAN_MAX_TRANSACTION_RETRY:#{null}}")
  private Integer _ebeanMaxTransactionRetry;

  @Bean(name = "entityService")
  @DependsOn({"entityAspectDao", "kafkaEventProducer"})
  @Nonnull
  protected EntityService<ChangeItemImpl> createInstance(
      @Qualifier("kafkaEventProducer") final KafkaEventProducer eventProducer,
      @Qualifier("entityAspectDao") final AspectDao aspectDao,
      final ConfigurationProvider configurationProvider,
      @Value("${featureFlags.showBrowseV2}") final boolean enableBrowsePathV2) {

    FeatureFlags featureFlags = configurationProvider.getFeatureFlags();

    return new EntityServiceImpl(
        aspectDao,
        eventProducer,
        featureFlags.isAlwaysEmitChangeLog(),
        featureFlags.getPreProcessHooks(),
        _ebeanMaxTransactionRetry,
        enableBrowsePathV2);
  }
}
