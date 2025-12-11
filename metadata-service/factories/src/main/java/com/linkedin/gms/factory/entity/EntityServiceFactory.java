/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.entity;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Slf4j
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
      @Qualifier("configurationProvider") ConfigurationProvider configurationProvider,
      @Value("${featureFlags.showBrowseV2}") final boolean enableBrowsePathV2,
      @Value("${featureFlags.cdcModeChangeLog}") final boolean enableCDCModeChangeLog,
      final List<ThrottleSensor> throttleSensors) {

    FeatureFlags featureFlags = configurationProvider.getFeatureFlags();

    EntityServiceImpl entityService =
        new EntityServiceImpl(
            aspectDao,
            eventProducer,
            featureFlags.isAlwaysEmitChangeLog(),
            featureFlags.isCdcModeChangeLog(),
            featureFlags.getPreProcessHooks(),
            _ebeanMaxTransactionRetry,
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
}
