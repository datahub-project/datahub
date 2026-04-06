package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.browsepaths.BackfillBrowsePathsV2;
import com.linkedin.datahub.upgrade.system.dataprocessinstances.BackfillDataProcessInstances;
import com.linkedin.datahub.upgrade.system.kafka.KafkaNonBlockingSetup;
import com.linkedin.datahub.upgrade.system.policyfields.BackfillPolicyFields;
import com.linkedin.datahub.upgrade.system.retention.IngestRetentionPolicies;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class NonBlockingConfigs {

  @Bean
  public NonBlockingSystemUpgrade backfillBrowsePathsV2(
      final OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      @Value("${systemUpdate.browsePathsV2.enabled}") final boolean enabled,
      @Value("${systemUpdate.browsePathsV2.reprocess.enabled}") final boolean reprocessEnabled,
      @Value("${systemUpdate.browsePathsV2.batchSize}") final Integer batchSize) {
    return new BackfillBrowsePathsV2(
        opContext, entityService, searchService, enabled, reprocessEnabled, batchSize);
  }

  @Bean
  public NonBlockingSystemUpgrade backfillProcessInstancesHasRunEvents(
      final OperationContext opContext,
      EntityService<?> entityService,
      ElasticSearchService elasticSearchService,
      SearchClientShim<?> restHighLevelClient,
      @Value("${systemUpdate.processInstanceHasRunEvents.enabled}") final boolean enabled,
      @Value("${systemUpdate.processInstanceHasRunEvents.reprocess.enabled}")
          boolean reprocessEnabled,
      @Value("${systemUpdate.processInstanceHasRunEvents.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.processInstanceHasRunEvents.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.processInstanceHasRunEvents.totalDays}") Integer totalDays,
      @Value("${systemUpdate.processInstanceHasRunEvents.windowDays}") Integer windowDays) {
    return new BackfillDataProcessInstances(
        opContext,
        entityService,
        elasticSearchService,
        restHighLevelClient,
        enabled,
        reprocessEnabled,
        batchSize,
        delayMs,
        totalDays,
        windowDays);
  }

  @Bean
  public BackfillPolicyFields backfillPolicyFields(
      final OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      @Value("${systemUpdate.policyFields.enabled}") final boolean enabled,
      @Value("${systemUpdate.policyFields.reprocess.enabled}") final boolean reprocessEnabled,
      @Value("${systemUpdate.policyFields.batchSize}") final Integer batchSize) {
    return new BackfillPolicyFields(
        opContext, entityService, searchService, enabled, reprocessEnabled, batchSize);
  }

  @Autowired private OperationContext opContext;

  @Bean
  public NonBlockingSystemUpgrade kafkaSetupNonBlocking(
      final ConfigurationProvider configurationProvider, KafkaProperties properties) {
    return new KafkaNonBlockingSetup(opContext, configurationProvider.getKafka(), properties);
  }

  @Bean
  public NonBlockingSystemUpgrade ingestRetentionPolicies(
      @org.springframework.beans.factory.annotation.Qualifier("retentionService")
          final RetentionService<?> retentionService,
      @org.springframework.beans.factory.annotation.Qualifier("entityService")
          final EntityService<?> entityService,
      @Value("${entityService.retention.enabled}") final boolean enabled,
      @Value("${entityService.retention.applyOnBootstrap}") final boolean applyAfterIngest,
      @Value("${datahub.plugin.retention.path}") final String pluginPath) {
    return new IngestRetentionPolicies(
        retentionService, entityService, enabled, applyAfterIngest, pluginPath);
  }
}
