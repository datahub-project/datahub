package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.dataprocessinstances.BackfillDataProcessInstances;
import com.linkedin.datahub.upgrade.system.entities.RemoveQueryEdges;
import com.linkedin.datahub.upgrade.system.schemafield.MigrateSchemaFieldDocIds;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.config.search.BulkDeleteConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Non-blocking system upgrades that require an Elasticsearch/OpenSearch write path. The rest live
 * in {@link NonBlockingConfigs}.
 */
@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
@ConditionalOnProperty(
    prefix = "elasticsearch",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class ElasticsearchNonBlockingSystemUpgradeConfig {

  @Bean
  public NonBlockingSystemUpgrade removeQueryEdges(
      final OperationContext opContext,
      final ConfigurationProvider configurationProvider,
      EntityService<?> entityService,
      ESWriteDAO esWriteDao,
      @Value("${systemUpdate.removeQueryEdges.enabled}") final boolean enabled,
      @Value("${systemUpdate.removeQueryEdges.numRetries}") final int numRetries) {
    BulkDeleteConfiguration override =
        configurationProvider.getElasticSearch().getBulkDelete().toBuilder()
            .numRetries(numRetries)
            .build();
    return new RemoveQueryEdges(opContext, entityService, esWriteDao, enabled, override);
  }

  @Bean
  public NonBlockingSystemUpgrade backfillProcessInstancesHasRunEvents(
      final OperationContext opContext,
      final EntityService<?> entityService,
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
  public NonBlockingSystemUpgrade schemaFieldsDocIds(
      @Qualifier("systemOperationContext") final OperationContext opContext,
      @Qualifier("baseElasticSearchComponents")
          final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components,
      final EntityService<?> entityService,
      @Value("${elasticsearch.index.docIds.schemaField.hashIdEnabled}") final boolean hashEnabled,
      @Value("${systemUpdate.schemaFieldsDocIds.enabled}") final boolean enabled,
      @Value("${systemUpdate.schemaFieldsDocIds.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.schemaFieldsDocIds.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.schemaFieldsDocIds.limit}") final Integer limit) {
    return new MigrateSchemaFieldDocIds(
        opContext, components, entityService, enabled && hashEnabled, batchSize, delayMs, limit);
  }
}
