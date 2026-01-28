package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.browsepaths.BackfillBrowsePathsV2;
import com.linkedin.datahub.upgrade.system.browsepaths.BackfillIcebergBrowsePathsV2;
import com.linkedin.datahub.upgrade.system.dataprocessinstances.BackfillDataProcessInstances;
import com.linkedin.datahub.upgrade.system.entities.RemoveQueryEdges;
import com.linkedin.datahub.upgrade.system.entityconsistency.FixEntityConsistency;
import com.linkedin.datahub.upgrade.system.ingestion.BackfillIngestionSourceInfoIndices;
import com.linkedin.datahub.upgrade.system.kafka.KafkaNonBlockingSetup;
import com.linkedin.datahub.upgrade.system.policyfields.BackfillPolicyFields;
import com.linkedin.datahub.upgrade.system.schemafield.GenerateSchemaFieldsFromSchemaMetadata;
import com.linkedin.datahub.upgrade.system.schemafield.MigrateSchemaFieldDocIds;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.config.search.BulkDeleteConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class NonBlockingConfigs {

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
  public NonBlockingSystemUpgrade backfillIcebergBrowsePathsV2(
      final OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      @Value("${systemUpdate.browsePathsV2Iceberg.enabled}") final boolean enabled,
      @Value("${systemUpdate.browsePathsV2Iceberg.batchSize}") final Integer batchSize) {
    return new BackfillIcebergBrowsePathsV2(
        opContext, entityService, searchService, enabled, batchSize);
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
  public NonBlockingSystemUpgrade backfillIngestionSourceInfoIndices(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      @Value("${systemUpdate.ingestionIndices.enabled}") final boolean enabled,
      @Value("${systemUpdate.ingestionIndices.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.ingestionIndices.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.ingestionIndices.limit}") final Integer limit) {
    return new BackfillIngestionSourceInfoIndices(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
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

  @Bean
  public NonBlockingSystemUpgrade schemaFieldsFromSchemaMetadata(
      @Qualifier("systemOperationContext") final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      // SYSTEM_UPDATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA_ENABLED
      @Value("${systemUpdate.schemaFieldsFromSchemaMetadata.enabled}") final boolean enabled,
      // SYSTEM_UPDATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA_BATCH_SIZE
      @Value("${systemUpdate.schemaFieldsFromSchemaMetadata.batchSize}") final Integer batchSize,
      // SYSTEM_UPDATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA_DELAY_MS
      @Value("${systemUpdate.schemaFieldsFromSchemaMetadata.delayMs}") final Integer delayMs,
      // SYSTEM_UPDATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA_LIMIT
      @Value("${systemUpdate.schemaFieldsFromSchemaMetadata.limit}") final Integer limit) {
    return new GenerateSchemaFieldsFromSchemaMetadata(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }

  @Bean
  public NonBlockingSystemUpgrade schemaFieldsDocIds(
      @Qualifier("systemOperationContext") final OperationContext opContext,
      @Qualifier("baseElasticSearchComponents")
          final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components,
      final EntityService<?> entityService,
      // ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED
      @Value("${elasticsearch.index.docIds.schemaField.hashIdEnabled}") final boolean hashEnabled,
      // SYSTEM_UPDATE_SCHEMA_FIELDS_DOC_IDS_ENABLED
      @Value("${systemUpdate.schemaFieldsDocIds.enabled}") final boolean enabled,
      // SYSTEM_UPDATE_SCHEMA_FIELDS_DOC_IDS_BATCH_SIZE
      @Value("${systemUpdate.schemaFieldsDocIds.batchSize}") final Integer batchSize,
      // SYSTEM_UPDATE_SCHEMA_FIELDS_DOC_IDS_DELAY_MS
      @Value("${systemUpdate.schemaFieldsDocIds.delayMs}") final Integer delayMs,
      // SYSTEM_UPDATE_SCHEMA_FIELDS_DOC_IDS_LIMIT
      @Value("${systemUpdate.schemaFieldsDocIds.limit}") final Integer limit) {
    return new MigrateSchemaFieldDocIds(
        opContext, components, entityService, enabled && hashEnabled, batchSize, delayMs, limit);
  }

  @Autowired private OperationContext opContext;

  @Bean
  public NonBlockingSystemUpgrade kafkaSetupNonBlocking(
      final ConfigurationProvider configurationProvider, KafkaProperties properties) {
    return new KafkaNonBlockingSetup(opContext, configurationProvider.getKafka(), properties);
  }

  @Bean
  public NonBlockingSystemUpgrade fixEntityConsistency(
      @Qualifier("systemOperationContext") final OperationContext opContext,
      final EntityService<?> entityService,
      @Qualifier("consistencyService") final ConsistencyService consistencyService,
      final ConfigurationProvider configurationProvider) {
    return new FixEntityConsistency(
        opContext,
        entityService,
        consistencyService,
        configurationProvider.getSystemUpdate().getEntityConsistency());
  }
}
