package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.browsepaths.BackfillBrowsePathsV2;
import com.linkedin.datahub.upgrade.system.browsepaths.BackfillIcebergBrowsePathsV2;
import com.linkedin.datahub.upgrade.system.dataplatforminstances.IngestDataPlatformInstances;
import com.linkedin.datahub.upgrade.system.dataplatforms.IndexDataPlatforms;
import com.linkedin.datahub.upgrade.system.entityconsistency.FixEntityConsistency;
import com.linkedin.datahub.upgrade.system.homepagelinks.MigrateHomePageLinks;
import com.linkedin.datahub.upgrade.system.ingestion.BackfillIngestionSourceInfoIndices;
import com.linkedin.datahub.upgrade.system.ingestion.IngestEntityTypes;
import com.linkedin.datahub.upgrade.system.kafka.KafkaNonBlockingSetup;
import com.linkedin.datahub.upgrade.system.migrations.MigrateAspects;
import com.linkedin.datahub.upgrade.system.policyfields.BackfillPolicyFields;
import com.linkedin.datahub.upgrade.system.restoreindices.RestoreDbtSiblingsIndices;
import com.linkedin.datahub.upgrade.system.restoreindices.columnlineage.RestoreColumnLineageIndices;
import com.linkedin.datahub.upgrade.system.restoreindices.forminfo.RestoreFormInfoIndices;
import com.linkedin.datahub.upgrade.system.restoreindices.glossary.RestoreGlossaryIndices;
import com.linkedin.datahub.upgrade.system.retention.IngestRetentionPolicies;
import com.linkedin.datahub.upgrade.system.schemafield.GenerateSchemaFieldsFromSchemaMetadata;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutatorChain;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabledCondition;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
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
      @Value("${systemUpdate.schemaFieldsFromSchemaMetadata.enabled}") final boolean enabled,
      @Value("${systemUpdate.schemaFieldsFromSchemaMetadata.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.schemaFieldsFromSchemaMetadata.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.schemaFieldsFromSchemaMetadata.limit}") final Integer limit) {
    return new GenerateSchemaFieldsFromSchemaMetadata(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }

  @Autowired private OperationContext opContext;

  @Bean
  @Conditional(KafkaMessagingEnabledCondition.class)
  public NonBlockingSystemUpgrade kafkaSetupNonBlocking(
      final ConfigurationProvider configurationProvider, KafkaProperties properties) {
    return new KafkaNonBlockingSetup(opContext, configurationProvider.getKafka(), properties);
  }

  // ConsistencyService walks the system-metadata catalog through SystemMetadataScrollClient,
  // which has both Elasticsearch and PostgreSQL implementations - so this upgrade runs on either
  // deployment profile.
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

  @Bean
  public NonBlockingSystemUpgrade ingestRetentionPolicies(
      @Qualifier("retentionService") final RetentionService<?> retentionService,
      @Qualifier("entityService") final EntityService<?> entityService,
      @Value("${entityService.retention.enabled}") final boolean enabled,
      @Value("${entityService.retention.applyOnBootstrap}") final boolean applyAfterIngest,
      @Value("${entityService.retention.applyOnPolicyChange:true}")
          final boolean applyOnPolicyChange,
      @Value("${entityService.retention.overwriteNonSystemPolicies:false}")
          final boolean overwriteNonSystemPolicies,
      @Value("${datahub.plugin.retention.path}") final String pluginPath) {
    return new IngestRetentionPolicies(
        retentionService,
        entityService,
        enabled,
        applyAfterIngest,
        applyOnPolicyChange,
        overwriteNonSystemPolicies,
        pluginPath);
  }

  @Bean
  public NonBlockingSystemUpgrade ingestEntityTypes(
      @Qualifier("systemOperationContext") final OperationContext opContext,
      final EntityService<?> entityService,
      @Value("${systemUpdate.ingestEntityTypes.enabled}") final boolean enabled) {
    return new IngestEntityTypes(opContext, entityService, enabled);
  }

  @Bean
  public NonBlockingSystemUpgrade restoreColumnLineageIndices(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Value("${systemUpdate.restoreColumnLineageIndices.enabled}") final boolean enabled) {
    return new RestoreColumnLineageIndices(entityService, enabled);
  }

  @Bean
  public NonBlockingSystemUpgrade restoreFormInfoIndices(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Value("${systemUpdate.restoreFormInfoIndices.enabled}") final boolean enabled) {
    return new RestoreFormInfoIndices(entityService, enabled);
  }

  @Bean
  public NonBlockingSystemUpgrade restoreGlossaryIndices(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Qualifier("entitySearchService") final EntitySearchService entitySearchService,
      @Value("${systemUpdate.restoreGlossaryIndices.enabled}") final boolean enabled) {
    return new RestoreGlossaryIndices(entityService, entitySearchService, enabled);
  }

  @Bean
  public NonBlockingSystemUpgrade ingestDataPlatformInstances(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Qualifier("entityAspectDao") final AspectMigrationsDao migrationsDao,
      @Value("${systemUpdate.ingestDataPlatformInstances.enabled}") final boolean enabled) {
    return new IngestDataPlatformInstances(entityService, migrationsDao, enabled);
  }

  @Bean
  public NonBlockingSystemUpgrade indexDataPlatforms(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Qualifier("entitySearchService") final EntitySearchService entitySearchService,
      @Value("${systemUpdate.indexDataPlatforms.enabled}") final boolean enabled) {
    return new IndexDataPlatforms(entityService, entitySearchService, enabled);
  }

  @Bean
  public NonBlockingSystemUpgrade migrateHomePageLinks(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Qualifier("entitySearchService") final EntitySearchService entitySearchService,
      final ConfigurationProvider configurationProvider,
      @Value("${systemUpdate.migrateHomePageLinks.enabled}") final boolean enabled) {
    return new MigrateHomePageLinks(
        entityService,
        entitySearchService,
        enabled,
        configurationProvider.getFeatureFlags().isShowHomePageRedesign());
  }

  @Bean
  public NonBlockingSystemUpgrade migrateAspects(
      @Qualifier("systemOperationContext") final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      final AspectMigrationMutatorChain aspectMigrationMutatorChain,
      final GitVersion gitVersion,
      @Qualifier("revision") final String revision,
      @Value("${systemUpdate.migrateAspects.enabled}") final boolean enabled,
      @Value("${systemUpdate.migrateAspects.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.migrateAspects.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.migrateAspects.limit}") final Integer limit) {
    String upgradeVersion = String.format("%s-%s", gitVersion.getVersion(), revision);
    return new MigrateAspects(
        opContext,
        entityService,
        aspectDao,
        aspectMigrationMutatorChain,
        upgradeVersion,
        enabled,
        batchSize,
        delayMs,
        limit);
  }

  @Bean
  public NonBlockingSystemUpgrade restoreDbtSiblingsIndices(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Value("${systemUpdate.restoreDbtSiblingsIndices.enabled}") final boolean enabled) {
    return new RestoreDbtSiblingsIndices(entityService, enabled);
  }
}
