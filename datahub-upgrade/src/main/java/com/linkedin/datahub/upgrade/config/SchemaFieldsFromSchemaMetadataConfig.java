package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.schemafield.GenerateSchemaFieldsFromSchemaMetadata;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class SchemaFieldsFromSchemaMetadataConfig {

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
}
