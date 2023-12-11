package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.generated.KeyValueSchema;
import com.linkedin.datahub.graphql.generated.PlatformSchema;
import com.linkedin.datahub.graphql.generated.TableSchema;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.schema.SchemaMetadata;
import javax.annotation.Nonnull;

public class PlatformSchemaMapper
    implements ModelMapper<SchemaMetadata.PlatformSchema, PlatformSchema> {

  public static final PlatformSchemaMapper INSTANCE = new PlatformSchemaMapper();

  public static PlatformSchema map(@Nonnull final SchemaMetadata.PlatformSchema metadata) {
    return INSTANCE.apply(metadata);
  }

  @Override
  public PlatformSchema apply(@Nonnull final SchemaMetadata.PlatformSchema input) {
    Object result;
    if (input.isSchemaless()) {
      return null;
    } else if (input.isPrestoDDL()) {
      final TableSchema prestoSchema = new TableSchema();
      prestoSchema.setSchema(input.getPrestoDDL().getRawSchema());
      result = prestoSchema;
    } else if (input.isOracleDDL()) {
      final TableSchema oracleSchema = new TableSchema();
      oracleSchema.setSchema(input.getOracleDDL().getTableSchema());
      result = oracleSchema;
    } else if (input.isMySqlDDL()) {
      final TableSchema mySqlSchema = new TableSchema();
      mySqlSchema.setSchema(input.getMySqlDDL().getTableSchema());
      result = mySqlSchema;
    } else if (input.isKafkaSchema()) {
      final TableSchema kafkaSchema = new TableSchema();
      kafkaSchema.setSchema(input.getKafkaSchema().getDocumentSchema());
      result = kafkaSchema;
    } else if (input.isOrcSchema()) {
      final TableSchema orcSchema = new TableSchema();
      orcSchema.setSchema(input.getOrcSchema().getSchema());
      result = orcSchema;
    } else if (input.isBinaryJsonSchema()) {
      final TableSchema binaryJsonSchema = new TableSchema();
      binaryJsonSchema.setSchema(input.getBinaryJsonSchema().getSchema());
      result = binaryJsonSchema;
    } else if (input.isEspressoSchema()) {
      final KeyValueSchema espressoSchema = new KeyValueSchema();
      espressoSchema.setKeySchema(input.getEspressoSchema().getTableSchema());
      espressoSchema.setValueSchema(input.getEspressoSchema().getDocumentSchema());
      result = espressoSchema;
    } else if (input.isKeyValueSchema()) {
      final KeyValueSchema otherKeyValueSchema = new KeyValueSchema();
      otherKeyValueSchema.setKeySchema(input.getKeyValueSchema().getKeySchema());
      otherKeyValueSchema.setValueSchema(input.getKeyValueSchema().getValueSchema());
      result = otherKeyValueSchema;
    } else if (input.isOtherSchema()) {
      final TableSchema otherTableSchema = new TableSchema();
      otherTableSchema.setSchema(input.getOtherSchema().getRawSchema());
      result = otherTableSchema;
    } else {
      throw new RuntimeException(
          String.format(
              "Unrecognized platform schema type %s provided",
              input.memberType().getType().name()));
    }
    return (PlatformSchema) result;
  }
}
