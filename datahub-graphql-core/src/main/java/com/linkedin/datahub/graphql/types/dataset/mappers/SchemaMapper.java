package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Schema;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.SchemaMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SchemaMapper {

  public static final SchemaMapper INSTANCE = new SchemaMapper();

  public static Schema map(
      @Nullable QueryContext context,
      @Nonnull final SchemaMetadata metadata,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, metadata, null, entityUrn);
  }

  public static Schema map(
      @Nullable QueryContext context,
      @Nonnull final SchemaMetadata metadata,
      @Nullable final SystemMetadata systemMetadata,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, metadata, systemMetadata, entityUrn);
  }

  public Schema apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.schema.SchemaMetadata input,
      @Nullable final SystemMetadata systemMetadata,
      @Nonnull final Urn entityUrn) {
    final Schema result = new Schema();
    if (input.getDataset() != null) {
      result.setDatasetUrn(input.getDataset().toString());
    }
    if (systemMetadata != null) {
      result.setLastObserved(systemMetadata.getLastObserved());
    }
    result.setName(input.getSchemaName());
    result.setPlatformUrn(input.getPlatform().toString());
    result.setVersion(input.getVersion());
    result.setCluster(input.getCluster());
    result.setHash(input.getHash());
    result.setPrimaryKeys(input.getPrimaryKeys());
    result.setFields(
        input.getFields().stream()
            // do not map the field if the field.getFieldPath() is null or empty
            .filter(field -> field.getFieldPath() != null && !field.getFieldPath().isEmpty())
            .map(field -> SchemaFieldMapper.map(context, field, entityUrn))
            .collect(Collectors.toList()));
    result.setPlatformSchema(PlatformSchemaMapper.map(context, input.getPlatformSchema()));
    if (input.getForeignKeys() != null) {
      result.setForeignKeys(
          input.getForeignKeys().stream()
              .map(fk -> ForeignKeyConstraintMapper.map(context, fk))
              .collect(Collectors.toList()));
    }
    return result;
  }
}
