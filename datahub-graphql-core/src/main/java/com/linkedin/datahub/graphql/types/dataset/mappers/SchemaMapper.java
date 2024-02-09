package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Schema;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.SchemaMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SchemaMapper {

  public static final SchemaMapper INSTANCE = new SchemaMapper();

  public static Schema map(@Nonnull final SchemaMetadata metadata, @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(metadata, null, entityUrn);
  }

  public static Schema map(
      @Nonnull final SchemaMetadata metadata,
      @Nullable final SystemMetadata systemMetadata,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(metadata, systemMetadata, entityUrn);
  }

  public Schema apply(
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
            .map(field -> SchemaFieldMapper.map(field, entityUrn))
            .collect(Collectors.toList()));
    result.setPlatformSchema(PlatformSchemaMapper.map(input.getPlatformSchema()));
    if (input.getForeignKeys() != null) {
      result.setForeignKeys(
          input.getForeignKeys().stream()
              .map(ForeignKeyConstraintMapper::map)
              .collect(Collectors.toList()));
    }
    return result;
  }
}
