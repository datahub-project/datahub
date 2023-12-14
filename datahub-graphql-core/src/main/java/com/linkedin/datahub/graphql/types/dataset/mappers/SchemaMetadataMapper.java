package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.schema.SchemaMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class SchemaMetadataMapper {

  public static final SchemaMetadataMapper INSTANCE = new SchemaMetadataMapper();

  public static com.linkedin.datahub.graphql.generated.SchemaMetadata map(
      @Nonnull final EnvelopedAspect aspect, @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(aspect, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.SchemaMetadata apply(
      @Nonnull final EnvelopedAspect aspect, @Nonnull final Urn entityUrn) {
    final SchemaMetadata input = new SchemaMetadata(aspect.getValue().data());
    final com.linkedin.datahub.graphql.generated.SchemaMetadata result =
        new com.linkedin.datahub.graphql.generated.SchemaMetadata();

    if (input.hasDataset()) {
      result.setDatasetUrn(input.getDataset().toString());
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
    result.setAspectVersion(aspect.getVersion());
    if (input.hasForeignKeys()) {
      result.setForeignKeys(
          input.getForeignKeys().stream()
              .map(foreignKeyConstraint -> ForeignKeyConstraintMapper.map(foreignKeyConstraint))
              .collect(Collectors.toList()));
    }
    return result;
  }
}
