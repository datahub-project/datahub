package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.schema.SchemaMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SchemaMetadataMapper {

  public static final SchemaMetadataMapper INSTANCE = new SchemaMetadataMapper();

  public static com.linkedin.datahub.graphql.generated.SchemaMetadata map(
      @Nullable QueryContext context,
      @Nonnull final EnvelopedAspect aspect,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, aspect, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.SchemaMetadata apply(
      @Nullable QueryContext context,
      @Nonnull final EnvelopedAspect aspect,
      @Nonnull final Urn entityUrn) {
    final SchemaMetadata input = new SchemaMetadata(aspect.getValue().data());
    return apply(context, input, entityUrn, aspect.getVersion());
  }

  public com.linkedin.datahub.graphql.generated.SchemaMetadata apply(
      @Nullable QueryContext context,
      @Nonnull final SchemaMetadata input,
      final Urn entityUrn,
      final long version) {
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
            // do not map the field if the field.getFieldPath() is null or empty
            .filter(field -> field.getFieldPath() != null && !field.getFieldPath().isEmpty())
            .map(field -> SchemaFieldMapper.map(context, field, entityUrn))
            .collect(Collectors.toList()));
    result.setPlatformSchema(PlatformSchemaMapper.map(context, input.getPlatformSchema()));
    result.setAspectVersion(version);
    if (input.hasForeignKeys()) {
      result.setForeignKeys(
          input.getForeignKeys().stream()
              .map(
                  foreignKeyConstraint ->
                      ForeignKeyConstraintMapper.map(context, foreignKeyConstraint))
              .collect(Collectors.toList()));
    }
    return result;
  }
}
