package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.schema.SchemaMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class SchemaMetadataMapper {

  public static final SchemaMetadataMapper INSTANCE = new SchemaMetadataMapper();

  public static com.linkedin.datahub.graphql.generated.SchemaMetadata map(
<<<<<<< HEAD
      @Nonnull final EnvelopedAspect aspect, final Urn entityUrn) {
=======
      @Nonnull final EnvelopedAspect aspect, @Nonnull final Urn entityUrn) {
>>>>>>> oss_master
    return INSTANCE.apply(aspect, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.SchemaMetadata apply(
<<<<<<< HEAD
      @Nonnull final EnvelopedAspect aspect, final Urn entityUrn) {
    final SchemaMetadata input = new SchemaMetadata(aspect.getValue().data());
    return apply(input, entityUrn, aspect.getVersion());
  }

  public com.linkedin.datahub.graphql.generated.SchemaMetadata apply(
      @Nonnull final SchemaMetadata input, final Urn entityUrn, final long version) {
=======
      @Nonnull final EnvelopedAspect aspect, @Nonnull final Urn entityUrn) {
    final SchemaMetadata input = new SchemaMetadata(aspect.getValue().data());
>>>>>>> oss_master
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
<<<<<<< HEAD
    result.setAspectVersion(version);
=======
    result.setAspectVersion(aspect.getVersion());
>>>>>>> oss_master
    if (input.hasForeignKeys()) {
      result.setForeignKeys(
          input.getForeignKeys().stream()
              .map(foreignKeyConstraint -> ForeignKeyConstraintMapper.map(foreignKeyConstraint))
              .collect(Collectors.toList()));
    }
    return result;
  }
}
