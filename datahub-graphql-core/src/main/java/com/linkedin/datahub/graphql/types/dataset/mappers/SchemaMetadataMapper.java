package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.schema.SchemaMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class SchemaMetadataMapper implements ModelMapper<EnvelopedAspect, com.linkedin.datahub.graphql.generated.SchemaMetadata> {

    public static final SchemaMetadataMapper INSTANCE = new SchemaMetadataMapper();

    public static com.linkedin.datahub.graphql.generated.SchemaMetadata map(@Nonnull final EnvelopedAspect aspect) {
        return INSTANCE.apply(aspect);
    }

    @Override
    public com.linkedin.datahub.graphql.generated.SchemaMetadata apply(@Nonnull final EnvelopedAspect aspect) {
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
        result.setFields(input.getFields().stream().map(SchemaFieldMapper::map).collect(Collectors.toList()));
        result.setPlatformSchema(PlatformSchemaMapper.map(input.getPlatformSchema()));
        result.setAspectVersion(aspect.getVersion());
        if (input.hasForeignKeys()) {
            result.setForeignKeys(input.getForeignKeys().stream().map(foreignKeyConstraint -> ForeignKeyConstraintMapper.map(
                foreignKeyConstraint
            )).collect(Collectors.toList()));
        }
        return result;
    }
}
