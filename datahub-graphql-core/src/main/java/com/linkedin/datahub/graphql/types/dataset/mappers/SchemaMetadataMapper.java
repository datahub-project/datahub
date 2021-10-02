package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.schema.SchemaMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class SchemaMetadataMapper implements ModelMapper<VersionedAspect, com.linkedin.datahub.graphql.generated.SchemaMetadata> {

    public static final SchemaMetadataMapper INSTANCE = new SchemaMetadataMapper();

    public static com.linkedin.datahub.graphql.generated.SchemaMetadata map(@Nonnull final VersionedAspect metadata) {
        return INSTANCE.apply(metadata);
    }

    @Override
    public com.linkedin.datahub.graphql.generated.SchemaMetadata apply(@Nonnull final VersionedAspect inputWithMetadata) {
        SchemaMetadata input = inputWithMetadata.getAspect().getSchemaMetadata();
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
        result.setAspectVersion(inputWithMetadata.getVersion());
        if (input.hasForeignKeys()) {
            result.setForeignKeys(input.getForeignKeys().stream().map(foreignKeyConstraint -> ForeignKeyConstraintMapper.map(
                foreignKeyConstraint
            )).collect(Collectors.toList()));
        }
        return result;
    }
}
