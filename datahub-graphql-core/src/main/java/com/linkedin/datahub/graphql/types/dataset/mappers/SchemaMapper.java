package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.generated.Schema;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.schema.SchemaMetadata;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class SchemaMapper implements ModelMapper<SchemaMetadata, Schema> {

    public static final SchemaMapper INSTANCE = new SchemaMapper();

    public static Schema map(@Nonnull final SchemaMetadata metadata) {
        return INSTANCE.apply(metadata);
    }

    @Override
    public Schema apply(@Nonnull final com.linkedin.schema.SchemaMetadata input) {
        final Schema result = new Schema();
        if (input.getDataset() != null) {
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
        if (input.getForeignKeys() != null) {
            result.setForeignKeys(input.getForeignKeys().stream()
                .map(ForeignKeyConstraintMapper::map)
                .collect(Collectors.toList()));
        }
        return result;
    }
}
