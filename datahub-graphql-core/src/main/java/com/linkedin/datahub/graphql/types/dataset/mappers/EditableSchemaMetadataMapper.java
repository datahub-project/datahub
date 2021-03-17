package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.schema.EditableSchemaMetadata;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class EditableSchemaMetadataMapper implements ModelMapper<EditableSchemaMetadata, com.linkedin.datahub.graphql.generated.EditableSchemaMetadata> {

    public static final EditableSchemaMetadataMapper INSTANCE = new EditableSchemaMetadataMapper();

    public static com.linkedin.datahub.graphql.generated.EditableSchemaMetadata map(@Nonnull final EditableSchemaMetadata metadata) {
        return INSTANCE.apply(metadata);
    }

    @Override
    public com.linkedin.datahub.graphql.generated.EditableSchemaMetadata apply(@Nonnull final EditableSchemaMetadata input) {
        final com.linkedin.datahub.graphql.generated.EditableSchemaMetadata result = new com.linkedin.datahub.graphql.generated.EditableSchemaMetadata();
        result.setEditableSchemaFieldInfo(input.getEditableSchemaFieldInfo().stream().map(EditableSchemaFieldInfoMapper::map).collect(Collectors.toList()));
        return result;
    }

}
