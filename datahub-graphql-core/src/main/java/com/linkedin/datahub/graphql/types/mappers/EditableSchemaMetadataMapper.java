package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.schema.EditableSchemaFieldInfo;
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
        result.setEditableSchemaFieldInfo(input.getEditableSchemaFieldInfo().stream().map(this::mapEditableSchemaFieldInfo).collect(Collectors.toList()));
        return result;
    }

    private com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo mapEditableSchemaFieldInfo(@Nonnull final EditableSchemaFieldInfo input) {
        final com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo result = new com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo();
        if (input.hasDescription()) {
            result.setDescription((input.getDescription()));
        }
        if (input.hasFieldPath()) {
            result.setFieldPath((input.getFieldPath()));
        }
        if (input.hasGlobalTags()) {
            result.setGlobalTags(GlobalTagsMapper.map(input.getGlobalTags()));
        }
        return result;
    }
}
