package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.schema.EditableSchemaFieldInfo;

import javax.annotation.Nonnull;


public class EditableSchemaFieldInfoMapper implements ModelMapper<EditableSchemaFieldInfo, com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo> {

    public static final EditableSchemaFieldInfoMapper INSTANCE = new EditableSchemaFieldInfoMapper();

    public static com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo map(@Nonnull final EditableSchemaFieldInfo fieldInfo) {
        return INSTANCE.apply(fieldInfo);
    }

    @Override
    public com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo apply(@Nonnull final EditableSchemaFieldInfo input) {
        final com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo result = new com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo();
        if (input.hasDescription()) {
            result.setDescription((input.getDescription()));
        }
        if (input.hasFieldPath()) {
            result.setFieldPath((input.getFieldPath()));
        }
        if (input.hasGlobalTags()) {
            result.setGlobalTags(GlobalTagsMapper.map(input.getGlobalTags()));
            result.setTags(GlobalTagsMapper.map(input.getGlobalTags()));
        }
        if (input.hasGlossaryTerms()) {
            result.setGlossaryTerms(GlossaryTermsMapper.map(input.getGlossaryTerms()));
        }
        return result;
    }
}
