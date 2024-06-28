package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.schema.EditableSchemaFieldInfo;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EditableSchemaFieldInfoMapper {

  public static final EditableSchemaFieldInfoMapper INSTANCE = new EditableSchemaFieldInfoMapper();

  public static com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo map(
      @Nullable final QueryContext context,
      @Nonnull final EditableSchemaFieldInfo fieldInfo,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, fieldInfo, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo apply(
      @Nullable final QueryContext context,
      @Nonnull final EditableSchemaFieldInfo input,
      @Nonnull final Urn entityUrn) {
    final com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo result =
        new com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo();
    if (input.hasDescription()) {
      result.setDescription((input.getDescription()));
    }
    if (input.hasFieldPath()) {
      result.setFieldPath((input.getFieldPath()));
    }
    if (input.hasGlobalTags()) {
      result.setGlobalTags(GlobalTagsMapper.map(context, input.getGlobalTags(), entityUrn));
      result.setTags(GlobalTagsMapper.map(context, input.getGlobalTags(), entityUrn));
    }
    if (input.hasGlossaryTerms()) {
      result.setGlossaryTerms(
          GlossaryTermsMapper.map(context, input.getGlossaryTerms(), entityUrn));
    }
    return result;
  }
}
