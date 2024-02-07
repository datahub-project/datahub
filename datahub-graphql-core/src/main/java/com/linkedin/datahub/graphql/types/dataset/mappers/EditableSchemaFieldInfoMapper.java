package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.types.businessattribute.mappers.BusinessAttributesMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.schema.EditableSchemaFieldInfo;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EditableSchemaFieldInfoMapper {
  private static final Logger _logger =
      LoggerFactory.getLogger(EditableSchemaFieldInfoMapper.class.getName());

  public static final EditableSchemaFieldInfoMapper INSTANCE = new EditableSchemaFieldInfoMapper();

  public static com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo map(
      @Nonnull final EditableSchemaFieldInfo fieldInfo, @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(fieldInfo, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo apply(
      @Nonnull final EditableSchemaFieldInfo input, @Nonnull final Urn entityUrn) {
    final com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo result =
        new com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo();
    if (input.hasDescription()) {
      result.setDescription((input.getDescription()));
    }
    if (input.hasFieldPath()) {
      result.setFieldPath((input.getFieldPath()));
    }
    if (input.hasGlobalTags()) {
      result.setGlobalTags(GlobalTagsMapper.map(input.getGlobalTags(), entityUrn));
      result.setTags(GlobalTagsMapper.map(input.getGlobalTags(), entityUrn));
    }
    if (input.hasGlossaryTerms()) {
      result.setGlossaryTerms(GlossaryTermsMapper.map(input.getGlossaryTerms(), entityUrn));
    }
    if (input.hasBusinessAttribute()) {
      result.setBusinessAttributes(
          BusinessAttributesMapper.map(input.getBusinessAttribute(), entityUrn));
    }
    return result;
  }
}
