package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SchemaField;
import com.linkedin.datahub.graphql.generated.SchemaFieldDataType;
import com.linkedin.datahub.graphql.generated.SchemaFieldEntity;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SchemaFieldMapper {

  public static final SchemaFieldMapper INSTANCE = new SchemaFieldMapper();

  public static SchemaField map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.schema.SchemaField metadata,
      @Nonnull Urn entityUrn) {
    return INSTANCE.apply(context, metadata, entityUrn);
  }

  public SchemaField apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.schema.SchemaField input,
      @Nonnull Urn entityUrn) {
    final SchemaField result = new SchemaField();
    result.setDescription(input.getDescription());
    result.setFieldPath(input.getFieldPath());
    result.setJsonPath(input.getJsonPath());
    result.setRecursive(input.isRecursive());
    result.setNullable(input.isNullable());
    result.setNativeDataType(input.getNativeDataType());
    result.setType(mapSchemaFieldDataType(input.getType()));
    result.setLabel(input.getLabel());
    if (input.hasGlobalTags()) {
      result.setGlobalTags(GlobalTagsMapper.map(context, input.getGlobalTags(), entityUrn));
      result.setTags(GlobalTagsMapper.map(context, input.getGlobalTags(), entityUrn));
    }
    if (input.hasGlossaryTerms()) {
      result.setGlossaryTerms(
          GlossaryTermsMapper.map(context, input.getGlossaryTerms(), entityUrn));
    }
    result.setIsPartOfKey(input.isIsPartOfKey());
    result.setIsPartitioningKey(input.isIsPartitioningKey());
    result.setJsonProps(input.getJsonProps());
    result.setSchemaFieldEntity(this.createSchemaFieldEntity(input, entityUrn));
    return result;
  }

  public SchemaFieldDataType mapSchemaFieldDataType(
      @Nonnull final com.linkedin.schema.SchemaFieldDataType dataTypeUnion) {
    final com.linkedin.schema.SchemaFieldDataType.Type type = dataTypeUnion.getType();
    if (type.isBytesType()) {
      return SchemaFieldDataType.BYTES;
    } else if (type.isFixedType()) {
      return SchemaFieldDataType.FIXED;
    } else if (type.isBooleanType()) {
      return SchemaFieldDataType.BOOLEAN;
    } else if (type.isStringType()) {
      return SchemaFieldDataType.STRING;
    } else if (type.isNumberType()) {
      return SchemaFieldDataType.NUMBER;
    } else if (type.isDateType()) {
      return SchemaFieldDataType.DATE;
    } else if (type.isTimeType()) {
      return SchemaFieldDataType.TIME;
    } else if (type.isEnumType()) {
      return SchemaFieldDataType.ENUM;
    } else if (type.isNullType()) {
      return SchemaFieldDataType.NULL;
    } else if (type.isArrayType()) {
      return SchemaFieldDataType.ARRAY;
    } else if (type.isMapType()) {
      return SchemaFieldDataType.MAP;
    } else if (type.isRecordType()) {
      return SchemaFieldDataType.STRUCT;
    } else if (type.isUnionType()) {
      return SchemaFieldDataType.UNION;
    } else {
      throw new RuntimeException(
          String.format(
              "Unrecognized SchemaFieldDataType provided %s", type.memberType().toString()));
    }
  }

  private SchemaFieldEntity createSchemaFieldEntity(
      @Nonnull final com.linkedin.schema.SchemaField input, @Nonnull Urn entityUrn) {
    SchemaFieldEntity schemaFieldEntity = new SchemaFieldEntity();
    schemaFieldEntity.setUrn(
        SchemaFieldUtils.generateSchemaFieldUrn(entityUrn, input.getFieldPath()).toString());
    schemaFieldEntity.setType(EntityType.SCHEMA_FIELD);
    return schemaFieldEntity;
  }
}
