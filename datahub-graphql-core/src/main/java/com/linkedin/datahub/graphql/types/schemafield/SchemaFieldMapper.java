package com.linkedin.datahub.graphql.types.schemafield;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.businessattribute.BusinessAttributes;
import com.linkedin.common.Deprecation;
import com.linkedin.common.Documentation;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SchemaFieldEntity;
import com.linkedin.datahub.graphql.types.businessattribute.mappers.BusinessAttributesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DocumentationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SchemaFieldMapper implements ModelMapper<EntityResponse, SchemaFieldEntity> {

  public static final SchemaFieldMapper INSTANCE = new SchemaFieldMapper();

  public static SchemaFieldEntity map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public SchemaFieldEntity apply(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    Urn entityUrn = entityResponse.getUrn();
    final SchemaFieldEntity result = this.mapSchemaFieldUrn(context, entityUrn);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<SchemaFieldEntity> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        ((schemaField, dataMap) ->
            schemaField.setStructuredProperties(
                StructuredPropertiesMapper.map(
                    context, new StructuredProperties(dataMap), entityUrn))));
    mappingHelper.mapToResult(
        BUSINESS_ATTRIBUTE_ASPECT,
        (((schemaField, dataMap) ->
            schemaField.setBusinessAttributes(
                BusinessAttributesMapper.map(new BusinessAttributes(dataMap), entityUrn)))));
    mappingHelper.mapToResult(
        DOCUMENTATION_ASPECT_NAME,
        (entity, dataMap) ->
            entity.setDocumentation(DocumentationMapper.map(context, new Documentation(dataMap))));
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (entity, dataMap) -> entity.setStatus(StatusMapper.map(context, new Status(dataMap))));
    mappingHelper.mapToResult(
        DEPRECATION_ASPECT_NAME,
        ((schemaField, dataMap) ->
            schemaField.setDeprecation(
                DeprecationMapper.map(context, new Deprecation((dataMap))))));

    return result;
  }

  private SchemaFieldEntity mapSchemaFieldUrn(@Nullable QueryContext context, Urn urn) {
    try {
      SchemaFieldEntity result = new SchemaFieldEntity();
      result.setUrn(urn.toString());
      result.setType(EntityType.SCHEMA_FIELD);
      result.setFieldPath(urn.getEntityKey().get(1));
      Urn parentUrn = Urn.createFromString(urn.getEntityKey().get(0));
      result.setParent(UrnToEntityMapper.map(context, parentUrn));
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load schemaField entity", e);
    }
  }
}
