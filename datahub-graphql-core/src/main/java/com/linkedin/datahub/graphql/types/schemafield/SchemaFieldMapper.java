package com.linkedin.datahub.graphql.types.schemafield;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SchemaFieldEntity;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nonnull;

public class SchemaFieldMapper implements ModelMapper<EntityResponse, SchemaFieldEntity> {

  public static final SchemaFieldMapper INSTANCE = new SchemaFieldMapper();

  public static SchemaFieldEntity map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public SchemaFieldEntity apply(@Nonnull final EntityResponse entityResponse) {
    Urn entityUrn = entityResponse.getUrn();
    final SchemaFieldEntity result = this.mapSchemaFieldUrn(entityUrn);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<SchemaFieldEntity> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        ((schemaField, dataMap) ->
            schemaField.setStructuredProperties(
                StructuredPropertiesMapper.map(new StructuredProperties(dataMap)))));

    return result;
  }

  private SchemaFieldEntity mapSchemaFieldUrn(Urn urn) {
    try {
      SchemaFieldEntity result = new SchemaFieldEntity();
      result.setUrn(urn.toString());
      result.setType(EntityType.SCHEMA_FIELD);
      result.setFieldPath(urn.getEntityKey().get(1));
      Urn parentUrn = Urn.createFromString(urn.getEntityKey().get(0));
      result.setParent(UrnToEntityMapper.map(parentUrn));
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load schemaField entity", e);
    }
  }
}
