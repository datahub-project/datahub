package com.linkedin.datahub.graphql.types.structuredproperty;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.NumberValue;
import com.linkedin.datahub.graphql.generated.PropertyValue;
import com.linkedin.datahub.graphql.generated.StringValue;
import com.linkedin.datahub.graphql.generated.StructuredPropertiesEntry;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StructuredPropertiesMapper {

  public static final StructuredPropertiesMapper INSTANCE = new StructuredPropertiesMapper();

  public static com.linkedin.datahub.graphql.generated.StructuredProperties map(
      @Nullable QueryContext context,
      @Nonnull final StructuredProperties structuredProperties,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, structuredProperties, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.StructuredProperties apply(
      @Nullable QueryContext context,
      @Nonnull final StructuredProperties structuredProperties,
      @Nonnull final Urn entityUrn) {
    com.linkedin.datahub.graphql.generated.StructuredProperties result =
        new com.linkedin.datahub.graphql.generated.StructuredProperties();
    result.setProperties(
        structuredProperties.getProperties().stream()
            .map(p -> mapStructuredProperty(context, p, entityUrn))
            .collect(Collectors.toList()));
    return result;
  }

  private StructuredPropertiesEntry mapStructuredProperty(
      @Nullable QueryContext context,
      StructuredPropertyValueAssignment valueAssignment,
      @Nonnull final Urn entityUrn) {
    StructuredPropertiesEntry entry = new StructuredPropertiesEntry();
    entry.setStructuredProperty(createStructuredPropertyEntity(valueAssignment));
    final List<PropertyValue> values = new ArrayList<>();
    final List<Entity> entities = new ArrayList<>();
    valueAssignment
        .getValues()
        .forEach(
            value -> {
              if (value.isString()) {
                this.mapStringValue(context, value.getString(), values, entities);
              } else if (value.isDouble()) {
                values.add(new NumberValue(value.getDouble()));
              }
            });
    entry.setValues(values);
    entry.setValueEntities(entities);
    entry.setAssociatedUrn(entityUrn.toString());
    return entry;
  }

  private StructuredPropertyEntity createStructuredPropertyEntity(
      StructuredPropertyValueAssignment assignment) {
    StructuredPropertyEntity entity = new StructuredPropertyEntity();
    entity.setUrn(assignment.getPropertyUrn().toString());
    entity.setType(EntityType.STRUCTURED_PROPERTY);
    return entity;
  }

  private static void mapStringValue(
      @Nullable QueryContext context,
      String stringValue,
      List<PropertyValue> values,
      List<Entity> entities) {
    try {
      final Urn urnValue = Urn.createFromString(stringValue);
      entities.add(UrnToEntityMapper.map(context, urnValue));
    } catch (Exception e) {
      log.debug("String value is not an urn for this structured property entry");
    }
    values.add(new StringValue(stringValue));
  }
}
