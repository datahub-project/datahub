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
import com.linkedin.datahub.graphql.types.common.mappers.MetadataAttributionMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
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

  private static final String URN_PREFIX = "urn:";
  // Real urns nest about two levels (schemaField -> dataset -> dataPlatform). Bounding the depth
  // keeps a deeply nested value from overflowing the stack, which catch (Exception) cannot stop.
  private static final int MAX_NESTED_URN_DEPTH = 4;

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
    if (valueAssignment.getAttribution() != null) {
      entry.setAttribution(
          MetadataAttributionMapper.map(context, valueAssignment.getAttribution()));
    }
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
    final Urn urnValue = parseValueAsUrn(stringValue, 0);
    if (urnValue != null && isValidAgainstRegistry(context, urnValue)) {
      // UrnToEntityMapper returns null for entity types it does not know how to map. A string value
      // that merely parses as a URN (e.g. free text on a non-urn property, or a URN of an unmapped
      // entity type) must not contribute a null entity, otherwise downstream resolution of
      // valueEntities NPEs on the null element.
      final Entity mappedEntity = UrnToEntityMapper.map(context, urnValue);
      if (mappedEntity != null) {
        entities.add(mappedEntity);
      } else {
        log.warn(
            "Skipping value entity for structured property value '{}': entity type '{}' is not"
                + " mapped by UrnToEntityMapper",
            stringValue,
            urnValue.getEntityType());
      }
    }
    values.add(new StringValue(stringValue));
  }

  /**
   * Returns the urn a string value refers to, or null when the value is plain text. No parse
   * failure is propagated.
   *
   * <p>{@link Urn#createFromString(String)} on its own is not enough: text after the closing paren
   * of a tuple entity key is folded into the last key part, so
   * "urn:li:dataset:(urn:li:dataPlatform:hive,tbl,PROD) (hop 1): stale" parses into a dataset urn
   * whose fabric is "PROD) (hop 1): stale", and resolving that entity throws and takes down the
   * entity page. Text after a single part key cannot be told apart from the key itself and is kept
   * as part of it.
   */
  @Nullable
  private static Urn parseValueAsUrn(String value, int depth) {
    if (depth > MAX_NESTED_URN_DEPTH || !value.equals(value.strip())) {
      return null;
    }
    try {
      final Urn urnValue = Urn.createFromString(value);
      if (hasValidKeyParts(urnValue, depth)) {
        return urnValue;
      }
      log.debug("String value is not entirely an urn for this structured property entry");
    } catch (Exception e) {
      log.debug("String value is not an urn for this structured property entry");
    }
    return null;
  }

  private static boolean isValidAgainstRegistry(@Nullable QueryContext context, Urn urn) {
    if (context == null) {
      return true;
    }
    try {
      ValidationApiUtils.validateUrn(context.getOperationContext().getEntityRegistry(), urn);
      return true;
    } catch (RuntimeException e) {
      log.debug("Structured property value {} is not a valid urn: {}", urn, e.getMessage());
      return false;
    }
  }

  private static boolean hasValidKeyParts(Urn urn, int depth) {
    for (String part : urn.getEntityKey().getParts()) {
      if (part.startsWith(URN_PREFIX)) {
        if (parseValueAsUrn(part, depth + 1) == null) {
          return false;
        }
      } else if (part.contains("(") || part.contains(")")) {
        // Urn components cannot hold unencoded parentheses, so a part such as "PROD) (hop 1)" is
        // text the tuple parser absorbed rather than a real key component.
        return false;
      }
    }
    return true;
  }
}
