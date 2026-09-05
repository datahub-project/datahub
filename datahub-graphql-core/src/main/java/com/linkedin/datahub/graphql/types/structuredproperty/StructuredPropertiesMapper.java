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
    final Urn urnValue = parseValueAsUrn(stringValue.trim());
    if (urnValue != null) {
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
   * Returns the urn a string value refers to, or null when the value is plain text. Only a value
   * that is entirely a well formed urn is an entity reference, and no parse failure is propagated.
   *
   * <p>{@link Urn#createFromString(String)} on its own is not enough: it stops caring about the
   * input once the parentheses of a tuple entity key balance, so free text that merely contains
   * something urn shaped, such as "urn:li:dataset:(urn:li:dataPlatform:hive,tbl,PROD) (hop 1):
   * stale", parses into a dataset urn whose last key part is "PROD) (hop 1): stale". Mapping that
   * as a value entity succeeds here, but resolving the entity afterwards throws
   * IllegalArgumentException ("No enum constant com.linkedin.common.FabricType...") and takes down
   * the entity page and every search that returns it.
   */
  @Nullable
  private static Urn parseValueAsUrn(String value) {
    try {
      final Urn urnValue = Urn.createFromString(value);
      // Re-encoding the parsed urn from its parts drops anything the parser tolerated after the
      // entity key, so it reproduces the value only when the value was a complete urn.
      final Urn reEncoded =
          new Urn(urnValue.getNamespace(), urnValue.getEntityType(), urnValue.getEntityKey());
      if (value.equals(reEncoded.toString()) && hasValidKeyParts(urnValue)) {
        return urnValue;
      }
      log.debug("String value is not entirely an urn for this structured property entry");
    } catch (Exception e) {
      log.debug("String value is not an urn for this structured property entry");
    }
    return null;
  }

  private static boolean hasValidKeyParts(Urn urn) {
    for (String part : urn.getEntityKey().getParts()) {
      if (part.startsWith(URN_PREFIX)) {
        // A nested urn has to be a complete urn itself.
        if (parseValueAsUrn(part) == null) {
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
