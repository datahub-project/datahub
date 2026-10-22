package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.models.StructuredPropertyUtils.getLogicalValueType;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toElasticsearchFieldName;
import static com.linkedin.metadata.search.utils.ESUtils.COPY_TO;
import static com.linkedin.metadata.search.utils.ESUtils.FIELDS;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Parsed name of a URN structured property. The property field stays the exact URN, with {@code
 * .lowercase}. The sibling {@code structuredProperties.<field>__urn_name} stores that name in its
 * original case, and {@code __urn_name.lowercase} folds it. {@code searchConfiguration} also adds
 * {@code text} and {@code stemmed}. {@code copyTo} copies the parsed name into shared fields.
 */
public final class StructuredPropertyUrnIdentity {

  /**
   * Appended to the property's Elasticsearch field name. The double underscore is not produced when
   * a qualified name's {@code .} characters become {@code _}.
   */
  public static final String FIELD_SUFFIX = "__urn_name";

  private StructuredPropertyUrnIdentity() {}

  public static boolean applies(@Nonnull final LogicalValueType logicalValueType) {
    return logicalValueType == LogicalValueType.URN;
  }

  @Nonnull
  public static String fieldName(@Nonnull final String elasticsearchFieldName) {
    return elasticsearchFieldName + FIELD_SUFFIX;
  }

  /**
   * Indexed keyword of the parsed name, in its original case. {@code .lowercase} folds it. Analyzed
   * subfields and {@code copy_to} are added when the property is in full text.
   */
  @Nonnull
  public static Map<String, Object> mapping(
      @Nonnull final StructuredPropertyDefinition definition,
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put(TYPE, "keyword");
    Map<String, Object> fields = new HashMap<>();
    fields.put(V3SearchFields.LOWERCASE, V3SearchFields.lowercaseKeywordField());
    if (definition.hasSearchConfiguration()) {
      fields.putAll(V3SearchFields.analyzedSubfields());
      List<String> copyTo =
          V3SearchFields.copyDestinations(
                  entitySpecs,
                  definition.getSearchConfiguration().hasCopyTo()
                      ? definition.getSearchConfiguration().getCopyTo()
                      : List.of())
              .stream()
              .map(V3SearchFields::path)
              .toList();
      if (!copyTo.isEmpty()) {
        mapping.put(COPY_TO, copyTo);
      }
    }
    mapping.put(FIELDS, fields);
    return mapping;
  }

  /**
   * Writes one identity array beside each opted-in URN property present on {@code document}. A null
   * value clears the identity so a removed assignment does not keep the previous text.
   */
  public static void project(
      @Nonnull final OperationContext opContext,
      @Nonnull final ObjectNode document,
      @Nullable final RecordTemplate aspect) {
    if (!(aspect instanceof StructuredProperties properties) || !properties.hasProperties()) {
      return;
    }
    Map<Urn, Set<StructuredPropertyValueAssignment>> assignments =
        properties.getProperties().stream()
            .collect(
                Collectors.groupingBy(
                    StructuredPropertyValueAssignment::getPropertyUrn, Collectors.toSet()));
    Map<Urn, StructuredPropertyDefinition> definitions =
        definitions(opContext, assignments.keySet());
    for (Map.Entry<Urn, Set<StructuredPropertyValueAssignment>> entry : assignments.entrySet()) {
      StructuredPropertyDefinition definition = definitions.get(entry.getKey());
      if (definition == null || !applies(getLogicalValueType(definition.getValueType()))) {
        continue;
      }
      String valueField =
          STRUCTURED_PROPERTY_MAPPING_FIELD
              + "."
              + toElasticsearchFieldName(entry.getKey(), definition);
      String identityField = valueField + FIELD_SUFFIX;
      JsonNode value = document.get(valueField);
      if (value == null || value.isNull()) {
        document.putNull(identityField);
        continue;
      }
      ArrayNode identities = identities(opContext.getEntityRegistry(), entry.getValue());
      if (identities.isEmpty()) {
        document.putNull(identityField);
      } else {
        document.set(identityField, identities);
      }
    }
  }

  @Nonnull
  private static ArrayNode identities(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final Set<StructuredPropertyValueAssignment> assignments) {
    ArrayNode identities = JsonNodeFactory.instance.arrayNode();
    Set<String> seen = new LinkedHashSet<>();
    for (StructuredPropertyValueAssignment assignment : assignments) {
      if (!assignment.hasValues()) {
        continue;
      }
      for (PrimitivePropertyValue propertyValue : assignment.getValues()) {
        String raw = propertyValue.getString();
        if (raw == null || !raw.startsWith("urn:")) {
          continue;
        }
        try {
          String identity = UrnSearchIdentity.identity(Urn.createFromString(raw), entityRegistry);
          if (!identity.isEmpty()) {
            seen.add(identity);
          }
        } catch (Exception ignored) {
          // A value that only looks like a URN stays exact and contributes no identity.
        }
      }
    }
    seen.forEach(identities::add);
    return identities;
  }

  @Nonnull
  private static Map<Urn, StructuredPropertyDefinition> definitions(
      @Nonnull final OperationContext opContext, @Nonnull final Set<Urn> propertyUrns) {
    try {
      Map<Urn, Map<String, Aspect>> aspects =
          opContext
              .getAspectRetriever()
              .getLatestAspectObjects(
                  opContext, propertyUrns, Set.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME));
      Map<Urn, StructuredPropertyDefinition> definitions = new HashMap<>();
      for (Map.Entry<Urn, Map<String, Aspect>> entry : aspects.entrySet()) {
        Aspect aspect = entry.getValue().get(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
        if (aspect != null && aspect.data() != null) {
          definitions.put(entry.getKey(), new StructuredPropertyDefinition(aspect.data()));
        }
      }
      return definitions;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load structured property definitions for V3 URN identity", e);
    }
  }
}
