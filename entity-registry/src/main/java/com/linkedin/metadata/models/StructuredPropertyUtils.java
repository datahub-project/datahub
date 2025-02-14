package com.linkedin.metadata.models;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.GetMode;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertySettings;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.util.Pair;
import java.sql.Date;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StructuredPropertyUtils {

  private StructuredPropertyUtils() {}

  static final Date MIN_DATE = Date.valueOf("1000-01-01");
  static final Date MAX_DATE = Date.valueOf("9999-12-31");

  public static final String INVALID_SETTINGS_MESSAGE =
      "Cannot have property isHidden = true while other display location settings are also true.";
  public static final String ONLY_ONE_BADGE =
      "Cannot have more than one property set with show as badge. Property urns currently set: ";

  public static LogicalValueType getLogicalValueType(
      StructuredPropertyDefinition structuredPropertyDefinition) {
    return getLogicalValueType(structuredPropertyDefinition.getValueType());
  }

  public static LogicalValueType getLogicalValueType(@Nullable Urn valueType) {
    String valueTypeId = getValueTypeId(valueType);
    if ("string".equals(valueTypeId)) {
      return LogicalValueType.STRING;
    } else if ("date".equals(valueTypeId)) {
      return LogicalValueType.DATE;
    } else if ("number".equals(valueTypeId)) {
      return LogicalValueType.NUMBER;
    } else if ("urn".equals(valueTypeId)) {
      return LogicalValueType.URN;
    } else if ("rich_text".equals(valueTypeId)) {
      return LogicalValueType.RICH_TEXT;
    }
    return LogicalValueType.UNKNOWN;
  }

  @Nullable
  public static String getValueTypeId(@Nullable final Urn valueType) {
    if (valueType != null) {
      String valueTypeId = valueType.getId();
      if (valueTypeId.startsWith("datahub.")) {
        valueTypeId = valueTypeId.split("\\.")[1];
      }
      return valueTypeId.toLowerCase();
    } else {
      return null;
    }
  }

  /**
   * Given a structured property input field or facet name, return a valid structured property facet
   * name
   *
   * @param fieldOrFacetName input name
   * @param aspectRetriever aspect retriever
   * @return guranteed facet name
   */
  public static Optional<String> toStructuredPropertyFacetName(
      @Nonnull String fieldOrFacetName, @Nullable AspectRetriever aspectRetriever) {
    return lookupDefinitionFromFilterOrFacetName(fieldOrFacetName, aspectRetriever)
        .map(
            urnDefinition -> {
              switch (getLogicalValueType(urnDefinition.getSecond())) {
                case DATE:
                case NUMBER:
                  return STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX
                      + StructuredPropertyUtils.toElasticsearchFieldName(
                          urnDefinition.getFirst(), urnDefinition.getSecond());
                default:
                  return STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX
                      + StructuredPropertyUtils.toElasticsearchFieldName(
                          urnDefinition.getFirst(), urnDefinition.getSecond())
                      + ".keyword";
              }
            });
  }

  /**
   * Lookup structured property definition given the name used for the field in APIs such as a
   * search filter or aggregation query facet name.
   *
   * @param fieldOrFacetName the field name used in a filter or facet name in an aggregation query
   * @param aspectRetriever method to look up the definition aspect
   * @return the structured property definition if found
   */
  public static Optional<Pair<Urn, StructuredPropertyDefinition>>
      lookupDefinitionFromFilterOrFacetName(
          @Nonnull String fieldOrFacetName, @Nullable AspectRetriever aspectRetriever) {
    if (fieldOrFacetName.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD + ".")) {
      // Coming in from the UI this is structuredProperties.<FQN> + any particular specifier for
      // subfield (.keyword etc)
      String fqn = fieldOrFacetToFQN(fieldOrFacetName);

      // FQN Maps directly to URN with urn:li:structuredProperties:FQN
      Urn urn = toURNFromFQN(fqn);

      Map<Urn, Map<String, Aspect>> result =
          Objects.requireNonNull(aspectRetriever)
              .getLatestAspectObjects(
                  Collections.singleton(urn),
                  Collections.singleton(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME));
      Optional<Aspect> definition =
          Optional.ofNullable(
              result
                  .getOrDefault(urn, Collections.emptyMap())
                  .getOrDefault(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME, null));
      return definition.map(
          definitonAspect ->
              Pair.of(urn, new StructuredPropertyDefinition(definitonAspect.data())));
    }
    return Optional.empty();
  }

  /**
   * Given the structured property definition extract the Elasticsearch field name with nesting and
   * character replacement.
   *
   * <p>Sanitizes fully qualified name for use in an ElasticSearch field name Replaces `.`
   * characters
   *
   * @param definition The structured property definition
   * @return The sanitized version that can be used as a field name
   */
  public static String toElasticsearchFieldName(
      @Nonnull Urn propertyUrn, @Nullable StructuredPropertyDefinition definition) {
    String qualifiedName = definition != null ? definition.getQualifiedName() : propertyUrn.getId();

    if (qualifiedName.contains(" ")) {
      throw new IllegalArgumentException(
          "Fully qualified structured property name cannot contain spaces");
    }
    if (definition != null && definition.getVersion(GetMode.NULL) != null) {
      // includes type suffix
      return String.join(
          ".",
          STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD,
          definition.getQualifiedName().replace('.', '_'),
          definition.getVersion(),
          getLogicalValueType(definition).name().toLowerCase());
    } else {
      // un-typed property
      return qualifiedName.replace('.', '_');
    }
  }

  /**
   * Return an elasticsearch type from structured property type
   *
   * @param fieldName filter or facet field name - must match actual FQN of structured prop
   * @param aspectRetriever aspect retriever
   * @return elasticsearch type
   */
  public static Set<String> toElasticsearchFieldType(
      @Nonnull String fieldName, @Nullable AspectRetriever aspectRetriever) {
    LogicalValueType logicalValueType =
        lookupDefinitionFromFilterOrFacetName(fieldName, aspectRetriever)
            .map(definition -> getLogicalValueType(definition.getValue()))
            .orElse(LogicalValueType.STRING);

    switch (logicalValueType) {
      case NUMBER:
        return Collections.singleton("double");
      case DATE:
        return Collections.singleton("long");
      case RICH_TEXT:
        return Collections.singleton("text");
      case UNKNOWN:
      case STRING:
      case URN:
      default:
        return Collections.singleton("keyword");
    }
  }

  public static void validateStructuredPropertyFQN(
      @Nonnull Collection<String> fullyQualifiedNames, @Nonnull AspectRetriever aspectRetriever) {
    Set<Urn> structuredPropertyUrns =
        fullyQualifiedNames.stream()
            .map(StructuredPropertyUtils::toURNFromFQN)
            .collect(Collectors.toSet());
    Set<Urn> removedUrns = getRemovedUrns(structuredPropertyUrns, aspectRetriever);
    if (!removedUrns.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Cannot filter on deleted Structured Property %s", removedUrns));
    }
  }

  /**
   * Given a Structured Property fqn, calculate the expected URN
   *
   * @param fqn structured property's fqn
   * @return the expected structured property urn
   */
  public static Urn toURNFromFQN(@Nonnull String fqn) {
    return UrnUtils.getUrn(String.join(":", "urn:li", STRUCTURED_PROPERTY_ENTITY_NAME, fqn));
  }

  public static void validateFilter(
      @Nullable Filter filter, @Nullable AspectRetriever aspectRetriever) {

    if (filter == null) {
      return;
    }

    Set<String> fqns = new HashSet<>();

    if (filter.getCriteria() != null) {
      for (Criterion c : filter.getCriteria()) {
        if (c.getField().startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)) {
          String fqn = fieldOrFacetToFQN(c.getField());
          fqns.add(fqn);
        }
      }
    }

    if (filter.getOr() != null) {
      for (ConjunctiveCriterion cc : filter.getOr()) {
        for (Criterion c : cc.getAnd()) {
          if (c.getField().startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)) {
            String fqn = fieldOrFacetToFQN(c.getField());
            fqns.add(fqn);
          }
        }
      }
    }

    if (!fqns.isEmpty()) {
      validateStructuredPropertyFQN(fqns, Objects.requireNonNull(aspectRetriever));
    }
  }

  private static String fieldOrFacetToFQN(String fieldOrFacet) {
    return fieldOrFacet
        .substring(STRUCTURED_PROPERTY_MAPPING_FIELD.length() + 1)
        .replace(".keyword", "")
        .replace(".delimited", "");
  }

  public static Date toDate(PrimitivePropertyValue value) throws DateTimeParseException {
    return Date.valueOf(value.getString());
  }

  public static boolean isValidDate(PrimitivePropertyValue value) {
    if (value.getString() == null) {
      return false;
    }
    if (value.getString().length() != 10) {
      return false;
    }
    Date date;
    try {
      date = toDate(value);
    } catch (DateTimeParseException e) {
      return false;
    }
    return date.compareTo(MIN_DATE) >= 0 && date.compareTo(MAX_DATE) <= 0;
  }

  private static Set<Urn> getRemovedUrns(Set<Urn> urns, @Nonnull AspectRetriever aspectRetriever) {
    return aspectRetriever
        .getLatestAspectObjects(urns, ImmutableSet.of(STATUS_ASPECT_NAME))
        .entrySet()
        .stream()
        .filter(
            entry ->
                entry.getValue().containsKey(STATUS_ASPECT_NAME)
                    && new Status(entry.getValue().get(STATUS_ASPECT_NAME).data()).isRemoved())
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  /**
   * Given a collection of structured properties, return the structured properties with soft deleted
   * assignments removed
   *
   * @param properties collection of structured properties
   * @param aspectRetriever typically entity service or entity client + entity registry
   * @return structured properties object without value assignments for deleted structured
   *     properties and whether values were filtered
   */
  public static Map<Urn, Boolean> filterSoftDelete(
      Map<Urn, StructuredProperties> properties, AspectRetriever aspectRetriever) {
    final Set<Urn> structuredPropertiesUrns =
        properties.values().stream()
            .flatMap(structuredProperties -> structuredProperties.getProperties().stream())
            .map(StructuredPropertyValueAssignment::getPropertyUrn)
            .collect(Collectors.toSet());

    final Set<Urn> removedUrns = getRemovedUrns(structuredPropertiesUrns, aspectRetriever);

    return properties.entrySet().stream()
        .map(
            entry ->
                Pair.of(
                    entry.getKey(), filterSoftDelete(entry.getValue(), removedUrns).getSecond()))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  private static Pair<StructuredProperties, Boolean> filterSoftDelete(
      StructuredProperties structuredProperties, Set<Urn> softDeletedPropertyUrns) {

    Pair<StructuredPropertyValueAssignmentArray, Boolean> filtered =
        filterValueAssignment(structuredProperties.getProperties(), softDeletedPropertyUrns);

    if (filtered.getSecond()) {
      return Pair.of(structuredProperties.setProperties(filtered.getFirst()), true);
    } else {
      return Pair.of(structuredProperties, false);
    }
  }

  private static Pair<StructuredPropertyValueAssignmentArray, Boolean> filterValueAssignment(
      StructuredPropertyValueAssignmentArray in, Set<Urn> softDeletedPropertyUrns) {
    if (in.stream().noneMatch(p -> softDeletedPropertyUrns.contains(p.getPropertyUrn()))) {
      return Pair.of(in, false);
    } else {
      return Pair.of(
          new StructuredPropertyValueAssignmentArray(
              in.stream()
                  .filter(
                      assignment -> !softDeletedPropertyUrns.contains(assignment.getPropertyUrn()))
                  .collect(Collectors.toSet())),
          true);
    }
  }

  /*
   * We accept both ID and qualifiedName as inputs when creating a structured property. However,
   * these two fields should ALWAYS be the same. If they don't provide either, use a UUID for both.
   * If they provide both, ensure they are the same otherwise throw. Otherwise, use what is provided.
   */
  public static String getPropertyId(
      @Nullable final String inputId, @Nullable final String inputQualifiedName) {
    if (inputId != null && inputQualifiedName != null && !inputId.equals(inputQualifiedName)) {
      throw new IllegalArgumentException(
          "Qualified name and the ID of a structured property must match");
    }

    String id = UUID.randomUUID().toString();

    if (inputQualifiedName != null) {
      id = inputQualifiedName;
    } else if (inputId != null) {
      id = inputId;
    }

    return id;
  }

  /*
   * Ensure that a structured property settings aspect is valid by ensuring that if isHidden is true,
   * the other fields concerning display locations are false;
   */
  public static boolean validatePropertySettings(
      StructuredPropertySettings settings, boolean shouldThrow) {
    if (settings.isIsHidden()) {
      if (settings.isShowInSearchFilters()
          || settings.isShowInAssetSummary()
          || settings.isShowAsAssetBadge()
          || settings.isShowInColumnsTable()) {
        if (shouldThrow) {
          throw new IllegalArgumentException(INVALID_SETTINGS_MESSAGE);
        } else {
          return false;
        }
      }
    }
    return true;
  }
}
