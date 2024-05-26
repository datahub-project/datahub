package com.linkedin.metadata.models;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.util.Pair;
import java.sql.Date;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StructuredPropertyUtils {

  private StructuredPropertyUtils() {}

  static final Date MIN_DATE = Date.valueOf("1000-01-01");
  static final Date MAX_DATE = Date.valueOf("9999-12-31");

  /**
   * Sanitizes fully qualified name for use in an ElasticSearch field name Replaces . and " "
   * characters
   *
   * @param fullyQualifiedName The original fully qualified name of the property
   * @return The sanitized version that can be used as a field name
   */
  public static String sanitizeStructuredPropertyFQN(@Nonnull String fullyQualifiedName) {
    if (fullyQualifiedName.contains(" ")) {
      throw new IllegalArgumentException(
          "Fully qualified structured property name cannot contain spaces");
    }
    return fullyQualifiedName.replace('.', '_');
  }

  public static void validateStructuredPropertyFQN(
      @Nonnull Collection<String> fullyQualifiedNames, @Nonnull AspectRetriever aspectRetriever) {
    Set<Urn> structuredPropertyUrns =
        fullyQualifiedNames.stream()
            .map(StructuredPropertyUtils::toURNFromFieldName)
            .collect(Collectors.toSet());
    Set<Urn> removedUrns = getRemovedUrns(structuredPropertyUrns, aspectRetriever);
    if (!removedUrns.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Cannot filter on deleted Structured Property %s", removedUrns));
    }
  }

  public static Urn toURNFromFieldName(@Nonnull String fieldName) {
    return UrnUtils.getUrn(
        String.join(":", "urn:li", STRUCTURED_PROPERTY_ENTITY_NAME, fieldName.replace('_', '.')));
  }

  public static void validateFilter(
      @Nullable Filter filter, @Nonnull AspectRetriever aspectRetriever) {

    if (filter == null) {
      return;
    }

    Set<String> fieldNames = new HashSet<>();

    if (filter.getCriteria() != null) {
      for (Criterion c : filter.getCriteria()) {
        if (c.getField().startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)) {
          fieldNames.add(c.getField().substring(STRUCTURED_PROPERTY_MAPPING_FIELD.length() + 1));
        }
      }
    }

    if (filter.getOr() != null) {
      for (ConjunctiveCriterion cc : filter.getOr()) {
        for (Criterion c : cc.getAnd()) {
          if (c.getField().startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)) {
            fieldNames.add(c.getField().substring(STRUCTURED_PROPERTY_MAPPING_FIELD.length() + 1));
          }
        }
      }
    }

    if (!fieldNames.isEmpty()) {
      validateStructuredPropertyFQN(fieldNames, aspectRetriever);
    }
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

  private static Set<Urn> getRemovedUrns(Set<Urn> urns, AspectRetriever aspectRetriever) {
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
}
