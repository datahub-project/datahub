package com.linkedin.metadata.service.util;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetadataTestServiceUtils {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String KEYWORD_SUFFIX = ".keyword";
  private static final List<String> UNARY_OPERATORS = ImmutableList.of("exists");

  private MetadataTestServiceUtils() {}

  public static void applyAppSource(
      @Nonnull List<MetadataChangeProposal> changes, @Nonnull String appSource) {
    changes.forEach(
        change -> {
          SystemMetadata systemMetadata =
              Optional.ofNullable(change.getSystemMetadata()).orElse(createDefaultSystemMetadata());
          StringMap properties =
              Optional.ofNullable(systemMetadata.getProperties()).orElse(new StringMap());
          properties.put(APP_SOURCE, appSource);
          systemMetadata.setProperties(properties);
          change.setSystemMetadata(systemMetadata);
        });
  }

  public static List<JsonNode> convertFilterToTestConditions(@Nullable final Filter filter) {
    return convertFilterToTestConditions(filter, null);
  }

  public static List<JsonNode> convertFilterToTestConditions(
      @Nullable final Filter filter,
      @Nullable Map<String, List<PathSpec>> searchableFieldsToPathSpecs) {
    // We know we have an OR of ANDs to deal with.
    if (filter != null && filter.hasOr()) {
      final List<JsonNode> orConditions = new ArrayList<>();
      for (ConjunctiveCriterion orCondition : filter.getOr()) {
        ObjectNode andNode = OBJECT_MAPPER.createObjectNode();
        final List<JsonNode> andConditions = new ArrayList<>();
        // Build AND conditions:
        for (Criterion andCriterion : orCondition.getAnd()) {
          andConditions.add(
              convertCriterionToTestConditions(andCriterion, searchableFieldsToPathSpecs));
        }
        andNode.put("and", OBJECT_MAPPER.createArrayNode().addAll(andConditions));
        orConditions.add(andNode);
      }
      return orConditions;
    }
    log.warn(String.format("Found form assignment with empty filter conditions! %s", filter));
    return Collections.emptyList();
  }

  private static ObjectNode convertCriterionToTestConditions(
      @Nonnull final Criterion criterion,
      @Nullable Map<String, List<PathSpec>> searchableFieldsToPathSpecs) {
    // special case - everything has entityType indexed
    if (criterion.getField().equals("_entityType") || criterion.getField().equals("entityType")) {
      return convertCriterionToTestCondition(criterion, "_entityType");
    }
    // everyone has an urn field
    if (criterion.getField().equals("urn")) {
      return convertCriterionToTestCondition(criterion, "urn");
    }

    if (searchableFieldsToPathSpecs != null) {
      final String filterField = criterion.getField().replace(KEYWORD_SUFFIX, "");
      final List<PathSpec> pathSpecs = new ArrayList<>();
      if (FIELDS_TO_EXPANDED_FIELDS_LIST.containsKey(filterField)) {
        // If a filter field should be expanded,
        List<String> expandedFields = FIELDS_TO_EXPANDED_FIELDS_LIST.get(filterField);
        expandedFields.forEach(
            field ->
                pathSpecs.addAll(
                    searchableFieldsToPathSpecs.get(field.replace(KEYWORD_SUFFIX, ""))));
      } else {
        pathSpecs.addAll(searchableFieldsToPathSpecs.get(filterField));
      }

      if (pathSpecs.size() == 1) {
        PathSpec pathSpec = pathSpecs.get(0);
        String field = String.join(".", pathSpec.getPathComponents());
        return convertCriterionToTestCondition(criterion, field);
      }
      // if there's more than one way to get to this searchable field, turn criterion into list of
      // ors for each path
      if (pathSpecs.size() > 1) {
        ObjectNode orNode = OBJECT_MAPPER.createObjectNode();
        final List<JsonNode> orConditions = new ArrayList<>();
        for (PathSpec spec : pathSpecs) {
          String field = String.join(".", spec.getPathComponents());
          orConditions.add(convertCriterionToTestCondition(criterion, field));
        }
        orNode.put("or", OBJECT_MAPPER.createArrayNode().addAll(orConditions));
        return orNode;
      }
      throw new IllegalArgumentException(
          String.format("Unsupported filter field with no path specs: %s", criterion.getField()));
    }

    final String finalField = mapFilterField(criterion.getField());
    return convertCriterionToTestCondition(criterion, finalField);
  }

  private static ObjectNode convertCriterionToTestCondition(
      @Nonnull final Criterion criterion, @Nonnull final String field) {

    final String finalOperator = mapFilterOperator(criterion.getCondition());
    final List<String> finalValues = criterion.getValues();
    final String value = criterion.getValue();

    final ObjectNode predicateNode = OBJECT_MAPPER.createObjectNode();
    predicateNode.put("property", field);
    predicateNode.put("operator", finalOperator);
    ArrayNode valuesNode = OBJECT_MAPPER.createArrayNode();
    if (finalValues.size() == 0) {
      valuesNode.add(value);
    } else {
      finalValues.forEach(valuesNode::add);
    }
    if (!UNARY_OPERATORS.contains(finalOperator)) {
      predicateNode.put("values", valuesNode);
    }

    // Finally, if we are negating the conditions, negate that here.
    if (criterion.isNegated()) {
      ObjectNode notNode = OBJECT_MAPPER.createObjectNode();
      notNode.put("not", predicateNode);
      return notNode;
    }

    // Otherwise, return the original predicate node.
    return predicateNode;
  }

  @Nonnull
  private static String mapFilterField(@Nonnull final String filterField) {
    // TODO: Refactor this for extensibility.
    switch (filterField) {
      case "platform":
      case "platform.keyword":
        return "dataPlatformInstance.platform";
      case "domain":
      case "domains":
      case "domains.keyword":
        return "domains.domains";
      case "container":
      case "container.keyword":
        return "container.container";
      case "_entityType":
      case "entityType":
        return "_entityType";
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported filter field %s provided in test conditions", filterField));
    }
  }

  @Nonnull
  private static String mapFilterOperator(@Nonnull final Condition filterCondition) {
    // TODO: Refactor this for extensibility.
    switch (filterCondition) {
      case EQUAL:
      case IN:
        return "equals";
      case GREATER_THAN:
        return "greater_than";
      case LESS_THAN:
        return "less_than";
      case EXISTS:
        return "exists";
      case CONTAIN:
        return "contains_str";
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported filter operator %s provided in test conditions", filterCondition));
    }
  }
}
