package com.linkedin.metadata.search.elasticsearch.query.request;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.custom.AutocompleteConfiguration;
import com.linkedin.metadata.config.search.custom.BoolQueryConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.HighlightFields;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.query.SearchFlags;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.search.SearchModule;

@Slf4j
@Builder(builderMethodName = "hiddenBuilder")
@Getter
public class CustomizedQueryHandler {
  private static final NamedXContentRegistry X_CONTENT_REGISTRY;

  static {
    SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
    X_CONTENT_REGISTRY = new NamedXContentRegistry(searchModule.getNamedXContents());
  }

  @Nullable private CustomConfiguration config;
  private CustomSearchConfiguration customSearchConfiguration;

  @Builder.Default
  private List<Map.Entry<Pattern, QueryConfiguration>> queryConfigurations = List.of();

  @Builder.Default
  private List<Map.Entry<Pattern, AutocompleteConfiguration>> autocompleteConfigurations =
      List.of();

  public Optional<QueryConfiguration> lookupQueryConfig(String query) {
    return queryConfigurations.stream()
        .filter(e -> e.getKey().matcher(query).matches())
        .map(Map.Entry::getValue)
        .findFirst();
  }

  public Optional<AutocompleteConfiguration> lookupAutocompleteConfig(String query) {
    return autocompleteConfigurations.stream()
        .filter(e -> e.getKey().matcher(query).matches())
        .map(Map.Entry::getValue)
        .findFirst();
  }

  public static String unquote(String query) {
    return query.replaceAll("[\"']", "");
  }

  public static boolean isQuoted(String query) {
    return Stream.of("\"", "'").anyMatch(query::contains);
  }

  public static Optional<BoolQueryBuilder> boolQueryBuilder(
      @Nonnull ObjectMapper objectMapper,
      QueryConfiguration customQueryConfiguration,
      String query) {
    if (customQueryConfiguration.getBoolQuery() != null) {
      log.debug(
          "Using custom query configuration queryRegex: {}",
          customQueryConfiguration.getQueryRegex());
    }
    return Optional.ofNullable(customQueryConfiguration.getBoolQuery())
        .map(bq -> toBoolQueryBuilder(objectMapper, query, bq));
  }

  public static Optional<BoolQueryBuilder> boolQueryBuilder(
      @Nonnull ObjectMapper objectMapper,
      AutocompleteConfiguration customAutocompleteConfiguration,
      String query) {
    if (customAutocompleteConfiguration.getBoolQuery() != null) {
      log.debug(
          "Using custom query autocomplete queryRegex: {}",
          customAutocompleteConfiguration.getQueryRegex());
    }
    return Optional.ofNullable(customAutocompleteConfiguration.getBoolQuery())
        .map(bq -> toBoolQueryBuilder(objectMapper, query, bq));
  }

  private static BoolQueryBuilder toBoolQueryBuilder(
      @Nonnull ObjectMapper objectMapper, String query, BoolQueryConfiguration boolQuery) {
    try {
      String jsonFragment =
          objectMapper
              .writeValueAsString(boolQuery)
              .replace("\"{{query_string}}\"", objectMapper.writeValueAsString(query))
              .replace(
                  "\"{{unquoted_query_string}}\"", objectMapper.writeValueAsString(unquote(query)));
      XContentParser parser =
          XContentType.JSON
              .xContent()
              .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, jsonFragment);
      return BoolQueryBuilder.fromXContent(parser);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static FunctionScoreQueryBuilder functionScoreQueryBuilder(
      @Nonnull ObjectMapper objectMapper,
      @Nonnull QueryConfiguration customQueryConfiguration,
      QueryBuilder queryBuilder,
      String query) {
    return toFunctionScoreQueryBuilder(
        objectMapper, queryBuilder, customQueryConfiguration.getFunctionScore(), query);
  }

  public static Optional<FunctionScoreQueryBuilder> functionScoreQueryBuilder(
      @Nonnull ObjectMapper objectMapper,
      @Nonnull AutocompleteConfiguration customAutocompleteConfiguration,
      QueryBuilder queryBuilder,
      @Nullable QueryConfiguration customQueryConfiguration,
      String query) {

    Optional<FunctionScoreQueryBuilder> result = Optional.empty();

    if ((customAutocompleteConfiguration.getFunctionScore() == null
            || customAutocompleteConfiguration.getFunctionScore().isEmpty())
        && customAutocompleteConfiguration.isInheritFunctionScore()
        && customQueryConfiguration != null) {
      log.debug(
          "Inheriting query configuration for autocomplete function scoring: "
              + customQueryConfiguration);
      // inherit if not overridden
      result =
          Optional.of(
              toFunctionScoreQueryBuilder(
                  objectMapper, queryBuilder, customQueryConfiguration.getFunctionScore(), query));
    } else if (customAutocompleteConfiguration.getFunctionScore() != null
        && !customAutocompleteConfiguration.getFunctionScore().isEmpty()) {
      log.debug("Applying custom autocomplete function scores.");
      result =
          Optional.of(
              toFunctionScoreQueryBuilder(
                  objectMapper,
                  queryBuilder,
                  customAutocompleteConfiguration.getFunctionScore(),
                  query));
    }

    return result;
  }

  private static FunctionScoreQueryBuilder toFunctionScoreQueryBuilder(
      @Nonnull ObjectMapper objectMapper,
      @Nonnull QueryBuilder queryBuilder,
      @Nonnull Map<String, Object> params,
      String query) {
    try {
      HashMap<String, Object> body = new HashMap<>(params);
      if (!body.isEmpty()) {
        log.debug("Using custom scoring functions: {}", body);
      }

      body.put("query", objectMapper.readValue(queryBuilder.toString(), Map.class));

      String jsonFragment =
          objectMapper
              .writeValueAsString(Map.of("function_score", body))
              .replace("\"{{query_string}}\"", objectMapper.writeValueAsString(query))
              .replace(
                  "\"{{unquoted_query_string}}\"", objectMapper.writeValueAsString(unquote(query)));
      XContentParser parser =
          XContentType.JSON
              .xContent()
              .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, jsonFragment);
      return (FunctionScoreQueryBuilder) FunctionScoreQueryBuilder.parseInnerQueryBuilder(parser);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static CustomizedQueryHandlerBuilder builder(
      @Nonnull CustomConfiguration config,
      @Nullable CustomSearchConfiguration customSearchConfiguration) {
    CustomizedQueryHandlerBuilder builder =
        hiddenBuilder().customSearchConfiguration(customSearchConfiguration);

    builder.config(config);

    if (customSearchConfiguration != null) {
      builder.queryConfigurations(
          customSearchConfiguration.getQueryConfigurations().stream()
              .map(cfg -> Map.entry(Pattern.compile(cfg.getQueryRegex()), cfg))
              .collect(Collectors.toList()));
      builder.autocompleteConfigurations(
          customSearchConfiguration.getAutocompleteConfigurations().stream()
              .map(cfg -> Map.entry(Pattern.compile(cfg.getQueryRegex()), cfg))
              .collect(Collectors.toList()));
    }

    return builder;
  }

  /**
   * Apply field configuration to a set of search fields Only processes fields that actually exist
   * as searchable
   */
  public Set<SearchFieldConfig> applySearchFieldConfiguration(
      @Nonnull Set<SearchFieldConfig> baseFields, @Nullable String fieldConfigLabel) {

    if (customSearchConfiguration == null
        || customSearchConfiguration.getFieldConfigurations() == null
        || fieldConfigLabel == null) {
      return baseFields;
    }

    FieldConfiguration fieldConfig =
        customSearchConfiguration.getFieldConfigurations().get(fieldConfigLabel);

    if (fieldConfig == null || fieldConfig.getSearchFields() == null) {
      return baseFields;
    }

    SearchFields searchFields = fieldConfig.getSearchFields();

    // Validate configuration
    if (!searchFields.isValid()) {
      log.error(
          "Invalid field configuration for label: {}. Replace cannot be used with add/remove.",
          fieldConfigLabel);
      return baseFields;
    }

    // Handle replace mode - only include fields that exist
    if (!searchFields.getReplace().isEmpty()) {
      Set<SearchFieldConfig> replacementFields =
          createSearchFieldConfigsFromNames(searchFields.getReplace(), baseFields, true);

      if (replacementFields.isEmpty()) {
        log.warn(
            "Field configuration replace resulted in no valid fields for label: {}. "
                + "Using base fields instead.",
            fieldConfigLabel);
        return baseFields;
      }

      return replacementFields;
    }

    // Handle add/remove mode
    // Use LinkedHashMap to preserve order and automatically handle duplicates
    Map<String, SearchFieldConfig> resultFieldsMap =
        baseFields.stream()
            .collect(
                Collectors.toMap(
                    SearchFieldConfig::fieldName, f -> f, (v1, v2) -> v1, LinkedHashMap::new));

    // Remove fields - treating field names as literals
    if (!searchFields.getRemove().isEmpty()) {
      for (String fieldToRemove : searchFields.getRemove()) {
        resultFieldsMap.remove(fieldToRemove);
      }
    }

    // Add fields - only those that exist
    if (!searchFields.getAdd().isEmpty()) {
      Set<SearchFieldConfig> fieldsToAdd =
          createSearchFieldConfigsFromNames(searchFields.getAdd(), baseFields, true);

      // Add fields to the map (will replace if already exists, preventing duplicates)
      fieldsToAdd.forEach(field -> resultFieldsMap.put(field.fieldName(), field));
    }

    if (resultFieldsMap.isEmpty()) {
      log.warn(
          "Field configuration resulted in no searchable fields for label: {}. "
              + "Using base fields instead.",
          fieldConfigLabel);
      return baseFields;
    }

    return new HashSet<>(resultFieldsMap.values());
  }

  /**
   * Apply field configuration to autocomplete fields Only processes fields that actually exist as
   * searchable with autocomplete enabled
   */
  public List<Pair<String, String>> applyAutocompleteFieldConfiguration(
      @Nonnull List<Pair<String, String>> baseFields, @Nullable String fieldConfigLabel) {

    if (customSearchConfiguration == null
        || customSearchConfiguration.getFieldConfigurations() == null
        || fieldConfigLabel == null) {
      return baseFields;
    }

    FieldConfiguration fieldConfig =
        customSearchConfiguration.getFieldConfigurations().get(fieldConfigLabel);

    if (fieldConfig == null || fieldConfig.getSearchFields() == null) {
      log.warn("No field configuration found for label: {}", fieldConfigLabel);
      return baseFields;
    }

    SearchFields searchFields = fieldConfig.getSearchFields();

    // Validate configuration
    if (!searchFields.isValid()) {
      log.error(
          "Invalid field configuration for label: {}. Replace cannot be used with add/remove.",
          fieldConfigLabel);
      return baseFields;
    }

    // Convert to map for easier manipulation - LinkedHashMap to avoid duplicates
    Map<String, String> baseFieldMap =
        baseFields.stream()
            .collect(
                Collectors.toMap(
                    Pair::getLeft, Pair::getRight, (v1, v2) -> v1, LinkedHashMap::new));

    // Handle replace mode
    if (!searchFields.getReplace().isEmpty()) {
      Map<String, String> replacementFields = new LinkedHashMap<>();
      Set<String> notFoundFields = new HashSet<>();

      for (String field : searchFields.getReplace()) {
        String boost = findBoostForField(field, baseFieldMap);
        if (boost != null) {
          replacementFields.put(field, boost);
        } else {
          notFoundFields.add(field);
        }
      }

      if (!notFoundFields.isEmpty()) {
        log.warn(
            "Field configuration references non-autocomplete fields: {}. These fields will be ignored.",
            notFoundFields);
      }

      if (replacementFields.isEmpty()) {
        log.warn(
            "Field configuration replace resulted in no valid autocomplete fields for label: {}. "
                + "Using base fields instead.",
            fieldConfigLabel);
        return baseFields;
      }

      return replacementFields.entrySet().stream()
          .sorted(
              (p1, p2) -> {
                // Sort by boost score (descending) then by field name
                int boostCompare =
                    Double.compare(
                        Double.parseDouble(p2.getValue()), Double.parseDouble(p1.getValue()));
                return boostCompare != 0 ? boostCompare : p1.getKey().compareTo(p2.getKey());
              })
          .map(e -> Pair.of(e.getKey(), e.getValue()))
          .collect(Collectors.toList());
    }

    // Handle add/remove mode
    Map<String, String> resultFieldMap = new LinkedHashMap<>(baseFieldMap);

    // Remove fields - treating as literals
    if (!searchFields.getRemove().isEmpty()) {
      searchFields.getRemove().forEach(resultFieldMap::remove);
    }

    // Add fields
    if (!searchFields.getAdd().isEmpty()) {
      Set<String> notFoundFields = new HashSet<>();

      for (String fieldToAdd : searchFields.getAdd()) {
        if (!resultFieldMap.containsKey(fieldToAdd)) {
          // Try to find boost from base fields or use default
          String boost = findBoostForField(fieldToAdd, baseFieldMap);
          if (boost != null) {
            resultFieldMap.put(fieldToAdd, boost);
          } else {
            notFoundFields.add(fieldToAdd);
          }
        }
      }

      if (!notFoundFields.isEmpty()) {
        log.warn(
            "Cannot add non-autocomplete fields: {}. These fields will be ignored.",
            notFoundFields);
      }
    }

    if (resultFieldMap.isEmpty()) {
      log.warn(
          "Field configuration resulted in no autocomplete fields for label: {}. "
              + "Using base fields instead.",
          fieldConfigLabel);
      return baseFields;
    }

    // Convert back to list of pairs
    return resultFieldMap.entrySet().stream()
        .map(e -> Pair.of(e.getKey(), e.getValue()))
        .sorted(
            (p1, p2) -> {
              // Sort by boost score (descending) then by field name
              int boostCompare =
                  Double.compare(
                      Double.parseDouble(p2.getRight()), Double.parseDouble(p1.getRight()));
              return boostCompare != 0 ? boostCompare : p1.getLeft().compareTo(p2.getLeft());
            })
        .collect(Collectors.toList());
  }

  /**
   * Find boost score for a field from reference fields Checks exact match and base field patterns
   */
  private String findBoostForField(String fieldName, Map<String, String> referenceFields) {
    // Check for exact match
    if (referenceFields.containsKey(fieldName)) {
      return referenceFields.get(fieldName);
    }

    // Check for base field (e.g., "name" for "name.delimited")
    if (fieldName.contains(".")) {
      String baseField = fieldName.substring(0, fieldName.indexOf("."));
      if (referenceFields.containsKey(baseField)) {
        return referenceFields.get(baseField);
      }
    }

    // Check if this field is a known subfield pattern
    for (Map.Entry<String, String> entry : referenceFields.entrySet()) {
      String refField = entry.getKey();
      // Check if fieldName is a subfield of refField
      if (fieldName.startsWith(refField + ".")) {
        return entry.getValue();
      }
    }

    return null;
  }

  /**
   * Get highlight field configuration with tracking of explicitly configured fields. This method
   * returns both the fields to highlight and which fields were explicitly configured.
   */
  public HighlightConfigurationResult getHighlightFieldConfiguration(
      @Nonnull Set<String> baseFields, @Nullable String fieldConfigLabel) {

    if (customSearchConfiguration == null || fieldConfigLabel == null) {
      return HighlightConfigurationResult.builder()
          .fieldsToHighlight(baseFields)
          .explicitlyConfiguredFields(Collections.emptySet())
          .build();
    }

    FieldConfiguration fieldConfig =
        customSearchConfiguration.getFieldConfigurations().get(fieldConfigLabel);

    if (fieldConfig == null || fieldConfig.getHighlightFields() == null) {
      return HighlightConfigurationResult.builder()
          .fieldsToHighlight(baseFields)
          .explicitlyConfiguredFields(Collections.emptySet())
          .build();
    }

    HighlightFields highlightFields = fieldConfig.getHighlightFields();
    Set<String> explicitlyConfiguredFields = new HashSet<>();

    // Handle replace mode
    if (!highlightFields.getReplace().isEmpty()) {
      explicitlyConfiguredFields.addAll(highlightFields.getReplace());
      return HighlightConfigurationResult.builder()
          .fieldsToHighlight(new HashSet<>(highlightFields.getReplace()))
          .explicitlyConfiguredFields(explicitlyConfiguredFields)
          .build();
    }

    // Handle add/remove mode
    Set<String> resultFields = new HashSet<>(baseFields);

    if (!highlightFields.getRemove().isEmpty()) {
      Set<String> expandedFieldsToRemove =
          expandFieldPatterns(highlightFields.getRemove(), baseFields);
      resultFields.removeAll(expandedFieldsToRemove);
    }

    if (!highlightFields.getAdd().isEmpty()) {
      explicitlyConfiguredFields.addAll(highlightFields.getAdd());
      resultFields.addAll(highlightFields.getAdd());
    }

    return HighlightConfigurationResult.builder()
        .fieldsToHighlight(resultFields)
        .explicitlyConfiguredFields(explicitlyConfiguredFields)
        .build();
  }

  /**
   * Apply highlight field configuration. Uses LinkedHashSet to maintain order while preventing
   * duplicates.
   */
  public Set<String> applyHighlightFieldConfiguration(
      @Nonnull Set<String> baseFields, @Nullable String fieldConfigLabel) {

    if (customSearchConfiguration == null || fieldConfigLabel == null) {
      return baseFields;
    }

    FieldConfiguration fieldConfig =
        customSearchConfiguration.getFieldConfigurations().get(fieldConfigLabel);

    if (fieldConfig == null || fieldConfig.getHighlightFields() == null) {
      return baseFields;
    }

    HighlightFields highlightFields = fieldConfig.getHighlightFields();

    // Handle replace mode
    if (!highlightFields.getReplace().isEmpty()) {
      // LinkedHashSet maintains insertion order and prevents duplicates
      return new LinkedHashSet<>(highlightFields.getReplace());
    }

    // Handle add/remove mode
    Set<String> resultFields = new LinkedHashSet<>(baseFields);

    if (!highlightFields.getRemove().isEmpty()) {
      resultFields.removeAll(highlightFields.getRemove());
    }

    if (!highlightFields.getAdd().isEmpty()) {
      resultFields.addAll(highlightFields.getAdd());
    }

    return resultFields;
  }

  /** Check if highlighting is enabled for a configuration */
  public boolean isHighlightingEnabled(@Nullable String fieldConfigLabel) {
    if (customSearchConfiguration == null || fieldConfigLabel == null) {
      return true; // Default enabled
    }

    FieldConfiguration fieldConfig =
        customSearchConfiguration.getFieldConfigurations().get(fieldConfigLabel);

    if (fieldConfig == null || fieldConfig.getHighlightFields() == null) {
      return true; // Default enabled
    }

    return fieldConfig.getHighlightFields().isEnabled();
  }

  public String resolveFieldConfiguration(
      @Nullable SearchFlags searchFlags, Function<CustomConfiguration, String> labelFunction) {
    if (searchFlags == null || searchFlags.getFieldConfiguration() == null) {
      return config != null ? labelFunction.apply(config) : null;
    }
    return searchFlags.getFieldConfiguration();
  }

  /**
   * Create SearchFieldConfig objects from field names Only returns configs for fields that exist in
   * the reference set
   *
   * @param forceQueryByDefault if true, sets queryByDefault=true for explicitly configured fields
   */
  /**
   * Create SearchFieldConfig objects from field names Only returns configs for fields that exist in
   * the reference set
   *
   * @param forceQueryByDefault if true, sets queryByDefault=true for explicitly configured fields
   */
  private Set<SearchFieldConfig> createSearchFieldConfigsFromNames(
      List<String> fieldNames,
      Set<SearchFieldConfig> referenceFields,
      boolean forceQueryByDefault) {

    Map<String, SearchFieldConfig> referenceMap =
        referenceFields.stream()
            .collect(Collectors.toMap(SearchFieldConfig::fieldName, f -> f, (v1, v2) -> v1));

    // Use LinkedHashMap to preserve order and avoid duplicates
    Map<String, SearchFieldConfig> resultMap = new LinkedHashMap<>();
    Set<String> notFoundFields = new HashSet<>();

    for (String fieldName : fieldNames) {
      SearchFieldConfig fieldConfig = findReferenceField(fieldName, referenceMap);
      if (fieldConfig != null) {
        // Only force queryByDefault for explicitly configured fields
        if (forceQueryByDefault && !fieldConfig.isQueryByDefault()) {
          fieldConfig = fieldConfig.toBuilder().isQueryByDefault(true).build();
          log.debug("Setting queryByDefault=true for explicitly configured field: {}", fieldName);
        }
        // Using map ensures no duplicates by field name
        resultMap.put(fieldConfig.fieldName(), fieldConfig);
      } else {
        notFoundFields.add(fieldName);
      }
    }

    if (!notFoundFields.isEmpty()) {
      log.warn(
          "Field configuration references non-searchable fields: {}. These fields will be ignored.",
          notFoundFields);
    }

    return new HashSet<>(resultMap.values());
  }

  /**
   * Expand field patterns (e.g., "schema.*") to actual field names and validate they exist in
   * available fields.
   *
   * @param patterns List of field patterns, can include wildcards like "schema.*"
   * @param availableFields Set of valid field names to match against
   * @return Expanded set of field names, excluding any patterns that don't match
   */
  private Set<String> expandFieldPatterns(List<String> patterns, Set<String> availableFields) {
    Set<String> expanded = new HashSet<>();
    Set<String> notFoundPatterns = new HashSet<>();

    for (String pattern : patterns) {
      if (pattern.endsWith(".*")) {
        String prefix = pattern.substring(0, pattern.length() - 2);
        Set<String> matchedFields =
            availableFields.stream()
                .filter(field -> field.startsWith(prefix + "."))
                .collect(Collectors.toSet());

        if (matchedFields.isEmpty()) {
          notFoundPatterns.add(pattern);
        } else {
          expanded.addAll(matchedFields);
        }
      } else {
        if (availableFields.contains(pattern)) {
          expanded.add(pattern);
        } else {
          notFoundPatterns.add(pattern);
        }
      }
    }

    if (!notFoundPatterns.isEmpty()) {
      log.warn(
          "Field configuration contains patterns that don't match any searchable fields: {}. "
              + "These patterns will be ignored.",
          notFoundPatterns);
    }

    return expanded;
  }

  private SearchFieldConfig findReferenceField(
      String fieldName, Map<String, SearchFieldConfig> referenceMap) {
    // Direct match
    if (referenceMap.containsKey(fieldName)) {
      return referenceMap.get(fieldName);
    }
    // Check if it's a subfield
    String baseField = fieldName.split("\\.")[0];
    return referenceMap.get(baseField);
  }
}
