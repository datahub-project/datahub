package com.linkedin.metadata.search.query.request;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.BoolQueryConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.HighlightFields;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.HighlightConfigurationResult;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;
import org.testng.annotations.Test;

public class CustomizedQueryHandlerTest {
  public static final ObjectMapper TEST_MAPPER = new YAMLMapper();
  private static final CustomConfiguration TEST_CONFIG = new CustomConfiguration();
  private static final CustomSearchConfiguration TEST_SEARCH_CONFIG;

  static {
    try {
      int maxSize =
          Integer.parseInt(
              System.getenv()
                  .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
      TEST_MAPPER
          .getFactory()
          .setStreamReadConstraints(
              StreamReadConstraints.builder().maxStringLength(maxSize).build());
      CustomConfiguration customConfiguration = new CustomConfiguration();
      customConfiguration.setEnabled(true);
      customConfiguration.setFile("search_config_test.yml");
      TEST_SEARCH_CONFIG = customConfiguration.resolve(TEST_MAPPER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static final SearchQueryBuilder SEARCH_QUERY_BUILDER;

  static {
    SEARCH_QUERY_BUILDER = new SearchQueryBuilder(new SearchConfiguration(), TEST_SEARCH_CONFIG);
  }

  private static final List<QueryConfiguration> EXPECTED_CONFIGURATION =
      List.of(
          QueryConfiguration.builder()
              .queryRegex("[*]|")
              .simpleQuery(false)
              .exactMatchQuery(false)
              .prefixMatchQuery(false)
              .functionScore(
                  Map.of(
                      "score_mode",
                      "avg",
                      "boost_mode",
                      "multiply",
                      "functions",
                      List.of(
                          Map.of(
                              "weight",
                              1,
                              "filter",
                              Map.<String, Object>of("match_all", Map.<String, Object>of())),
                          Map.of(
                              "weight",
                              0.5,
                              "filter",
                              Map.<String, Object>of(
                                  "term", Map.of("materialized", Map.of("value", true)))),
                          Map.of(
                              "weight",
                              0.5,
                              "filter",
                              Map.<String, Object>of(
                                  "term",
                                  Map.<String, Object>of("deprecated", Map.of("value", true)))))))
              .build(),
          QueryConfiguration.builder()
              .queryRegex(".*")
              .simpleQuery(true)
              .exactMatchQuery(true)
              .prefixMatchQuery(true)
              .boolQuery(
                  BoolQueryConfiguration.builder()
                      .must(List.of(Map.of("term", Map.of("name", "{{query_string}}"))))
                      .build())
              .functionScore(
                  Map.of(
                      "score_mode",
                      "avg",
                      "boost_mode",
                      "multiply",
                      "functions",
                      List.of(
                          Map.of(
                              "weight",
                              1,
                              "filter",
                              Map.<String, Object>of("match_all", Map.<String, Object>of())),
                          Map.of(
                              "weight",
                              0.5,
                              "filter",
                              Map.<String, Object>of(
                                  "term", Map.of("materialized", Map.of("value", true)))),
                          Map.of(
                              "weight",
                              1.5,
                              "filter",
                              Map.<String, Object>of(
                                  "term",
                                  Map.<String, Object>of("deprecated", Map.of("value", false)))))))
              .build());

  @Test
  public void configParsingTest() {
    assertNotNull(TEST_SEARCH_CONFIG);
    assertEquals(TEST_SEARCH_CONFIG.getQueryConfigurations(), EXPECTED_CONFIGURATION);
  }

  @Test
  public void customizedQueryHandlerInitTest() {
    CustomizedQueryHandler test =
        CustomizedQueryHandler.builder(TEST_CONFIG, TEST_SEARCH_CONFIG).build();

    assertEquals(
        test.getQueryConfigurations().stream()
            .map(e -> e.getKey().toString())
            .collect(Collectors.toList()),
        List.of("[*]|", ".*"));

    assertEquals(
        test.getQueryConfigurations().stream()
            .map(e -> Map.entry(e.getKey().toString(), e.getValue()))
            .collect(Collectors.toList()),
        EXPECTED_CONFIGURATION.stream()
            .map(cfg -> Map.entry(cfg.getQueryRegex(), cfg))
            .collect(Collectors.toList()));
  }

  @Test
  public void patternMatchTest() {
    CustomizedQueryHandler test =
        CustomizedQueryHandler.builder(TEST_CONFIG, TEST_SEARCH_CONFIG).build();

    for (String selectAllQuery : List.of("*", "")) {
      QueryConfiguration actual = test.lookupQueryConfig(selectAllQuery).get();
      assertEquals(
          actual,
          EXPECTED_CONFIGURATION.get(0),
          String.format("Failed to match: `%s`", selectAllQuery));
    }

    for (String otherQuery : List.of("foo", "bar")) {
      QueryConfiguration actual = test.lookupQueryConfig(otherQuery).get();
      assertEquals(actual, EXPECTED_CONFIGURATION.get(1));
    }
  }

  @Test
  public void functionScoreQueryBuilderTest() {
    CustomizedQueryHandler test =
        CustomizedQueryHandler.builder(TEST_CONFIG, TEST_SEARCH_CONFIG).build();
    MatchAllQueryBuilder inputQuery = QueryBuilders.matchAllQuery();

    /*
     * Test select star
     */
    FunctionScoreQueryBuilder selectStarTest =
        CustomizedQueryHandler.functionScoreQueryBuilder(
            new ObjectMapper(), test.lookupQueryConfig("*").get(), inputQuery, "*");

    FunctionScoreQueryBuilder.FilterFunctionBuilder[] expectedSelectStarScoreFunctions = {
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.weightFactorFunction(1f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("materialized", true),
          ScoreFunctionBuilders.weightFactorFunction(0.5f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("deprecated", true),
          ScoreFunctionBuilders.weightFactorFunction(0.5f))
    };
    FunctionScoreQueryBuilder expectedSelectStar =
        new FunctionScoreQueryBuilder(expectedSelectStarScoreFunctions)
            .scoreMode(FunctionScoreQuery.ScoreMode.AVG)
            .boostMode(CombineFunction.MULTIPLY);

    assertEquals(selectStarTest, expectedSelectStar);

    /*
     * Test default (non-select start)
     */
    FunctionScoreQueryBuilder defaultTest =
        CustomizedQueryHandler.functionScoreQueryBuilder(
            new ObjectMapper(), test.lookupQueryConfig("foobar").get(), inputQuery, "foobar");

    FunctionScoreQueryBuilder.FilterFunctionBuilder[] expectedDefaultScoreFunctions = {
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.weightFactorFunction(1f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("materialized", true),
          ScoreFunctionBuilders.weightFactorFunction(0.5f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("deprecated", false),
          ScoreFunctionBuilders.weightFactorFunction(1.5f))
    };
    FunctionScoreQueryBuilder expectedDefault =
        new FunctionScoreQueryBuilder(expectedDefaultScoreFunctions)
            .scoreMode(FunctionScoreQuery.ScoreMode.AVG)
            .boostMode(CombineFunction.MULTIPLY);

    assertEquals(defaultTest, expectedDefault);
  }

  @Test
  public void testSearchFieldConfigurationWithReplace() {
    // Create a custom configuration with field configurations
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "minimal",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder().replace(List.of("name", "description")).build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    // Create base fields
    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build(),
            SearchFieldConfig.builder()
                .fieldName("description")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build(),
            SearchFieldConfig.builder()
                .fieldName("tags")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build(),
            SearchFieldConfig.builder()
                .fieldName("owners")
                .boost(1.0f)
                .isQueryByDefault(false)
                .build());

    Set<SearchFieldConfig> result = handler.applySearchFieldConfiguration(baseFields, "minimal");

    assertEquals(result.size(), 2);
    assertTrue(result.stream().anyMatch(f -> f.fieldName().equals("name")));
    assertTrue(result.stream().anyMatch(f -> f.fieldName().equals("description")));
    assertFalse(result.stream().anyMatch(f -> f.fieldName().equals("tags")));
  }

  @Test
  public void testSearchFieldConfigurationWithAddRemove() {
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "custom",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .add(List.of("platform"))
                                .remove(List.of("owners"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build(),
            SearchFieldConfig.builder()
                .fieldName("owners")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build(),
            SearchFieldConfig.builder()
                .fieldName("platform")
                .boost(1.0f)
                .isQueryByDefault(false)
                .build());

    Set<SearchFieldConfig> result = handler.applySearchFieldConfiguration(baseFields, "custom");

    assertEquals(result.size(), 2);
    assertTrue(result.stream().anyMatch(f -> f.fieldName().equals("name")));
    assertTrue(result.stream().anyMatch(f -> f.fieldName().equals("platform")));
    assertFalse(result.stream().anyMatch(f -> f.fieldName().equals("owners")));

    // Verify that platform was set to queryByDefault=true
    SearchFieldConfig platformField =
        result.stream().filter(f -> f.fieldName().equals("platform")).findFirst().orElse(null);
    assertNotNull(platformField);
    assertTrue(platformField.isQueryByDefault());
  }

  @Test
  public void testSearchFieldConfigurationInvalidReplace() {
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "invalid",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder().replace(List.of("nonexistent")).build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build());

    // Should fall back to base fields when replace results in empty set
    Set<SearchFieldConfig> result = handler.applySearchFieldConfiguration(baseFields, "invalid");
    assertEquals(result, baseFields);
  }

  @Test
  public void testAutocompleteFieldConfiguration() {
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "name-only",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder().replace(List.of("name", "urn")).build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    List<Pair<String, String>> baseFields =
        List.of(
            Pair.of("name", "2.0"),
            Pair.of("description", "1.5"),
            Pair.of("tags", "1.0"),
            Pair.of("urn", "1.0"));

    List<Pair<String, String>> result =
        handler.applyAutocompleteFieldConfiguration(baseFields, "name-only");

    assertEquals(result.size(), 2);
    assertTrue(
        result.stream().anyMatch(p -> p.getLeft().equals("name") && p.getRight().equals("2.0")));
    assertTrue(
        result.stream().anyMatch(p -> p.getLeft().equals("urn") && p.getRight().equals("1.0")));
  }

  @Test
  public void testHighlightFieldConfiguration() {
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "no-highlight",
                        FieldConfiguration.builder()
                            .highlightFields(HighlightFields.builder().enabled(false).build())
                            .build(),
                    "custom-highlight",
                        FieldConfiguration.builder()
                            .highlightFields(
                                HighlightFields.builder()
                                    .enabled(true)
                                    .replace(List.of("name", "description"))
                                    .build())
                            .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    // Test disabled highlighting
    assertFalse(handler.isHighlightingEnabled("no-highlight"));
    assertTrue(handler.isHighlightingEnabled("unknown-label")); // Default is enabled

    // Test highlight field replacement
    Set<String> baseFields = Set.of("name", "description", "tags", "owners");
    Set<String> result = handler.applyHighlightFieldConfiguration(baseFields, "custom-highlight");

    assertEquals(result.size(), 2);
    assertTrue(result.contains("name"));
    assertTrue(result.contains("description"));
    assertFalse(result.contains("tags"));
  }

  @Test
  public void testFieldConfigurationValidation() {
    // Test invalid configuration with both replace and add
    SearchFields invalidFields =
        SearchFields.builder().replace(List.of("name")).add(List.of("description")).build();

    assertFalse(invalidFields.isValid());

    // Test valid configurations
    SearchFields validReplace = SearchFields.builder().replace(List.of("name")).build();
    assertTrue(validReplace.isValid());

    SearchFields validAddRemove =
        SearchFields.builder().add(List.of("name")).remove(List.of("description")).build();
    assertTrue(validAddRemove.isValid());
  }

  @Test
  public void testNullAndMissingFieldConfiguration() {
    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, null).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build());

    // Should return base fields when no configuration
    assertEquals(handler.applySearchFieldConfiguration(baseFields, null), baseFields);
    assertEquals(handler.applySearchFieldConfiguration(baseFields, "nonexistent"), baseFields);

    // Highlighting should be enabled by default
    assertTrue(handler.isHighlightingEnabled(null));
    assertTrue(handler.isHighlightingEnabled("nonexistent"));
  }

  @Test
  public void testAutocompleteBoostPreservation() {
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "reorder",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .replace(List.of("tags", "description", "name"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    List<Pair<String, String>> baseFields =
        List.of(Pair.of("name", "3.0"), Pair.of("description", "2.0"), Pair.of("tags", "1.0"));

    List<Pair<String, String>> result =
        handler.applyAutocompleteFieldConfiguration(baseFields, "reorder");

    // Should be sorted by boost (descending) then by name
    assertEquals(result.size(), 3);
    assertEquals(result.get(0), Pair.of("name", "3.0"));
    assertEquals(result.get(1), Pair.of("description", "2.0"));
    assertEquals(result.get(2), Pair.of("tags", "1.0"));
  }

  @Test
  public void testSearchFieldConfigurationWithNonExistentWildcard() {
    // Test wildcard pattern that doesn't match any fields
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "no-match",
                    FieldConfiguration.builder()
                        .searchFields(SearchFields.builder().add(List.of("nonexistent.*")).build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build(),
            SearchFieldConfig.builder()
                .fieldName("description")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build());

    Set<SearchFieldConfig> result = handler.applySearchFieldConfiguration(baseFields, "no-match");

    // Should keep base fields since no matches were found
    assertEquals(result.size(), 2);
    assertEquals(result, baseFields);
  }

  @Test
  public void testSearchFieldConfigurationPreservesBoostScores() {
    // Test that boost scores are preserved when fields are not explicitly configured
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "preserve-boost",
                    FieldConfiguration.builder()
                        .searchFields(SearchFields.builder().add(List.of("platform")).build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(10.0f)
                .isQueryByDefault(true)
                .build(),
            SearchFieldConfig.builder()
                .fieldName("platform")
                .boost(5.0f)
                .isQueryByDefault(false)
                .build());

    Set<SearchFieldConfig> result =
        handler.applySearchFieldConfiguration(baseFields, "preserve-boost");

    // Check that name field preserves its boost
    SearchFieldConfig nameField =
        result.stream().filter(f -> f.fieldName().equals("name")).findFirst().orElse(null);
    assertNotNull(nameField);
    assertEquals(nameField.boost(), 10.0f);

    // Check that platform field preserves its boost but gets queryByDefault=true
    SearchFieldConfig platformField =
        result.stream().filter(f -> f.fieldName().equals("platform")).findFirst().orElse(null);
    assertNotNull(platformField);
    assertEquals(platformField.boost(), 5.0f);
    assertTrue(platformField.isQueryByDefault());
  }

  @Test
  public void testHighlightFieldConfigurationWithReplace() {
    // Test highlight fields with replace mode
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "highlight-replace",
                    FieldConfiguration.builder()
                        .highlightFields(
                            HighlightFields.builder()
                                .replace(List.of("name", "description"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<String> baseFields = Set.of("name", "description", "platform", "owners");

    Set<String> result = handler.applyHighlightFieldConfiguration(baseFields, "highlight-replace");

    assertEquals(result.size(), 2);
    assertTrue(result.contains("name"));
    assertTrue(result.contains("description"));
    assertFalse(result.contains("platform"));
    assertFalse(result.contains("owners"));
  }

  @Test
  public void testHighlightFieldConfigurationDisabled() {
    // Test when highlighting is disabled
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "highlight-disabled",
                    FieldConfiguration.builder()
                        .highlightFields(HighlightFields.builder().enabled(false).build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    assertFalse(handler.isHighlightingEnabled("highlight-disabled"));
    assertTrue(handler.isHighlightingEnabled("non-existent")); // Default is enabled
  }

  @Test
  public void testAutocompleteFieldConfigurationWithReplace() {
    // Test autocomplete fields with replace mode
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "autocomplete-replace",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder().replace(List.of("name", "description")).build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    List<Pair<String, String>> baseFields =
        List.of(
            Pair.of("name", "10.0"),
            Pair.of("description", "5.0"),
            Pair.of("platform", "1.0"),
            Pair.of("owners", "1.0"));

    List<Pair<String, String>> result =
        handler.applyAutocompleteFieldConfiguration(baseFields, "autocomplete-replace");

    assertEquals(result.size(), 2);
    assertTrue(result.stream().anyMatch(p -> p.getLeft().equals("name")));
    assertTrue(result.stream().anyMatch(p -> p.getLeft().equals("description")));

    // Check boost scores are preserved
    result.stream()
        .filter(p -> p.getLeft().equals("name"))
        .findFirst()
        .ifPresent(p -> assertEquals(p.getRight(), "10.0"));
  }

  @Test
  public void testAutocompleteFieldConfigurationWithWildcard() {
    // Test autocomplete with wildcard patterns
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "autocomplete-wildcard",
                    FieldConfiguration.builder()
                        .searchFields(SearchFields.builder().add(List.of("schema.*")).build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    List<Pair<String, String>> baseFields =
        List.of(
            Pair.of("name", "10.0"),
            Pair.of("schema.fieldName", "5.0"),
            Pair.of("schema.fieldType", "5.0"),
            Pair.of("platform", "1.0"));

    List<Pair<String, String>> result =
        handler.applyAutocompleteFieldConfiguration(baseFields, "autocomplete-wildcard");

    assertEquals(result.size(), 4);
    assertTrue(result.stream().anyMatch(p -> p.getLeft().equals("schema.fieldName")));
    assertTrue(result.stream().anyMatch(p -> p.getLeft().equals("schema.fieldType")));
  }

  @Test
  public void testIsQuotedAndUnquote() {
    // Test quote detection and unquoting
    assertTrue(CustomizedQueryHandler.isQuoted("\"test query\""));
    assertTrue(CustomizedQueryHandler.isQuoted("'test query'"));
    assertTrue(CustomizedQueryHandler.isQuoted("test \"quoted\" query"));
    assertFalse(CustomizedQueryHandler.isQuoted("test query"));

    assertEquals(CustomizedQueryHandler.unquote("\"test query\""), "test query");
    assertEquals(CustomizedQueryHandler.unquote("'test query'"), "test query");
    assertEquals(CustomizedQueryHandler.unquote("test \"quoted\" query"), "test quoted query");
  }

  @Test
  public void testLookupQueryConfig() {
    // Test query configuration lookup with regex patterns
    QueryConfiguration config1 = QueryConfiguration.builder().queryRegex("^exact match$").build();
    QueryConfiguration config2 = QueryConfiguration.builder().queryRegex(".*wildcard.*").build();

    CustomSearchConfiguration searchConfig =
        CustomSearchConfiguration.builder().queryConfigurations(List.of(config1, config2)).build();

    CustomizedQueryHandler handler =
        CustomizedQueryHandler.builder(TEST_CONFIG, searchConfig).build();

    Optional<QueryConfiguration> result = handler.lookupQueryConfig("exact match");
    assertTrue(result.isPresent());
    assertEquals(result.get().getQueryRegex(), "^exact match$");

    result = handler.lookupQueryConfig("contains wildcard pattern");
    assertTrue(result.isPresent());
    assertEquals(result.get().getQueryRegex(), ".*wildcard.*");

    result = handler.lookupQueryConfig("no match");
    assertFalse(result.isPresent());
  }

  @Test
  public void testEmptyFieldConfigurationReturnsBaseFields() {
    // Test that empty field configuration returns base fields unchanged
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "empty",
                    FieldConfiguration.builder()
                        .searchFields(SearchFields.builder().build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build(),
            SearchFieldConfig.builder()
                .fieldName("description")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build());

    Set<SearchFieldConfig> result = handler.applySearchFieldConfiguration(baseFields, "empty");

    assertEquals(result, baseFields);
  }

  @Test
  public void testInvalidFieldConfigurationLogsError() {
    // Test invalid configuration (replace with add/remove)
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "invalid",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .replace(List.of("name"))
                                .add(List.of("description"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build());

    // Should return base fields due to invalid configuration
    Set<SearchFieldConfig> result = handler.applySearchFieldConfiguration(baseFields, "invalid");

    assertEquals(result, baseFields);
  }

  @Test
  public void testApplySearchFieldConfiguration_NullCustomSearchConfiguration() {
    // Test when customSearchConfiguration is null
    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, null).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build());

    Set<SearchFieldConfig> result = handler.applySearchFieldConfiguration(baseFields, "any-label");
    assertEquals(result, baseFields);
  }

  @Test
  public void testApplySearchFieldConfiguration_NullFieldConfigurations() {
    // Test when fieldConfigurations is null
    CustomSearchConfiguration config = CustomSearchConfiguration.builder().build();
    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build());

    Set<SearchFieldConfig> result = handler.applySearchFieldConfiguration(baseFields, "label");
    assertEquals(result, baseFields);
  }

  @Test
  public void testApplySearchFieldConfiguration_EmptyResultFieldsMap() {
    // Test when resultFieldsMap becomes empty after remove operations
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "remove-all",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder().remove(List.of("name", "description")).build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build(),
            SearchFieldConfig.builder()
                .fieldName("description")
                .boost(1.0f)
                .isQueryByDefault(true)
                .build());

    Set<SearchFieldConfig> result = handler.applySearchFieldConfiguration(baseFields, "remove-all");

    // Should return base fields when result would be empty
    assertEquals(result, baseFields);
  }

  @Test
  public void testApplyAutocompleteFieldConfiguration_FindBoostForField() {
    // Test findBoostForField scenarios
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "test-boost",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .add(List.of("name.delimited", "description.ngram", "newField"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    List<Pair<String, String>> baseFields =
        List.of(Pair.of("name", "10.0"), Pair.of("description", "5.0"), Pair.of("tags", "1.0"));

    List<Pair<String, String>> result =
        handler.applyAutocompleteFieldConfiguration(baseFields, "test-boost");

    // Should include base fields plus subfields that inherit boost from base
    assertTrue(
        result.stream()
            .anyMatch(p -> p.getLeft().equals("name.delimited") && p.getRight().equals("10.0")));
    assertTrue(
        result.stream()
            .anyMatch(p -> p.getLeft().equals("description.ngram") && p.getRight().equals("5.0")));
    // newField should not be added as it doesn't exist in base fields
    assertFalse(result.stream().anyMatch(p -> p.getLeft().equals("newField")));
  }

  @Test
  public void testApplyAutocompleteFieldConfiguration_NotFoundFields() {
    // Test when adding fields that don't exist
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "add-nonexistent",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .add(List.of("nonexistent1", "nonexistent2"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    List<Pair<String, String>> baseFields =
        List.of(Pair.of("name", "10.0"), Pair.of("description", "5.0"));

    List<Pair<String, String>> result =
        handler.applyAutocompleteFieldConfiguration(baseFields, "add-nonexistent");

    // Should return base fields when no valid fields can be added
    assertEquals(result.size(), 2);
    assertTrue(result.stream().anyMatch(p -> p.getLeft().equals("name")));
    assertTrue(result.stream().anyMatch(p -> p.getLeft().equals("description")));
  }

  @Test
  public void testApplyAutocompleteFieldConfiguration_EmptyReplacementFields() {
    // Test when replacement results in empty fields
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "replace-nonexistent",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .replace(List.of("nonexistent1", "nonexistent2"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    List<Pair<String, String>> baseFields =
        List.of(Pair.of("name", "10.0"), Pair.of("description", "5.0"));

    List<Pair<String, String>> result =
        handler.applyAutocompleteFieldConfiguration(baseFields, "replace-nonexistent");

    // Should fall back to base fields
    assertEquals(result, baseFields);
  }

  @Test
  public void testGetHighlightFieldConfiguration_WithWildcardPatterns() {
    // Test highlight field configuration with wildcard patterns
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "wildcard-highlight",
                    FieldConfiguration.builder()
                        .highlightFields(
                            HighlightFields.builder()
                                .remove(List.of("schema.*", "tags"))
                                .add(List.of("customField"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<String> baseFields =
        Set.of(
            "name",
            "description",
            "schema.fieldName",
            "schema.fieldType",
            "schema.dataType",
            "tags",
            "owners");

    HighlightConfigurationResult result =
        handler.getHighlightFieldConfiguration(baseFields, "wildcard-highlight");

    // Verify wildcard pattern expansion
    assertFalse(result.getFieldsToHighlight().contains("schema.fieldName"));
    assertFalse(result.getFieldsToHighlight().contains("schema.fieldType"));
    assertFalse(result.getFieldsToHighlight().contains("schema.dataType"));
    assertFalse(result.getFieldsToHighlight().contains("tags"));
    assertTrue(result.getFieldsToHighlight().contains("name"));
    assertTrue(result.getFieldsToHighlight().contains("customField"));

    // Check explicitly configured fields
    assertTrue(result.getExplicitlyConfiguredFields().contains("customField"));
  }

  @Test
  public void testApplyHighlightFieldConfiguration_AddRemoveMode() {
    // Test highlight configuration with add/remove mode
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "add-remove-highlight",
                    FieldConfiguration.builder()
                        .highlightFields(
                            HighlightFields.builder()
                                .remove(List.of("description", "tags"))
                                .add(List.of("newHighlight", "anotherHighlight"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<String> baseFields = new LinkedHashSet<>(List.of("name", "description", "tags", "owners"));

    Set<String> result =
        handler.applyHighlightFieldConfiguration(baseFields, "add-remove-highlight");

    // Verify remove operation
    assertFalse(result.contains("description"));
    assertFalse(result.contains("tags"));

    // Verify add operation
    assertTrue(result.contains("newHighlight"));
    assertTrue(result.contains("anotherHighlight"));

    // Verify remaining fields
    assertTrue(result.contains("name"));
    assertTrue(result.contains("owners"));
  }

  @Test
  public void testExpandFieldPatterns_MixedPatterns() {
    // Test expandFieldPatterns with various pattern types
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "pattern-test",
                    FieldConfiguration.builder()
                        .highlightFields(
                            HighlightFields.builder()
                                .remove(List.of("schema.*", "literal", "nonexistent.*", "missing"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<String> baseFields =
        Set.of("name", "literal", "schema.field1", "schema.field2", "other.field");

    HighlightConfigurationResult result =
        handler.getHighlightFieldConfiguration(baseFields, "pattern-test");

    // Verify pattern expansion
    assertFalse(result.getFieldsToHighlight().contains("schema.field1"));
    assertFalse(result.getFieldsToHighlight().contains("schema.field2"));
    assertFalse(result.getFieldsToHighlight().contains("literal"));
    assertTrue(result.getFieldsToHighlight().contains("name"));
    assertTrue(result.getFieldsToHighlight().contains("other.field"));
  }

  @Test
  public void testFindReferenceField_SubfieldMatching() {
    // Test subfield matching in createSearchFieldConfigsFromNames
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "subfield-test",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .add(List.of("name.keyword", "description.analyzed"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(10.0f)
                .isQueryByDefault(true)
                .build(),
            SearchFieldConfig.builder()
                .fieldName("description")
                .boost(5.0f)
                .isQueryByDefault(false)
                .build());

    Set<SearchFieldConfig> result =
        handler.applySearchFieldConfiguration(baseFields, "subfield-test");

    // Should find base fields for subfields
    assertEquals(result.size(), 2);

    // Verify that subfields inherit from base fields and get queryByDefault=true
    SearchFieldConfig nameField =
        result.stream().filter(f -> f.fieldName().equals("name")).findFirst().orElse(null);
    assertNotNull(nameField);
    assertEquals(nameField.boost(), 10.0f);
    assertTrue(nameField.isQueryByDefault());

    SearchFieldConfig descField =
        result.stream().filter(f -> f.fieldName().equals("description")).findFirst().orElse(null);
    assertNotNull(descField);
    assertEquals(descField.boost(), 5.0f);
    assertTrue(descField.isQueryByDefault()); // Should be set to true for explicitly added fields
  }

  @Test
  public void testAutocompleteConfiguration_ComplexBoostInheritance() {
    // Test complex boost inheritance scenarios
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "complex-boost",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .add(List.of("name.delimited", "tags.keyword", "newField.subfield"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    List<Pair<String, String>> baseFields =
        List.of(
            Pair.of("name", "10.0"),
            Pair.of("tags.raw", "5.0"), // Different subfield
            Pair.of("description", "3.0"));

    List<Pair<String, String>> result =
        handler.applyAutocompleteFieldConfiguration(baseFields, "complex-boost");

    // Should include existing fields plus successfully matched new fields
    assertTrue(result.size() >= 3);

    // name.delimited should inherit boost from name
    Optional<Pair<String, String>> nameDelimited =
        result.stream().filter(p -> p.getLeft().equals("name.delimited")).findFirst();
    assertTrue(nameDelimited.isPresent());
    assertEquals(nameDelimited.get().getRight(), "10.0");
  }

  @Test
  public void testHighlightConfiguration_PreservesInsertionOrder() {
    // Test that LinkedHashSet preserves insertion order
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "ordered-highlight",
                    FieldConfiguration.builder()
                        .highlightFields(
                            HighlightFields.builder()
                                .replace(List.of("field3", "field1", "field2"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<String> baseFields = Set.of("field1", "field2", "field3", "field4");

    Set<String> result = handler.applyHighlightFieldConfiguration(baseFields, "ordered-highlight");

    // Convert to list to check order
    List<String> resultList = new ArrayList<>(result);
    assertEquals(resultList.size(), 3);
    assertEquals(resultList.get(0), "field3");
    assertEquals(resultList.get(1), "field1");
    assertEquals(resultList.get(2), "field2");
  }

  @Test
  public void testDuplicateFieldHandling() {
    // Test that duplicates are properly handled in various configurations
    CustomSearchConfiguration config =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "duplicate-test",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .add(List.of("name", "name", "description"))
                                .build())
                        .build()))
            .build();

    CustomizedQueryHandler handler = CustomizedQueryHandler.builder(TEST_CONFIG, config).build();

    Set<SearchFieldConfig> baseFields =
        Set.of(
            SearchFieldConfig.builder()
                .fieldName("name")
                .boost(10.0f)
                .isQueryByDefault(false)
                .build(),
            SearchFieldConfig.builder()
                .fieldName("description")
                .boost(5.0f)
                .isQueryByDefault(true)
                .build());

    Set<SearchFieldConfig> result =
        handler.applySearchFieldConfiguration(baseFields, "duplicate-test");

    // Should only have 2 fields despite duplicate in add list
    assertEquals(result.size(), 2);

    // Verify no duplicates by checking field names
    Set<String> fieldNames =
        result.stream().map(SearchFieldConfig::fieldName).collect(Collectors.toSet());
    assertEquals(fieldNames.size(), 2);
    assertTrue(fieldNames.contains("name"));
    assertTrue(fieldNames.contains("description"));
  }
}
