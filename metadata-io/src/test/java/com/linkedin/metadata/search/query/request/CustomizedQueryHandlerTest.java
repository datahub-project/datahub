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
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import java.io.IOException;
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
}
