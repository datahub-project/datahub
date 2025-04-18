package com.linkedin.metadata.search.query.request;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.AUTO_COMPLETE_ENTITY_TYPES;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.TEXT_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.URN_SEARCH_ANALYZER;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.Mockito;
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
    testQueryConfig = new SearchConfiguration();
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

    testQueryConfig.setExactMatch(exactMatchConfiguration);
    testQueryConfig.setWordGram(wordGramConfiguration);
    testQueryConfig.setPartial(partialConfiguration);
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
    Mockito.when(mockEntitySpec.getSearchableFieldSpecs())
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
                        Optional.empty(),
                        Optional.empty(),
                        13.0,
                        Optional.empty(),
                        Optional.empty(),
                        Map.of(),
                        List.of(),
                        false,
                        false,
                        Optional.empty()),
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
                        Optional.empty(),
                        Optional.empty(),
                        20.0,
                        Optional.empty(),
                        Optional.empty(),
                        Map.of(),
                        List.of(),
                        false,
                        false,
                        Optional.empty()),
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
                        Optional.empty(),
                        Optional.empty(),
                        3.0,
                        Optional.empty(),
                        Optional.empty(),
                        Map.of(),
                        List.of(),
                        false,
                        false,
                        Optional.empty()),
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
}
