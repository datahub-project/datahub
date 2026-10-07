package com.linkedin.metadata.search.query.request;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.AutocompleteConfiguration;
import com.linkedin.metadata.config.search.custom.BoolQueryConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.HighlightFields;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.lucene.search.function.FieldValueFactorFunction;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AutocompleteRequestHandlerTest {
  private static ElasticSearchConfiguration testQueryConfig;
  private AutocompleteRequestHandler handler;
  private OperationContext mockOpContext =
      TestOperationContexts.systemContextNoSearchAuthorization(mock(EntityRegistry.class));
  private OperationContext nonMockOpContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  static {
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

    testQueryConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .maxTermBucketSize(20)
                    .exactMatch(exactMatchConfiguration)
                    .wordGram(wordGramConfiguration)
                    .partial(partialConfiguration)
                    .build())
            .build();
  }

  private static final ElasticSearchConfiguration TEST_V3_QUERY_CONFIG =
      testQueryConfig.toBuilder()
          .entityIndex(
              V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.toBuilder()
                  .v3(
                      V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.getV3().toBuilder()
                          .keywordReadEnabled(true)
                          .build())
                  .build())
          .build();

  @BeforeClass
  public void beforeTest() {
    handler =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder().build(),
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);
  }

  private static final QueryConfiguration TEST_QUERY_CONFIG =
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
          .build();

  @Test
  public void testDefaultAutocompleteRequest() {
    // When field is null
    SearchRequest autocompleteRequest =
        handler.getSearchRequest(mockOpContext, null, "input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    assertEquals(sourceBuilder.size(), 10);
    BoolQueryBuilder wrapper =
        (BoolQueryBuilder) ((FunctionScoreQueryBuilder) sourceBuilder.query()).query();
    BoolQueryBuilder query = (BoolQueryBuilder) extractNestedQuery(wrapper);
    assertEquals(query.should().size(), 4);

    MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) query.should().get(0);
    assertEquals("keyPart1.keyword", matchQueryBuilder.fieldName());

    MultiMatchQueryBuilder autocompleteQuery = (MultiMatchQueryBuilder) query.should().get(3);
    Map<String, Float> queryFields = autocompleteQuery.fields();
    assertTrue(queryFields.containsKey("keyPart1.ngram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._2gram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._3gram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._4gram"));
    assertEquals(autocompleteQuery.type(), MultiMatchQueryBuilder.Type.BOOL_PREFIX);

    MatchPhrasePrefixQueryBuilder prefixQuery =
        (MatchPhrasePrefixQueryBuilder) query.should().get(1);
    assertEquals("keyPart1.delimited", prefixQuery.fieldName());

    assertEquals(wrapper.mustNot().size(), 1);
    TermQueryBuilder removedFilter = (TermQueryBuilder) wrapper.mustNot().get(0);
    assertEquals(removedFilter.fieldName(), "removed");
    assertEquals(removedFilter.value(), true);
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<HighlightBuilder.Field> highlightedFields = highlightBuilder.fields();
    assertEquals(highlightedFields.size(), 9);

    Set<String> expectedFields =
        Set.of(
            "keyPart1",
            "keyPart1.*",
            "keyPart1.ngram",
            "keyPart1.delimited",
            "keyPart1.keyword",
            "urn",
            "urn.*",
            "urn.ngram",
            "urn.delimited");

    Set<String> highlightFieldNames =
        highlightedFields.stream().map(HighlightBuilder.Field::name).collect(Collectors.toSet());

    assertEquals(expectedFields, highlightFieldNames);
  }

  @Test
  public void testAutocompleteRequestWithField() {
    // The field must be a valid field in the model. Pick from `keyPart1` or `urn`
    SearchRequest autocompleteRequest =
        handler.getSearchRequest(mockOpContext, null, "input", "keyPart1", null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    assertEquals(sourceBuilder.size(), 10);
    BoolQueryBuilder wrapper =
        (BoolQueryBuilder) ((FunctionScoreQueryBuilder) sourceBuilder.query()).query();
    assertEquals(wrapper.should().size(), 1);
    BoolQueryBuilder query = (BoolQueryBuilder) extractNestedQuery(wrapper);
    assertEquals(query.should().size(), 3);

    MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) query.should().get(0);
    assertEquals("keyPart1.keyword", matchQueryBuilder.fieldName());

    MultiMatchQueryBuilder autocompleteQuery = (MultiMatchQueryBuilder) query.should().get(2);
    Map<String, Float> queryFields = autocompleteQuery.fields();
    assertTrue(queryFields.containsKey("keyPart1.ngram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._2gram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._3gram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._4gram"));
    assertEquals(autocompleteQuery.type(), MultiMatchQueryBuilder.Type.BOOL_PREFIX);

    MatchPhrasePrefixQueryBuilder prefixQuery =
        (MatchPhrasePrefixQueryBuilder) query.should().get(1);
    assertEquals("keyPart1.delimited", prefixQuery.fieldName());

    TermQueryBuilder removedFilter = (TermQueryBuilder) wrapper.mustNot().get(0);
    assertEquals(removedFilter.fieldName(), "removed");
    assertEquals(removedFilter.value(), true);
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<HighlightBuilder.Field> highlightedFields = highlightBuilder.fields();
    assertEquals(highlightedFields.size(), 5);
    assertEquals(highlightedFields.get(0).name(), "keyPart1");
    assertEquals(highlightedFields.get(1).name(), "keyPart1.*");
    assertEquals(highlightedFields.get(2).name(), "keyPart1.ngram");
    assertEquals(highlightedFields.get(3).name(), "keyPart1.delimited");
    assertEquals(highlightedFields.get(4).name(), "keyPart1.keyword");
  }

  @Test
  public void testCustomConfigWithDefault() {
    // Exclude Default query
    AutocompleteRequestHandler withoutDefaultQuery =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(false)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
                                    .build())
                            .build()))
                .build(),
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    SearchRequest autocompleteRequest =
        withoutDefaultQuery.getSearchRequest(mockOpContext, null, "input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    FunctionScoreQueryBuilder wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    assertEquals(((BoolQueryBuilder) wrapper.query()).should().size(), 1);
    QueryBuilder customQuery = extractNestedQuery((BoolQueryBuilder) wrapper.query());
    assertEquals(customQuery, QueryBuilders.matchAllQuery());

    // Include Default query
    AutocompleteRequestHandler withDefaultQuery =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(true)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
                                    .build())
                            .build()))
                .build(),
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    autocompleteRequest =
        withDefaultQuery.getSearchRequest(mockOpContext, null, "input", null, null, 10);
    sourceBuilder = autocompleteRequest.source();
    wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    BoolQueryBuilder query =
        ((BoolQueryBuilder) ((BoolQueryBuilder) wrapper.query()).should().get(0));
    assertEquals(query.should().size(), 2);

    List<QueryBuilder> shouldQueries = query.should();

    // Default
    BoolQueryBuilder defaultQuery =
        (BoolQueryBuilder)
            shouldQueries.stream().filter(qb -> qb instanceof BoolQueryBuilder).findFirst().get();
    assertEquals(defaultQuery.should().size(), 4);

    // Custom
    customQuery =
        shouldQueries.stream().filter(qb -> qb instanceof MatchAllQueryBuilder).findFirst().get();
    assertEquals(customQuery, QueryBuilders.matchAllQuery());
  }

  @Test
  public void testCustomConfigWithInheritedQueryFunctionScores() {
    // Pickup scoring functions from non-autocomplete
    AutocompleteRequestHandler withInherit =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .queryConfigurations(List.of(TEST_QUERY_CONFIG))
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(false)
                            .inheritFunctionScore(true)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
                                    .build())
                            .build()))
                .build(),
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    SearchRequest autocompleteRequest =
        withInherit.getSearchRequest(mockOpContext, null, "input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    FunctionScoreQueryBuilder wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    assertEquals(((BoolQueryBuilder) wrapper.query()).should().size(), 1);

    QueryBuilder customQuery = extractNestedQuery(((BoolQueryBuilder) wrapper.query()));
    assertEquals(customQuery, QueryBuilders.matchAllQuery());

    FunctionScoreQueryBuilder.FilterFunctionBuilder[] expectedQueryConfigurationScoreFunctions = {
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.weightFactorFunction(1f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("materialized", true),
          ScoreFunctionBuilders.weightFactorFunction(0.5f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("deprecated", false),
          ScoreFunctionBuilders.weightFactorFunction(1.5f))
    };
    assertEquals(wrapper.filterFunctionBuilders(), expectedQueryConfigurationScoreFunctions);

    // no search query customization
    AutocompleteRequestHandler noQueryCustomization =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(false)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
                                    .build())
                            .build()))
                .build(),
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    autocompleteRequest =
        noQueryCustomization.getSearchRequest(mockOpContext, null, "input", null, null, 10);
    sourceBuilder = autocompleteRequest.source();
    wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    assertEquals(((BoolQueryBuilder) wrapper.query()).should().size(), 1);

    customQuery = extractNestedQuery((BoolQueryBuilder) wrapper.query());
    assertEquals(customQuery, QueryBuilders.matchAllQuery());

    // PDL annotation based on default behavior of query builder
    FunctionScoreQueryBuilder.FilterFunctionBuilder[] expectedDefaultScoreFunctions = {
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.weightFactorFunction(1f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.fieldValueFactorFunction("feature2")
              .modifier(FieldValueFactorFunction.Modifier.NONE)
              .missing(0.0)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.fieldValueFactorFunction("feature1")
              .modifier(FieldValueFactorFunction.Modifier.LOG1P)
              .missing(0.0))
    };
    assertEquals(wrapper.filterFunctionBuilders(), expectedDefaultScoreFunctions);
  }

  @Test
  public void testCustomConfigWithFunctionScores() {
    // Scoring functions explicit autocomplete override
    AutocompleteRequestHandler explicitNoInherit =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .queryConfigurations(List.of(TEST_QUERY_CONFIG)) // should be ignored
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(false)
                            .inheritFunctionScore(false)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
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
                                            1.5,
                                            "filter",
                                            Map.<String, Object>of(
                                                "term",
                                                Map.<String, Object>of(
                                                    "deprecated", Map.of("value", false)))))))
                            .build()))
                .build(),
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    SearchRequest autocompleteRequest =
        explicitNoInherit.getSearchRequest(mockOpContext, null, "input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    FunctionScoreQueryBuilder wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    assertEquals(((BoolQueryBuilder) wrapper.query()).should().size(), 1);

    QueryBuilder customQuery = extractNestedQuery((BoolQueryBuilder) wrapper.query());
    assertEquals(customQuery, QueryBuilders.matchAllQuery());

    FunctionScoreQueryBuilder.FilterFunctionBuilder[] expectedCustomScoreFunctions = {
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("deprecated", false),
          ScoreFunctionBuilders.weightFactorFunction(1.5f))
    };
    assertEquals(wrapper.filterFunctionBuilders(), expectedCustomScoreFunctions);

    // Pickup scoring functions explicit autocomplete override (even though default query and
    // inherit enabled)
    AutocompleteRequestHandler explicit =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .queryConfigurations(List.of(TEST_QUERY_CONFIG)) // should be ignored
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(true)
                            .inheritFunctionScore(true)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
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
                                            1.5,
                                            "filter",
                                            Map.<String, Object>of(
                                                "term",
                                                Map.<String, Object>of(
                                                    "deprecated", Map.of("value", false)))))))
                            .build()))
                .build(),
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    autocompleteRequest = explicit.getSearchRequest(mockOpContext, null, "input", null, null, 10);
    sourceBuilder = autocompleteRequest.source();
    wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    BoolQueryBuilder query =
        ((BoolQueryBuilder) ((BoolQueryBuilder) wrapper.query()).should().get(0));
    assertEquals(query.should().size(), 2);

    customQuery = query.should().get(0);
    assertEquals(customQuery, QueryBuilders.matchAllQuery());

    // standard query still present
    assertEquals(((BoolQueryBuilder) query.should().get(1)).should().size(), 4);

    // custom functions included
    assertEquals(wrapper.filterFunctionBuilders(), expectedCustomScoreFunctions);
  }

  @Test
  public void testFilterLatestVersions() {
    final Criterion filterCriterion =
        buildCriterion("platform", Condition.EQUAL, "mysql", "bigquery");

    final BoolQueryBuilder testQuery =
        getQuery(
            filterCriterion,
            nonMockOpContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME),
            true);

    List<QueryBuilder> isLatestQueries =
        testQuery.filter().stream()
            .filter(filter -> filter instanceof BoolQueryBuilder)
            .flatMap(filter -> ((BoolQueryBuilder) filter).must().stream())
            .filter(must -> must instanceof BoolQueryBuilder)
            .flatMap(must -> ((BoolQueryBuilder) must).should().stream())
            .filter(should -> should instanceof BoolQueryBuilder)
            .flatMap(
                should -> {
                  BoolQueryBuilder boolShould = (BoolQueryBuilder) should;

                  // Get isLatest: true term queries
                  Stream<QueryBuilder> filterQueries =
                      boolShould.filter().stream()
                          .filter(
                              f ->
                                  f instanceof TermQueryBuilder
                                      && ((TermQueryBuilder) f).fieldName().equals("isLatest"));

                  // Get isLatest exists queries
                  Stream<QueryBuilder> existsQueries =
                      boolShould.mustNot().stream()
                          .filter(mn -> mn instanceof BoolQueryBuilder)
                          .flatMap(mn -> ((BoolQueryBuilder) mn).must().stream())
                          .filter(
                              mq ->
                                  mq instanceof ExistsQueryBuilder
                                      && ((ExistsQueryBuilder) mq).fieldName().equals("isLatest"));

                  return Stream.concat(filterQueries, existsQueries);
                })
            .collect(Collectors.toList());

    assertTrue(isLatestQueries.size() == 2, "Expected to find two queries");
    final TermQueryBuilder termQueryBuilder = (TermQueryBuilder) isLatestQueries.get(0);
    assertEquals(termQueryBuilder.fieldName(), "isLatest");
    Set<Boolean> values = new HashSet<>();
    values.add((Boolean) termQueryBuilder.value());

    assertEquals(values.size(), 1, "Expected only true value.");
    assertTrue(values.contains(true));
    final ExistsQueryBuilder existsQueryBuilder = (ExistsQueryBuilder) isLatestQueries.get(1);
    assertEquals(existsQueryBuilder.fieldName(), "isLatest");
  }

  @Test
  public void testNoFilterLatestVersions() {
    final Criterion filterCriterion =
        buildCriterion("platform", Condition.EQUAL, "mysql", "bigquery");

    final BoolQueryBuilder testQuery =
        getQuery(
            filterCriterion,
            nonMockOpContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME),
            false);

    // bool -> filter -> [bool] -> must -> [bool]
    List<QueryBuilder> isLatestQueries =
        testQuery.filter().stream()
            .filter(filter -> filter instanceof BoolQueryBuilder)
            .flatMap(filter -> ((BoolQueryBuilder) filter).must().stream())
            .filter(must -> must instanceof BoolQueryBuilder)
            .flatMap(must -> ((BoolQueryBuilder) must).should().stream())
            .filter(should -> should instanceof BoolQueryBuilder)
            .flatMap(
                should -> {
                  BoolQueryBuilder boolShould = (BoolQueryBuilder) should;

                  // Get isLatest: true term queries
                  Stream<QueryBuilder> filterQueries =
                      boolShould.filter().stream()
                          .filter(
                              f ->
                                  f instanceof TermQueryBuilder
                                      && ((TermQueryBuilder) f).fieldName().equals("isLatest"));

                  // Get isLatest exists queries
                  Stream<QueryBuilder> existsQueries =
                      boolShould.mustNot().stream()
                          .filter(mn -> mn instanceof BoolQueryBuilder)
                          .flatMap(mn -> ((BoolQueryBuilder) mn).must().stream())
                          .filter(
                              mq ->
                                  mq instanceof ExistsQueryBuilder
                                      && ((ExistsQueryBuilder) mq).fieldName().equals("isLatest"));

                  return Stream.concat(filterQueries, existsQueries);
                })
            .collect(Collectors.toList());

    assertTrue(isLatestQueries.isEmpty(), "Expected to find no queries");
  }

  /**
   * Deliberate V3 change: autocomplete reads the shared _search.autocomplete field, which every
   * enableAutocomplete field copies into, and takes the suggestion from the fetched root values,
   * the name first, so it sets no highlighter.
   */
  @Test
  public void testV3AutocompleteQueriesSharedAutocompleteField() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                buildCriterion(
                                    "platform", Condition.EQUAL, "urn:li:dataPlatform:hive"),
                                buildCriterion("_entityType", Condition.EQUAL, "DATA_PRODUCT")))));
    SearchSourceBuilder v3 = getDatasetAutocompleteSource(TEST_V3_QUERY_CONFIG, filter);

    List<QueryBuilder> clauses = v3AutocompleteClauses(v3);
    assertEquals(clauses.size(), 3, clauses.toString());
    MultiMatchQueryBuilder boolPrefix = only(clauses, MultiMatchQueryBuilder.class);
    assertEquals(boolPrefix.type(), MultiMatchQueryBuilder.Type.BOOL_PREFIX);
    assertEquals(
        boolPrefix.fields().keySet(),
        Set.of(
            "_search.autocomplete.ngram",
            "_search.autocomplete.ngram._2gram",
            "_search.autocomplete.ngram._3gram",
            "_search.autocomplete.ngram._4gram"));
    assertEquals(
        only(clauses, MatchPhrasePrefixQueryBuilder.class).fieldName(),
        "_search.autocomplete.ngram");
    TermQueryBuilder exact = only(clauses, TermQueryBuilder.class);
    assertEquals(exact.fieldName(), "_search.autocomplete");
    assertEquals(exact.value(), "ord");
    assertNull(v3.highlighter());
    List<String> fetched = List.of(v3.fetchSource().includes());
    assertEquals(fetched.subList(0, 2), List.of("urn", "name"), fetched.toString());
    assertTrue(fetched.containsAll(List.of("qualifiedName", "id", "platform")), fetched.toString());

    // Filters read the .keyword subfield; V3 stores the registry entity name in _entityType
    String v3Query = v3.query().toString();
    assertTrue(v3Query.contains("platform.keyword"), v3Query);
    assertTrue(v3Query.contains("\"dataProduct\""), v3Query);
    assertFalse(v3Query.contains("DATA_PRODUCT"), v3Query);
  }

  /**
   * Deliberate V3 change: autocomplete on a requested field matches a prefix of that root field's
   * whole value, since root fields are not analyzed, and a field that holds no strings matches
   * nothing.
   */
  @Test
  public void testV3AutocompleteOnRequestedField() {
    AutocompleteRequestHandler v3Handler = getCachedDatasetHandler(TEST_V3_QUERY_CONFIG);

    SearchSourceBuilder named =
        v3Handler
            .getSearchRequest(nonMockOpContext, DATASET_ENTITY_NAME, "ord", "name", null, 10)
            .source();
    assertEquals(v3AutocompleteClauses(named), List.of(QueryBuilders.prefixQuery("name", "ord")));
    assertEquals(Set.of(named.fetchSource().includes()), Set.of("urn", "name"));

    SearchSourceBuilder notString =
        v3Handler
            .getSearchRequest(nonMockOpContext, DATASET_ENTITY_NAME, "ord", "removed", null, 10)
            .source();
    assertEquals(v3AutocompleteClauses(notString), List.of(new MatchNoneQueryBuilder()));
  }

  /**
   * Search V3 autocomplete reads the one shared field, so a field configuration that changes the
   * autocomplete fields does not apply to it.
   */
  @Test
  public void testV3AutocompleteKeepsTheSharedFieldUnderAFieldConfiguration() {
    CustomSearchConfiguration customConfig =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "names",
                    FieldConfiguration.builder()
                        .searchFields(SearchFields.builder().replace(List.of("name")).build())
                        .build()))
            .build();
    AutocompleteRequestHandler handler =
        new AutocompleteRequestHandler(
            nonMockOpContext,
            nonMockOpContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME),
            customConfig,
            QueryFilterRewriteChain.EMPTY,
            TEST_V3_QUERY_CONFIG,
            TEST_SEARCH_SERVICE_CONFIG);

    SearchSourceBuilder source =
        handler
            .getSearchRequest(
                nonMockOpContext.withSearchFlags(flags -> flags.setFieldConfiguration("names")),
                DATASET_ENTITY_NAME,
                "ord",
                null,
                null,
                10)
            .source();

    assertEquals(
        only(v3AutocompleteClauses(source), TermQueryBuilder.class).fieldName(),
        "_search.autocomplete");
  }

  /**
   * The V3 suggestion is the first fetched value, in suggestion order, holding a word the input
   * starts: the name when it matches, else the matching field, else the first value.
   */
  @Test
  public void testV3SuggestionPrefersTheMatchingName() {
    SearchHit[] hits = {
      suggestionHit("a", Map.of("name", "Orders Daily", "id", "db.orders_daily")),
      suggestionHit("b", Map.of("name", "Revenue", "id", "db.orders")),
      suggestionHit("c", Map.of("name", "Revenue", "id", "db.revenue")),
      // An empty array value has no first value to suggest
      suggestionHit("d", Map.of("name", List.of(), "id", "db.revenue"))
    };
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits())
        .thenReturn(new SearchHits(hits, new TotalHits(4L, TotalHits.Relation.EQUAL_TO), 1.0f));

    AutoCompleteResult result =
        getCachedDatasetHandler(TEST_V3_QUERY_CONFIG)
            .extractResult(nonMockOpContext, response, "ord");

    assertEquals(
        result.getSuggestions(), List.of("Orders Daily", "db.orders", "Revenue", "db.revenue"));
    assertEquals(result.getEntities().size(), 4);
  }

  private static SearchHit suggestionHit(String name, Map<String, Object> fields) {
    Map<String, Object> source = new HashMap<>(fields);
    source.put("urn", "urn:li:dataset:(urn:li:dataPlatform:hive," + name + ",PROD)");
    SearchHit hit = mock(SearchHit.class);
    when(hit.getSourceAsMap()).thenReturn(source);
    return hit;
  }

  /** The clauses of the V3 autocomplete query, under the default filters. */
  private static List<QueryBuilder> v3AutocompleteClauses(SearchSourceBuilder source) {
    BoolQueryBuilder nested =
        (BoolQueryBuilder) ((FunctionScoreQueryBuilder) source.query()).query();
    assertEquals(nested.should().size(), 1);
    return ((BoolQueryBuilder) nested.should().get(0)).should();
  }

  private static <T> T only(List<QueryBuilder> clauses, Class<T> type) {
    List<QueryBuilder> matching = clauses.stream().filter(type::isInstance).toList();
    assertEquals(matching.size(), 1, clauses.toString());
    return type.cast(matching.get(0));
  }

  /** A handler built for V2 first must not be reused when V3 keyword read is requested. */
  @Test
  public void testGetBuilderKeepsV2AndV3HandlersApart() {
    AutocompleteRequestHandler v2Handler = getCachedDatasetHandler(testQueryConfig);
    AutocompleteRequestHandler v3Handler = getCachedDatasetHandler(TEST_V3_QUERY_CONFIG);
    assertNotSame(v3Handler, v2Handler);
    assertSame(getCachedDatasetHandler(TEST_V3_QUERY_CONFIG), v3Handler);

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                buildCriterion("_entityType", Condition.EQUAL, "DATA_PRODUCT")))));
    String v3Query =
        v3Handler
            .getSearchRequest(nonMockOpContext, DATASET_ENTITY_NAME, "ord", null, filter, 10)
            .source()
            .query()
            .toString();
    String v2Query =
        v2Handler
            .getSearchRequest(nonMockOpContext, DATASET_ENTITY_NAME, "ord", null, filter, 10)
            .source()
            .query()
            .toString();
    // Only the V3 handler maps entity type values to registry names and scopes to its entity
    assertTrue(v3Query.contains("\"dataProduct\""), v3Query);
    assertTrue(v3Query.contains("\"dataset\""), v3Query);
    assertTrue(v2Query.contains("\"DATA_PRODUCT\""), v2Query);
    assertFalse(v2Query.contains("\"dataset\""), v2Query);
  }

  private AutocompleteRequestHandler getCachedDatasetHandler(
      ElasticSearchConfiguration searchConfiguration) {
    return AutocompleteRequestHandler.getBuilder(
        nonMockOpContext,
        nonMockOpContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME),
        CustomSearchConfiguration.builder().build(),
        QueryFilterRewriteChain.EMPTY,
        searchConfiguration,
        TEST_SEARCH_SERVICE_CONFIG);
  }

  private SearchSourceBuilder getDatasetAutocompleteSource(
      ElasticSearchConfiguration searchConfiguration, Filter filter) {
    return new AutocompleteRequestHandler(
            nonMockOpContext,
            nonMockOpContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME),
            CustomSearchConfiguration.builder().build(),
            QueryFilterRewriteChain.EMPTY,
            searchConfiguration,
            TEST_SEARCH_SERVICE_CONFIG)
        .getSearchRequest(nonMockOpContext, DATASET_ENTITY_NAME, "ord", null, filter, 10)
        .source();
  }

  private static QueryBuilder extractNestedQuery(BoolQueryBuilder nested) {
    assertEquals(nested.should().size(), 1);
    BoolQueryBuilder firstLevel = (BoolQueryBuilder) nested.should().get(0);
    assertEquals(firstLevel.should().size(), 1);
    return firstLevel.should().get(0);
  }

  private BoolQueryBuilder getQuery(
      final Criterion filterCriterion, final EntitySpec entitySpec, boolean filterNonLatest) {
    final Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(ImmutableList.of(filterCriterion)))));

    AutocompleteRequestHandler requestHandler =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            entitySpec,
            CustomSearchConfiguration.builder().build(),
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    return (BoolQueryBuilder)
        ((FunctionScoreQueryBuilder)
                requestHandler
                    .getSearchRequest(
                        mockOpContext.withSearchFlags(
                            flags ->
                                flags
                                    .setFulltext(false)
                                    .setFilterNonLatestVersions(filterNonLatest)),
                        null,
                        "",
                        "platform",
                        filter,
                        3)
                    .source()
                    .query())
            .query();
  }

  @Test
  public void testApplyResultLimitInAutocompleteRequest() {
    // Create SearchConfiguration with specific limits
    SearchServiceConfiguration testLimitConfig =
        TEST_SEARCH_SERVICE_CONFIG.toBuilder()
            .limit(
                new LimitConfig()
                    .setResults(
                        new ResultsLimitConfig().setMax(50).setApiDefault(50).setStrict(false)))
            .build();

    // Create a handler with our test configuration
    AutocompleteRequestHandler limitHandler =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder().build(),
            QueryFilterRewriteChain.EMPTY,
            TEST_OS_SEARCH_CONFIG,
            testLimitConfig);

    // Test with count below limit
    int requestedCount = 30;
    SearchRequest autocompleteRequest =
        limitHandler.getSearchRequest(mockOpContext, null, "input", null, null, requestedCount);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();

    // Verify the requested count was used (not limited)
    assertEquals(sourceBuilder.size(), requestedCount);

    // Test with count above limit (non-strict)
    requestedCount = 100;
    autocompleteRequest =
        limitHandler.getSearchRequest(mockOpContext, null, "input", null, null, requestedCount);
    sourceBuilder = autocompleteRequest.source();

    // Verify the max limit was applied
    assertEquals(sourceBuilder.size(), 50);
  }

  @Test
  public void testApplyResultLimitWithStrictConfiguration() {
    // Create SearchConfiguration with strict limits
    SearchServiceConfiguration strictConfig =
        TEST_SEARCH_SERVICE_CONFIG.toBuilder()
            .limit(
                LimitConfig.builder()
                    .results(new ResultsLimitConfig().setMax(50).setApiDefault(50).setStrict(true))
                    .build())
            .build();

    // Create a handler with our strict configuration
    AutocompleteRequestHandler strictHandler =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder().build(),
            QueryFilterRewriteChain.EMPTY,
            TEST_OS_SEARCH_CONFIG,
            strictConfig);

    // Test with count at the limit
    int requestedCount = 50;
    SearchRequest autocompleteRequest =
        strictHandler.getSearchRequest(mockOpContext, null, "input", null, null, requestedCount);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();

    // Verify exact limit was used
    assertEquals(sourceBuilder.size(), 50);

    // Test with count exceeding the limit in strict mode
    // This should throw an IllegalArgumentException
    try {
      requestedCount = 100;
      strictHandler.getSearchRequest(mockOpContext, null, "input", null, null, requestedCount);
      Assert.fail(
          "Should throw IllegalArgumentException when count exceeds limit with strict config");
    } catch (IllegalArgumentException e) {
      // Expected exception
      assertTrue(e.getMessage().contains("Result count exceeds limit of 50"));
    }
  }

  @Test
  public void testAutocompleteWithFieldConfiguration() {
    // Create custom configuration with field configurations
    CustomSearchConfiguration customConfig =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "minimal",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder().replace(List.of("keyPart1", "urn")).build())
                        .build()))
            .build();

    AutocompleteRequestHandler configHandler =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            customConfig,
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    // Create operation context with field configuration
    OperationContext opContextWithConfig = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    SearchFlags searchFlags = new SearchFlags().setFieldConfiguration("minimal");

    when(opContextWithConfig.getEntityRegistry()).thenReturn(mockOpContext.getEntityRegistry());
    when(opContextWithConfig.getObjectMapper()).thenReturn(mockOpContext.getObjectMapper());
    when(opContextWithConfig.getSearchContext()).thenReturn(searchContext);
    when(opContextWithConfig.getAspectRetriever()).thenReturn(mockOpContext.getAspectRetriever());
    when(opContextWithConfig.getOperationContextConfig())
        .thenReturn(nonMockOpContext.getOperationContextConfig());
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    SearchRequest autocompleteRequest =
        configHandler.getSearchRequest(opContextWithConfig, null, "input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();

    BoolQueryBuilder wrapper =
        (BoolQueryBuilder) ((FunctionScoreQueryBuilder) sourceBuilder.query()).query();
    BoolQueryBuilder query = (BoolQueryBuilder) extractNestedQuery(wrapper);

    // Should only have keyPart1 fields (urn doesn't have autocomplete fields in the test spec)
    boolean hasKeyPart1 = false;
    boolean hasOtherFields = false;

    for (QueryBuilder shouldQuery : query.should()) {
      if (shouldQuery instanceof MatchQueryBuilder) {
        MatchQueryBuilder match = (MatchQueryBuilder) shouldQuery;
        if (match.fieldName().startsWith("keyPart1")) {
          hasKeyPart1 = true;
        } else if (!match.fieldName().startsWith("urn")) {
          hasOtherFields = true;
        }
      } else if (shouldQuery instanceof MultiMatchQueryBuilder) {
        MultiMatchQueryBuilder multiMatch = (MultiMatchQueryBuilder) shouldQuery;
        for (String field : multiMatch.fields().keySet()) {
          if (field.startsWith("keyPart1")) {
            hasKeyPart1 = true;
          } else if (!field.startsWith("urn")) {
            hasOtherFields = true;
          }
        }
      }
    }

    assertTrue(hasKeyPart1, "Should have keyPart1 fields");
    assertFalse(hasOtherFields, "Should not have other fields");
  }

  @Test
  public void testAutocompleteFieldConfigurationWithAddRemove() {
    CustomSearchConfiguration customConfig =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "custom",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .add(List.of("textFieldOverride"))
                                .remove(List.of("keyPart1"))
                                .build())
                        .build()))
            .build();

    AutocompleteRequestHandler configHandler =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            customConfig,
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    OperationContext opContextWithConfig = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    SearchFlags searchFlags = new SearchFlags().setFieldConfiguration("custom");

    when(opContextWithConfig.getEntityRegistry()).thenReturn(mockOpContext.getEntityRegistry());
    when(opContextWithConfig.getObjectMapper()).thenReturn(mockOpContext.getObjectMapper());
    when(opContextWithConfig.getSearchContext()).thenReturn(searchContext);
    when(opContextWithConfig.getAspectRetriever()).thenReturn(mockOpContext.getAspectRetriever());
    when(opContextWithConfig.getOperationContextConfig())
        .thenReturn(nonMockOpContext.getOperationContextConfig());
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    // Note: textFieldOverride is not in the default autocomplete fields, so it won't be added
    // This test verifies that remove works correctly
    SearchRequest autocompleteRequest =
        configHandler.getSearchRequest(opContextWithConfig, null, "input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();

    BoolQueryBuilder wrapper =
        (BoolQueryBuilder) ((FunctionScoreQueryBuilder) sourceBuilder.query()).query();
    BoolQueryBuilder query = (BoolQueryBuilder) extractNestedQuery(wrapper);

    // Verify keyPart1 was removed
    for (QueryBuilder shouldQuery : query.should()) {
      if (shouldQuery instanceof MatchQueryBuilder) {
        MatchQueryBuilder match = (MatchQueryBuilder) shouldQuery;
        assertFalse(
            match.fieldName().startsWith("keyPart1"),
            "keyPart1 should be removed from autocomplete fields");
      } else if (shouldQuery instanceof MultiMatchQueryBuilder) {
        MultiMatchQueryBuilder multiMatch = (MultiMatchQueryBuilder) shouldQuery;
        for (String field : multiMatch.fields().keySet()) {
          assertFalse(
              field.startsWith("keyPart1"), "keyPart1 should be removed from autocomplete fields");
        }
      }
    }
  }

  @Test
  public void testAutocompleteHighlightFieldConfiguration() {
    CustomSearchConfiguration customConfig =
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
                                    .replace(List.of("keyPart1", "keyPart1.ngram"))
                                    .build())
                            .build()))
            .build();

    AutocompleteRequestHandler configHandler =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            customConfig,
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    // Test disabled highlighting
    OperationContext opContextNoHighlight = mock(OperationContext.class);
    SearchContext searchContextNoHighlight = mock(SearchContext.class);
    SearchFlags searchFlagsNoHighlight = new SearchFlags().setFieldConfiguration("no-highlight");

    when(opContextNoHighlight.getEntityRegistry()).thenReturn(mockOpContext.getEntityRegistry());
    when(opContextNoHighlight.getObjectMapper()).thenReturn(mockOpContext.getObjectMapper());
    when(opContextNoHighlight.getSearchContext()).thenReturn(searchContextNoHighlight);
    when(opContextNoHighlight.getAspectRetriever()).thenReturn(mockOpContext.getAspectRetriever());
    when(opContextNoHighlight.getOperationContextConfig())
        .thenReturn(nonMockOpContext.getOperationContextConfig());
    when(searchContextNoHighlight.getSearchFlags()).thenReturn(searchFlagsNoHighlight);

    SearchRequest noHighlightRequest =
        configHandler.getSearchRequest(opContextNoHighlight, null, "input", null, null, 10);
    assertNull(noHighlightRequest.source().highlighter(), "Highlighting should be disabled");

    // Test custom highlighting
    OperationContext opContextCustomHighlight = mock(OperationContext.class);
    SearchContext searchContextCustom = mock(SearchContext.class);
    SearchFlags searchFlagsCustom = new SearchFlags().setFieldConfiguration("custom-highlight");

    when(opContextCustomHighlight.getEntityRegistry())
        .thenReturn(mockOpContext.getEntityRegistry());
    when(opContextCustomHighlight.getObjectMapper()).thenReturn(mockOpContext.getObjectMapper());
    when(opContextCustomHighlight.getSearchContext()).thenReturn(searchContextCustom);
    when(opContextCustomHighlight.getAspectRetriever())
        .thenReturn(mockOpContext.getAspectRetriever());
    when(opContextCustomHighlight.getOperationContextConfig())
        .thenReturn(nonMockOpContext.getOperationContextConfig());
    when(searchContextCustom.getSearchFlags()).thenReturn(searchFlagsCustom);

    SearchRequest customHighlightRequest =
        configHandler.getSearchRequest(opContextCustomHighlight, null, "input", null, null, 10);
    HighlightBuilder highlightBuilder = customHighlightRequest.source().highlighter();
    assertNotNull(highlightBuilder);

    List<String> highlightFieldNames =
        highlightBuilder.fields().stream()
            .map(HighlightBuilder.Field::name)
            .collect(Collectors.toList());

    assertEquals(highlightFieldNames.size(), 2);
    assertTrue(highlightFieldNames.contains("keyPart1"));
    assertTrue(highlightFieldNames.contains("keyPart1.ngram"));
  }

  @Test
  public void testAutocompleteFieldConfigurationWithSpecificField() {
    CustomSearchConfiguration customConfig =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "minimal",
                    FieldConfiguration.builder()
                        .searchFields(SearchFields.builder().replace(List.of("keyPart1")).build())
                        .build()))
            .build();

    AutocompleteRequestHandler configHandler =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            customConfig,
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    OperationContext opContextWithConfig = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    SearchFlags searchFlags = new SearchFlags().setFieldConfiguration("minimal");

    when(opContextWithConfig.getEntityRegistry()).thenReturn(mockOpContext.getEntityRegistry());
    when(opContextWithConfig.getObjectMapper()).thenReturn(mockOpContext.getObjectMapper());
    when(opContextWithConfig.getSearchContext()).thenReturn(searchContext);
    when(opContextWithConfig.getAspectRetriever()).thenReturn(mockOpContext.getAspectRetriever());
    when(opContextWithConfig.getOperationContextConfig())
        .thenReturn(nonMockOpContext.getOperationContextConfig());
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    // When a specific field is provided, field configuration should still be applied
    // but only to that specific field if it's in the configuration
    SearchRequest autocompleteRequest =
        configHandler.getSearchRequest(opContextWithConfig, null, "input", "keyPart1", null, 10);

    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    BoolQueryBuilder wrapper =
        (BoolQueryBuilder) ((FunctionScoreQueryBuilder) sourceBuilder.query()).query();
    BoolQueryBuilder query = (BoolQueryBuilder) extractNestedQuery(wrapper);

    // Should have keyPart1 queries since it's in the replace list
    assertTrue(query.should().size() > 0, "Should have queries for keyPart1");
  }

  @Test
  public void testAutocompleteFieldConfigurationNullSafety() {
    // Test with null configuration
    AutocompleteRequestHandler nullConfigHandler =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            null,
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    OperationContext opContextNullConfig = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    SearchFlags searchFlags = new SearchFlags().setFieldConfiguration("any-label");

    when(opContextNullConfig.getEntityRegistry()).thenReturn(mockOpContext.getEntityRegistry());
    when(opContextNullConfig.getObjectMapper()).thenReturn(mockOpContext.getObjectMapper());
    when(opContextNullConfig.getSearchContext()).thenReturn(searchContext);
    when(opContextNullConfig.getAspectRetriever()).thenReturn(mockOpContext.getAspectRetriever());
    when(opContextNullConfig.getOperationContextConfig())
        .thenReturn(nonMockOpContext.getOperationContextConfig());
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    // Should not throw and should use default behavior
    SearchRequest request =
        nullConfigHandler.getSearchRequest(opContextNullConfig, null, "input", null, null, 10);
    assertNotNull(request);

    // Test with null config
    when(searchFlags.getFieldConfiguration()).thenReturn(null);
    SearchRequest requestNullFlags =
        nullConfigHandler.getSearchRequest(opContextNullConfig, null, "input", null, null, 10);
    assertNotNull(requestNullFlags);
  }

  @Test
  public void testAutocompleteBoostPreservation() {
    // Create mock entity spec with specific boost values
    EntitySpec mockSpec = mock(EntitySpec.class);
    SearchableFieldSpec fieldSpec1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec fieldSpec2 = mock(SearchableFieldSpec.class);

    SearchableAnnotation annotation1 = mock(SearchableAnnotation.class);
    when(annotation1.getFieldName()).thenReturn("field1");
    when(annotation1.getBoostScore()).thenReturn(3.0);
    when(annotation1.isEnableAutocomplete()).thenReturn(true);
    when(fieldSpec1.getSearchableAnnotation()).thenReturn(annotation1);

    SearchableAnnotation annotation2 = mock(SearchableAnnotation.class);
    when(annotation2.getFieldName()).thenReturn("field2");
    when(annotation2.getBoostScore()).thenReturn(1.5);
    when(annotation2.isEnableAutocomplete()).thenReturn(true);
    when(fieldSpec2.getSearchableAnnotation()).thenReturn(annotation2);

    when(mockSpec.getSearchableFieldSpecs()).thenReturn(List.of(fieldSpec1, fieldSpec2));

    CustomSearchConfiguration customConfig =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "reorder",
                    FieldConfiguration.builder()
                        .searchFields(
                            SearchFields.builder()
                                .replace(List.of("field2", "field1")) // Different order
                                .build())
                        .build()))
            .build();

    // Verify boost scores are preserved through CustomizedQueryHandler
    CustomizedQueryHandler handler =
        CustomizedQueryHandler.builder(new CustomConfiguration(), customConfig).build();
    List<Pair<String, String>> baseFields =
        List.of(Pair.of("field1", "3.0"), Pair.of("field2", "1.5"), Pair.of("urn", "1.0"));

    List<Pair<String, String>> configuredFields =
        handler.applyAutocompleteFieldConfiguration(baseFields, "reorder");

    // Should be sorted by boost (descending)
    assertEquals(configuredFields.size(), 2);
    assertEquals(configuredFields.get(0), Pair.of("field1", "3.0"));
    assertEquals(configuredFields.get(1), Pair.of("field2", "1.5"));
  }

  @Test
  public void testAutocompleteWithInvalidFieldConfiguration() {
    CustomSearchConfiguration customConfig =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "valid",
                    FieldConfiguration.builder()
                        .searchFields(SearchFields.builder().replace(List.of("keyPart1")).build())
                        .build()))
            .build();

    AutocompleteRequestHandler configHandler =
        AutocompleteRequestHandler.getBuilder(
            mockOpContext,
            TestEntitySpecBuilder.getSpec(),
            customConfig,
            QueryFilterRewriteChain.EMPTY,
            testQueryConfig,
            TEST_SEARCH_SERVICE_CONFIG);

    OperationContext opContextInvalidConfig = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    SearchFlags searchFlags = new SearchFlags().setFieldConfiguration("nonexistent");

    when(opContextInvalidConfig.getEntityRegistry()).thenReturn(mockOpContext.getEntityRegistry());
    when(opContextInvalidConfig.getObjectMapper()).thenReturn(mockOpContext.getObjectMapper());
    when(opContextInvalidConfig.getSearchContext()).thenReturn(searchContext);
    when(opContextInvalidConfig.getAspectRetriever())
        .thenReturn(mockOpContext.getAspectRetriever());
    when(opContextInvalidConfig.getOperationContextConfig())
        .thenReturn(nonMockOpContext.getOperationContextConfig());
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    // Should use default fields when configuration label doesn't exist
    SearchRequest request =
        configHandler.getSearchRequest(opContextInvalidConfig, null, "input", null, null, 10);

    SearchSourceBuilder sourceBuilder = request.source();
    BoolQueryBuilder wrapper =
        (BoolQueryBuilder) ((FunctionScoreQueryBuilder) sourceBuilder.query()).query();
    BoolQueryBuilder query = (BoolQueryBuilder) extractNestedQuery(wrapper);

    // Should have default fields
    boolean hasKeyPart1 =
        query.should().stream()
            .anyMatch(
                q -> {
                  if (q instanceof MatchQueryBuilder) {
                    return ((MatchQueryBuilder) q).fieldName().startsWith("keyPart1");
                  } else if (q instanceof MultiMatchQueryBuilder) {
                    return ((MultiMatchQueryBuilder) q)
                        .fields().keySet().stream().anyMatch(f -> f.startsWith("keyPart1"));
                  }
                  return false;
                });

    assertTrue(hasKeyPart1, "Should have default keyPart1 field when config doesn't exist");
  }
}
