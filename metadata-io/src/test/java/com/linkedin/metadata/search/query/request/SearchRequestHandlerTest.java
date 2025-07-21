package com.linkedin.metadata.search.query.request;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildExistsCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNullCriterion;
import static com.linkedin.metadata.utils.SearchUtil.*;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

@Import(SearchCommonTestConfiguration.class)
public class SearchRequestHandlerTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Qualifier("queryOperationContext")
  private OperationContext operationContext;

  public static ElasticSearchConfiguration testQueryConfig;
  public static List<String> validHighlightingFields = List.of("urn", "foreignKey");
  public static StringArray customHighlightFields =
      new StringArray(
          List.of(
              validHighlightingFields.get(0),
              validHighlightingFields.get(1),
              "notExistingField",
              ""));

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
        TEST_ES_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_ES_SEARCH_CONFIG.getSearch().toBuilder()
                    .maxTermBucketSize(20)
                    .exactMatch(exactMatchConfiguration)
                    .wordGram(wordGramConfiguration)
                    .partial(partialConfiguration)
                    .build())
            .build();
  }

  @Test
  public void testDatasetFieldsAndHighlights() {
    EntitySpec entitySpec = operationContext.getEntityRegistry().getEntitySpec("dataset");
    SearchRequestHandler datasetHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            entitySpec,
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    /*
      Ensure efficient query performance, we do not expect upstream/downstream/fineGrained lineage
    */
    List<String> highlightFields =
        datasetHandler.getDefaultHighlights(operationContext).fields().stream()
            .map(HighlightBuilder.Field::name)
            .collect(Collectors.toList());
    assertTrue(
        highlightFields.stream()
            .noneMatch(
                fieldName -> fieldName.contains("upstream") || fieldName.contains("downstream")),
        "unexpected lineage fields in highlights: " + highlightFields);
  }

  @Test
  public void testCustomHighlights() {
    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            testQueryConfig,
            null,
            mock(QueryFilterRewriteChain.class),
            TEST_SEARCH_SERVICE_CONFIG);
    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            operationContext.withSearchFlags(
                flags ->
                    flags.setFulltext(false).setCustomHighlightingFields(customHighlightFields)),
            "testQuery",
            null,
            null,
            0,
            10,
            List.of());
    SearchSourceBuilder sourceBuilder = searchRequest.source();
    assertNotNull(sourceBuilder.highlighter());
    assertEquals(sourceBuilder.highlighter().fields().size(), 8);
    assertTrue(
        sourceBuilder.highlighter().fields().stream()
            .map(HighlightBuilder.Field::name)
            .toList()
            .containsAll(validHighlightingFields));
  }

  @Test
  public void testSearchRequestHandlerHighlightingTurnedOff() {
    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            operationContext.withSearchFlags(
                flags -> flags.setFulltext(false).setSkipHighlighting(true)),
            "testQuery",
            null,
            null,
            0,
            10,
            List.of());
    SearchSourceBuilder sourceBuilder = searchRequest.source();
    assertEquals(sourceBuilder.from(), 0);
    assertEquals(sourceBuilder.size(), 10);
    // Filters
    Collection<AggregationBuilder> aggBuilders =
        sourceBuilder.aggregations().getAggregatorFactories();
    // Expect 3 aggregations: textFieldOverride, missing␝textFieldOverride, and _entityType,
    // _entityType␝typeNames
    assertEquals(aggBuilders.size(), 4);
    for (AggregationBuilder aggBuilder : aggBuilders) {
      if (aggBuilder.getName().startsWith("textFieldOverride")) {
        TermsAggregationBuilder filterPanelBuilder = (TermsAggregationBuilder) aggBuilder;
        assertEquals(filterPanelBuilder.field(), "textFieldOverride.keyword");
      } else if (!aggBuilder.getName().startsWith("_entityType")
          && !aggBuilder
              .getName()
              .equals(
                  MISSING_SPECIAL_TYPE
                      + AGGREGATION_SPECIAL_TYPE_DELIMITER
                      + "textFieldOverride")) {
        fail("Found unexpected aggregation: " + aggBuilder.getName());
      }
    }
    // Highlights should not be present
    assertNull(sourceBuilder.highlighter());
  }

  @Test
  public void testSearchRequestHandler() {
    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            operationContext.withSearchFlags(
                flags ->
                    flags.setFulltext(false).setSkipHighlighting(false).setSkipAggregates(false)),
            "testQuery",
            null,
            null,
            0,
            10,
            List.of());
    SearchSourceBuilder sourceBuilder = searchRequest.source();
    assertEquals(sourceBuilder.from(), 0);
    assertEquals(sourceBuilder.size(), 10);
    // Filters
    Collection<AggregationBuilder> aggBuilders =
        sourceBuilder.aggregations().getAggregatorFactories();
    // Expect 4 aggregations: textFieldOverride, missing:textFieldOverride, _entityType and
    // _entityType:typeNames
    assertEquals(aggBuilders.size(), 4);
    for (AggregationBuilder aggBuilder : aggBuilders) {
      if (aggBuilder.getName().startsWith("textFieldOverride")) {
        TermsAggregationBuilder filterPanelBuilder = (TermsAggregationBuilder) aggBuilder;
        assertEquals(filterPanelBuilder.field(), "textFieldOverride.keyword");
      } else if (!aggBuilder.getName().startsWith("_entityType")
          && !aggBuilder
              .getName()
              .equals(
                  MISSING_SPECIAL_TYPE
                      + AGGREGATION_SPECIAL_TYPE_DELIMITER
                      + "textFieldOverride")) {
        fail("Found unexpected aggregation: " + aggBuilder.getName());
      }
    }
    // Highlights
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<String> fields =
        highlightBuilder.fields().stream()
            .map(HighlightBuilder.Field::name)
            .collect(Collectors.toList());
    assertEquals(fields.size(), 32);
    List<String> highlightableFields =
        ImmutableList.of(
            "keyPart1",
            "textArrayField",
            "textFieldOverride",
            "foreignKey",
            "nestedForeignKey",
            "nestedArrayStringField",
            "nestedArrayArrayField",
            "customProperties",
            "esObjectField",
            "wordGramField",
            "esObjectFieldLong",
            "esObjectFieldBoolean",
            "esObjectFieldFloat",
            "esObjectFieldDouble",
            "esObjectFieldInteger");
    highlightableFields.forEach(
        field -> {
          assertTrue(fields.contains(field), "Missing: " + field);
          assertTrue(fields.contains(field + ".*"), "Missing: " + field + ".*");
        });
  }

  @Test
  public void testAggregationsInSearch() {
    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    final String nestedAggString =
        String.format("_entityType%stextFieldOverride", AGGREGATION_SEPARATOR_CHAR);
    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            operationContext.withSearchFlags(
                flags -> flags.setFulltext(true).setIncludeDefaultFacets(false)),
            "*",
            null,
            null,
            0,
            10,
            List.of(
                "textFieldOverride",
                "_entityType",
                nestedAggString,
                AGGREGATION_SEPARATOR_CHAR,
                "not_a_facet"));
    SearchSourceBuilder sourceBuilder = searchRequest.source();
    // Filters
    Collection<AggregationBuilder> aggregationBuilders =
        sourceBuilder.aggregations().getAggregatorFactories();
    assertEquals(aggregationBuilders.size(), 3);

    // Expected aggregations
    AggregationBuilder expectedTextFieldAggregationBuilder =
        AggregationBuilders.terms("textFieldOverride")
            .field("textFieldOverride.keyword")
            .size(testQueryConfig.getSearch().getMaxTermBucketSize());
    AggregationBuilder expectedEntityTypeAggregationBuilder =
        AggregationBuilders.terms("_entityType")
            .field(ES_INDEX_FIELD)
            .size(testQueryConfig.getSearch().getMaxTermBucketSize())
            .minDocCount(0);
    AggregationBuilder expectedNestedAggregationBuilder =
        AggregationBuilders.terms(nestedAggString)
            .field(ES_INDEX_FIELD)
            .size(testQueryConfig.getSearch().getMaxTermBucketSize())
            .minDocCount(0)
            .subAggregation(
                AggregationBuilders.terms(nestedAggString)
                    .field("textFieldOverride.keyword")
                    .size(testQueryConfig.getSearch().getMaxTermBucketSize()));

    for (AggregationBuilder builder : aggregationBuilders) {
      if (builder.getName().equals("textFieldOverride")
          || builder.getName().equals("_entityType")) {
        assertTrue(builder.getSubAggregations().isEmpty());
        if (builder.getName().equalsIgnoreCase("textFieldOverride")) {
          assertEquals(builder, expectedTextFieldAggregationBuilder);
        } else {
          assertEquals(builder, expectedEntityTypeAggregationBuilder);
        }
      } else if (builder.getName().equals(nestedAggString)) {
        assertEquals(builder.getSubAggregations().size(), 1);
        Optional<AggregationBuilder> subAgg = builder.getSubAggregations().stream().findFirst();
        assertTrue(subAgg.isPresent());
        assertEquals(subAgg.get().getName(), nestedAggString);
        assertEquals(builder, expectedNestedAggregationBuilder);
      } else {
        fail("Found unexpected aggregation builder: " + builder.getName());
      }
    }
  }

  @Test
  public void testFilteredSearch() {

    final SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    final BoolQueryBuilder testQuery = constructFilterQuery(requestHandler, false);

    testFilterQuery(testQuery);

    final BoolQueryBuilder queryWithRemoved = constructRemovedQuery(requestHandler, false);

    testRemovedQuery(queryWithRemoved);

    final BoolQueryBuilder testQueryScroll = constructFilterQuery(requestHandler, true);

    testFilterQuery(testQueryScroll);

    final BoolQueryBuilder queryWithRemovedScroll = constructRemovedQuery(requestHandler, true);

    testRemovedQuery(queryWithRemovedScroll);
  }

  private BoolQueryBuilder constructFilterQuery(
      SearchRequestHandler requestHandler, boolean scroll) {
    final Criterion filterCriterion = buildCriterion("keyword", Condition.EQUAL, "some value");

    final Filter filterWithoutRemovedCondition =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(ImmutableList.of(filterCriterion)))));

    final BoolQueryBuilder testQuery;
    if (scroll) {
      testQuery =
          (BoolQueryBuilder)
              requestHandler
                  .getSearchRequest(
                      operationContext.withSearchFlags(flags -> flags.setFulltext(false)),
                      "testQuery",
                      filterWithoutRemovedCondition,
                      null,
                      null,
                      null,
                      "5m",
                      10,
                      List.of())
                  .source()
                  .query();
    } else {
      testQuery =
          (BoolQueryBuilder)
              requestHandler
                  .getSearchRequest(
                      operationContext.withSearchFlags(flags -> flags.setFulltext(false)),
                      "testQuery",
                      filterWithoutRemovedCondition,
                      null,
                      0,
                      10,
                      List.of())
                  .source()
                  .query();
    }
    return testQuery;
  }

  private void testFilterQuery(BoolQueryBuilder testQuery) {
    Optional<TermQueryBuilder> mustNotHaveRemovedCondition =
        testQuery.filter().stream()
            .filter(or -> or instanceof BoolQueryBuilder)
            .map(or -> (BoolQueryBuilder) or)
            .flatMap(
                or -> {
                  System.out.println("processing: " + or.mustNot());
                  return or.mustNot().stream();
                })
            .filter(and -> and instanceof TermQueryBuilder)
            .map(and -> (TermQueryBuilder) and)
            .filter(match -> match.fieldName().equals("removed"))
            .findAny();

    assertTrue(
        mustNotHaveRemovedCondition.isPresent(),
        "Expected must not have removed condition to exist" + " if filter does not have it");
  }

  private BoolQueryBuilder constructRemovedQuery(
      SearchRequestHandler requestHandler, boolean scroll) {
    final Criterion filterCriterion = buildCriterion("keyword", Condition.EQUAL, "some value");

    final Criterion removedCriterion = buildCriterion("removed", Condition.EQUAL, "false");

    final Filter filterWithRemovedCondition =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(filterCriterion, removedCriterion)))));

    final BoolQueryBuilder queryWithRemoved;
    if (scroll) {
      queryWithRemoved =
          (BoolQueryBuilder)
              requestHandler
                  .getSearchRequest(
                      operationContext.withSearchFlags(flags -> flags.setFulltext(false)),
                      "testQuery",
                      filterWithRemovedCondition,
                      null,
                      null,
                      null,
                      "5m",
                      10,
                      List.of())
                  .source()
                  .query();
    } else {
      queryWithRemoved =
          (BoolQueryBuilder)
              requestHandler
                  .getSearchRequest(
                      operationContext.withSearchFlags(flags -> flags.setFulltext(false)),
                      "testQuery",
                      filterWithRemovedCondition,
                      null,
                      0,
                      10,
                      List.of())
                  .source()
                  .query();
    }
    return queryWithRemoved;
  }

  private void testRemovedQuery(BoolQueryBuilder queryWithRemoved) {
    Optional<MatchQueryBuilder> mustNotHaveRemovedCondition =
        queryWithRemoved.must().stream()
            .filter(or -> or instanceof BoolQueryBuilder)
            .map(or -> (BoolQueryBuilder) or)
            .flatMap(
                or -> {
                  System.out.println("processing: " + or.mustNot());
                  return or.mustNot().stream();
                })
            .filter(and -> and instanceof MatchQueryBuilder)
            .map(and -> (MatchQueryBuilder) and)
            .filter(match -> match.fieldName().equals("removed"))
            .findAny();

    assertFalse(
        mustNotHaveRemovedCondition.isPresent(),
        "Expect `must not have removed` condition to not"
            + " exist because filter already has it a condition for the removed property");
  }

  // For fields that are one of EDITABLE_FIELD_TO_QUERY_PAIRS, we want to make sure
  // a filter that has a list of values like below will filter on all values by generating a terms
  // query
  //  field EQUAL [value1, value2, ...]
  @Test
  public void testFilterFieldTagsByValues() {
    final Criterion filterCriterion = buildCriterion("fieldTags", Condition.EQUAL, "v1", "v2");

    final BoolQueryBuilder testQuery = getQuery(filterCriterion);

    // bool -> filter -> [bool] -> should -> [bool] -> filter -> [bool] -> should -> [terms]
    List<TermsQueryBuilder> termsQueryBuilders =
        testQuery.filter().stream()
            .filter(or -> or instanceof BoolQueryBuilder)
            .flatMap(or -> ((BoolQueryBuilder) or).should().stream())
            .filter(should -> should instanceof BoolQueryBuilder)
            .flatMap(should -> ((BoolQueryBuilder) should).filter().stream())
            .filter(must -> must instanceof BoolQueryBuilder)
            .flatMap(must -> ((BoolQueryBuilder) must).should().stream())
            .filter(should -> should instanceof TermsQueryBuilder)
            .map(should -> (TermsQueryBuilder) should)
            .collect(Collectors.toList());

    assertTrue(termsQueryBuilders.size() == 2, "Expected to find two terms queries");
    Map<String, List<String>> termsMap = new HashMap<>();
    termsQueryBuilders.forEach(
        termsQueryBuilder -> {
          String field = termsQueryBuilder.fieldName();
          List<Object> values = termsQueryBuilder.values();
          List<String> strValues = new ArrayList<>();
          for (Object value : values) {
            assertTrue(
                value instanceof String, "Expected value to be String, got: " + value.getClass());
            strValues.add((String) value);
          }
          Collections.sort(strValues);
          termsMap.put(field, strValues);
        });

    assertTrue(termsMap.containsKey("fieldTags.keyword"));
    assertTrue(termsMap.containsKey("editedFieldTags.keyword"));
    for (List<String> values : termsMap.values()) {
      assertTrue(values.size() == 2);
      assertTrue(values.get(0).equals("v1"));
      assertTrue(values.get(1).equals("v2"));
    }
  }

  // Test fields not in EDITABLE_FIELD_TO_QUERY_PAIRS with a list of values
  @Test
  public void testFilterPlatformByValues() {
    final Criterion filterCriterion =
        buildCriterion("platform", Condition.EQUAL, "mysql", "bigquery");

    final BoolQueryBuilder testQuery = getQuery(filterCriterion);

    // bool -> filter -> [bool] -> should -> [bool] -> filter -> [terms]
    List<TermsQueryBuilder> termsQueryBuilders =
        testQuery.filter().stream()
            .filter(must -> must instanceof BoolQueryBuilder)
            .flatMap(must -> ((BoolQueryBuilder) must).should().stream())
            .filter(should -> should instanceof BoolQueryBuilder)
            .flatMap(should -> ((BoolQueryBuilder) should).filter().stream())
            .filter(must -> must instanceof TermsQueryBuilder)
            .map(must -> (TermsQueryBuilder) must)
            .collect(Collectors.toList());

    assertTrue(termsQueryBuilders.size() == 1, "Expected to find one terms query");
    final TermsQueryBuilder termsQueryBuilder = termsQueryBuilders.get(0);
    assertEquals(termsQueryBuilder.fieldName(), "platform.keyword");
    Set<String> values = new HashSet<>();
    termsQueryBuilder
        .values()
        .forEach(
            value -> {
              assertTrue(value instanceof String);
              values.add((String) value);
            });

    assertEquals(values.size(), 2, "Expected two platform filter values");
    assertTrue(values.contains("mysql"));
    assertTrue(values.contains("bigquery"));
  }

  @Test
  public void testBrowsePathQueryFilter() {
    // Condition: has `browsePaths` AND does NOT have `browsePathV2`
    Criterion missingBrowsePathV2 = buildIsNullCriterion("browsePathV2");
    // Excludes entities without browsePaths
    Criterion hasBrowsePathV1 = buildExistsCriterion("browsePaths");

    CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(missingBrowsePathV2);
    criterionArray.add(hasBrowsePathV1);

    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);

    BoolQueryBuilder test =
        SearchRequestHandler.getFilterQuery(
            operationContext.withSearchFlags(flags -> flags.setFulltext(false)),
            filter,
            new HashMap<>(),
            QueryFilterRewriteChain.EMPTY);

    assertEquals(test.should().size(), 1);

    BoolQueryBuilder shouldQuery = (BoolQueryBuilder) test.should().get(0);
    assertEquals(shouldQuery.filter().size(), 2);

    BoolQueryBuilder mustNotHaveV2 = (BoolQueryBuilder) shouldQuery.filter().get(0);
    assertEquals(((ExistsQueryBuilder) mustNotHaveV2.mustNot().get(0)).fieldName(), "browsePathV2");

    BoolQueryBuilder mustHaveV1 = (BoolQueryBuilder) shouldQuery.filter().get(1);
    assertEquals(((ExistsQueryBuilder) mustHaveV1.must().get(0)).fieldName(), "browsePaths");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidStructuredProperty() {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    Map<Urn, Map<String, Aspect>> aspectResponse = new HashMap<>();
    DataMap statusData = new DataMap();
    statusData.put("removed", true);
    Aspect status = new Aspect(statusData);
    Urn structPropUrn = StructuredPropertyUtils.toURNFromFQN("under.scores.and.dots.make_a_mess");
    aspectResponse.put(structPropUrn, ImmutableMap.of(STATUS_ASPECT_NAME, status));
    when(aspectRetriever.getLatestAspectObjects(
            Collections.singleton(structPropUrn), ImmutableSet.of(STATUS_ASPECT_NAME)))
        .thenReturn(aspectResponse);
    OperationContext mockRetrieverContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            RetrieverContext.builder()
                .aspectRetriever(aspectRetriever)
                .cachingAspectRetriever(TestOperationContexts.emptyActiveUsersAspectRetriever(null))
                .graphRetriever(mock(GraphRetriever.class))
                .searchRetriever(mock(SearchRetriever.class))
                .build());

    Criterion structuredPropCriterion =
        buildExistsCriterion("structuredProperties.under.scores.and.dots.make_a_mess");

    CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(structuredPropCriterion);

    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);

    BoolQueryBuilder test =
        SearchRequestHandler.getFilterQuery(
            mockRetrieverContext.withSearchFlags(flags -> flags.setFulltext(false)),
            filter,
            new HashMap<>(),
            QueryFilterRewriteChain.EMPTY);
  }

  @Test
  public void testQueryByDefault() {
    final Set<String> COMMON =
        Set.of(
            "container",
            "fieldDescriptions",
            "description",
            "platform",
            "fieldPaths",
            "editedFieldGlossaryTerms",
            "editedFieldDescriptions",
            "fieldTags",
            "id",
            "editedDescription",
            "qualifiedName",
            "domains",
            "platformInstance",
            "tags",
            "urn",
            "customProperties",
            "fieldGlossaryTerms",
            "editedName",
            "name",
            "fieldLabels",
            "glossaryTerms",
            "editedFieldTags",
            "displayName",
            "title",
            "applications");

    Map<EntityType, Set<String>> expectedQueryByDefault =
        ImmutableMap.<EntityType, Set<String>>builder()
            .put(
                EntityType.DASHBOARD,
                Stream.concat(COMMON.stream(), Stream.of("tool")).collect(Collectors.toSet()))
            .put(
                EntityType.CHART,
                Stream.concat(COMMON.stream(), Stream.of("tool")).collect(Collectors.toSet()))
            .put(
                EntityType.MLMODEL,
                Stream.concat(COMMON.stream(), Stream.of("type")).collect(Collectors.toSet()))
            .put(
                EntityType.MLFEATURE_TABLE,
                Stream.concat(COMMON.stream(), Stream.of("features", "primaryKeys"))
                    .collect(Collectors.toSet()))
            .put(
                EntityType.MLFEATURE,
                Stream.concat(COMMON.stream(), Stream.of("featureNamespace"))
                    .collect(Collectors.toSet()))
            .put(
                EntityType.MLPRIMARY_KEY,
                Stream.concat(COMMON.stream(), Stream.of("featureNamespace"))
                    .collect(Collectors.toSet()))
            .put(
                EntityType.DATA_FLOW,
                Stream.concat(COMMON.stream(), Stream.of("cluster", "orchestrator", "flowId"))
                    .collect(Collectors.toSet()))
            .put(
                EntityType.DATA_JOB,
                Stream.concat(COMMON.stream(), Stream.of("jobId")).collect(Collectors.toSet()))
            .put(
                EntityType.GLOSSARY_TERM,
                Stream.concat(
                        COMMON.stream(),
                        Stream.of("values", "parentNode", "relatedTerms", "definition"))
                    .collect(Collectors.toSet()))
            .put(
                EntityType.GLOSSARY_NODE,
                Stream.concat(COMMON.stream(), Stream.of("definition", "parentNode"))
                    .collect(Collectors.toSet()))
            .put(
                EntityType.CORP_USER,
                Stream.concat(
                        COMMON.stream(), Stream.of("skills", "teams", "ldap", "fullName", "email"))
                    .collect(Collectors.toSet()))
            .put(
                EntityType.DOMAIN,
                Stream.concat(COMMON.stream(), Stream.of("parentDomain"))
                    .collect(Collectors.toSet()))
            .put(
                EntityType.SCHEMA_FIELD,
                Stream.concat(COMMON.stream(), Stream.of("schemaFieldAliases", "parent"))
                    .collect(Collectors.toSet()))
            .put(
                EntityType.DATA_PROCESS_INSTANCE,
                Stream.concat(
                        COMMON.stream(), Stream.of("parentInstance", "parentTemplate", "status"))
                    .collect(Collectors.toSet()))
            .build();

    for (EntityType entityType : SEARCHABLE_ENTITY_TYPES) {
      Set<String> expectedEntityQueryByDefault =
          expectedQueryByDefault.getOrDefault(entityType, COMMON);
      assertFalse(expectedEntityQueryByDefault.isEmpty());

      EntitySpec entitySpec =
          operationContext.getEntityRegistry().getEntitySpec(EntityTypeMapper.getName(entityType));
      SearchRequestHandler handler =
          SearchRequestHandler.getBuilder(
              operationContext,
              entitySpec,
              testQueryConfig,
              null,
              QueryFilterRewriteChain.EMPTY,
              TEST_SEARCH_SERVICE_CONFIG);

      Set<String> unexpected = new HashSet<>(handler.getDefaultQueryFieldNames());
      unexpected.removeAll(expectedEntityQueryByDefault);

      assertTrue(
          unexpected.isEmpty(),
          String.format(
              "Consider whether these field(s) for entity %s should be included for general search. Fields: %s If yes, please update the test expectations. If no, please annotate the PDL model with \"queryByDefault\": false",
              entityType, unexpected));
    }
  }

  @Test
  public void testFilterLatestVersions() {
    final Criterion filterCriterion =
        buildCriterion("platform", Condition.EQUAL, "mysql", "bigquery");

    final BoolQueryBuilder testQuery =
        getQuery(
            filterCriterion,
            operationContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME),
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

    assertEquals(isLatestQueries.size(), 2, "Expected to find two queries");
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
            operationContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME),
            false);

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

  @Test
  public void testAggregationFilterLatestVersions() {
    final Criterion filterCriterion =
        buildCriterion("platform", Condition.EQUAL, "mysql", "bigquery");

    final BoolQueryBuilder testQuery =
        getAggregationQuery(
            filterCriterion,
            operationContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME),
            true);

    List<QueryBuilder> isLatestQueries =
        testQuery.must().stream()
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

    assertEquals(isLatestQueries.size(), 2, "Expected to find two queries");
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
  public void testAggregationNoFilterLatestVersions() {
    final Criterion filterCriterion =
        buildCriterion("platform", Condition.EQUAL, "mysql", "bigquery");

    final BoolQueryBuilder testQuery =
        getAggregationQuery(
            filterCriterion,
            operationContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME),
            false);

    List<QueryBuilder> isLatestQueries =
        testQuery.must().stream()
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

  @Test
  public void testApplyResultLimitInSearchRequest() {
    // Create a custom SearchConfiguration with specific limits
    SearchServiceConfiguration limitConfig =
        TEST_SEARCH_SERVICE_CONFIG.toBuilder()
            .limit(
                new LimitConfig()
                    .setResults(
                        new ResultsLimitConfig().setMax(40).setApiDefault(40).setStrict(false)))
            .build();

    // Create a handler with our test configuration
    SearchRequestHandler limitHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            limitConfig);

    // Test with count below limit
    int requestedSize = 30;
    SearchRequest searchRequest =
        limitHandler.getSearchRequest(
            operationContext.withSearchFlags(flags -> flags.setFulltext(false)),
            "test query",
            null,
            null,
            0,
            requestedSize,
            List.of());

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify the requested size was used (not limited)
    assertEquals(sourceBuilder.size(), requestedSize);

    // Test with count above limit (non-strict)
    requestedSize = 100;
    searchRequest =
        limitHandler.getSearchRequest(
            operationContext.withSearchFlags(flags -> flags.setFulltext(false)),
            "test query",
            null,
            null,
            0,
            requestedSize,
            List.of());

    sourceBuilder = searchRequest.source();

    // Verify the max limit was applied
    assertEquals(sourceBuilder.size(), 40);
  }

  @Test
  public void testApplyResultLimitWithStrictConfiguration() {
    // Create a SearchConfiguration with strict limits
    SearchServiceConfiguration strictConfig =
        TEST_SEARCH_SERVICE_CONFIG.toBuilder()
            .limit(
                LimitConfig.builder()
                    .results(new ResultsLimitConfig().setMax(30).setApiDefault(30).setStrict(true))
                    .build())
            .build();

    // Create a handler with our strict configuration
    SearchRequestHandler strictHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            strictConfig);

    // Test with count at the limit
    int requestedSize = 30;
    SearchRequest searchRequest =
        strictHandler.getSearchRequest(
            operationContext.withSearchFlags(flags -> flags.setFulltext(false)),
            "test query",
            null,
            null,
            0,
            requestedSize,
            List.of());

    SearchSourceBuilder sourceBuilder = searchRequest.source();

    // Verify exact limit was used
    assertEquals(sourceBuilder.size(), 30);

    // Test with count exceeding the limit in strict mode
    // This should throw an IllegalArgumentException
    try {
      requestedSize = 50;
      strictHandler.getSearchRequest(
          operationContext.withSearchFlags(flags -> flags.setFulltext(false)),
          "test query",
          null,
          null,
          0,
          requestedSize,
          List.of());
      Assert.fail(
          "Should throw IllegalArgumentException when count exceeds limit with strict config");
    } catch (IllegalArgumentException e) {
      // Expected exception
      assertTrue(e.getMessage().contains("Result count exceeds limit of 30"));
    }
  }

  @Test
  public void testApplyResultLimitInFilterRequest() {
    // Create a SearchConfiguration with specific limits
    SearchServiceConfiguration limitConfig =
        TEST_SEARCH_SERVICE_CONFIG.toBuilder()
            .limit(
                new LimitConfig()
                    .setResults(
                        new ResultsLimitConfig().setMax(25).setApiDefault(25).setStrict(false)))
            .build();

    // Create a handler with our test configuration
    SearchRequestHandler limitHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            limitConfig);

    // Create a filter
    Criterion filterCriterion = buildCriterion("platform", Condition.EQUAL, "mysql");
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(ImmutableList.of(filterCriterion)))));

    // Test with size below limit
    int requestedSize = 20;
    SearchRequest filterRequest =
        limitHandler.getFilterRequest(operationContext, filter, null, 0, requestedSize);

    SearchSourceBuilder sourceBuilder = filterRequest.source();

    // Verify the requested size was used (not limited)
    assertEquals(sourceBuilder.size(), requestedSize);

    // Test with size above limit
    requestedSize = 50;
    filterRequest = limitHandler.getFilterRequest(operationContext, filter, null, 0, requestedSize);

    sourceBuilder = filterRequest.source();

    // Verify the max limit was applied
    assertEquals(sourceBuilder.size(), 25);
  }

  @Test
  public void testExtractResultWithNullSize() {
    // Create a mock SearchResponse
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(100L, TotalHits.Relation.EQUAL_TO));
    when(mockHits.getHits()).thenReturn(new SearchHit[0]);
    when(mockResponse.getAggregations()).thenReturn(null);
    when(mockResponse.getSuggest()).thenReturn(null);

    SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Test with null size
    SearchResult result = handler.extractResult(operationContext, mockResponse, null, 0, null);

    // Should use the default from the service config
    assertEquals(
        result.getPageSize().intValue(),
        TEST_SEARCH_SERVICE_CONFIG.getLimit().getResults().getApiDefault());
    assertEquals(result.getFrom().intValue(), 0);
    assertEquals(result.getNumEntities().intValue(), 100);
  }

  @Test
  public void testExtractResultWithLimitConfiguration() {
    // Create a custom SearchServiceConfiguration with specific limits
    SearchServiceConfiguration limitConfig =
        TEST_SEARCH_SERVICE_CONFIG.toBuilder()
            .limit(
                LimitConfig.builder()
                    .results(
                        ResultsLimitConfig.builder().max(50).apiDefault(30).strict(false).build())
                    .build())
            .build();

    SearchRequestHandler limitHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            limitConfig);

    // Create a mock SearchResponse
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(200L, TotalHits.Relation.EQUAL_TO));
    when(mockHits.getHits()).thenReturn(new SearchHit[0]);
    when(mockResponse.getAggregations()).thenReturn(null);
    when(mockResponse.getSuggest()).thenReturn(null);

    // Test with size above limit
    SearchResult result =
        limitHandler.extractResult(
            operationContext, mockResponse, null, 0, 100); // Requesting 100, but max is 50

    // Should be limited to 30, applying default
    assertEquals(result.getPageSize().intValue(), 30);

    // Test with size below limit
    result = limitHandler.extractResult(operationContext, mockResponse, null, 0, 25);

    // Should use the requested size
    assertEquals(result.getPageSize().intValue(), 25);

    // Test with null size - should use API default
    result = limitHandler.extractResult(operationContext, mockResponse, null, 0, null);

    // Should use the API default (30)
    assertEquals(result.getPageSize().intValue(), 30);
  }

  @Test
  public void testExtractScrollResultWithNullSize() {
    // Create a mock SearchResponse
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(100L, TotalHits.Relation.EQUAL_TO));
    when(mockHits.getHits()).thenReturn(new SearchHit[0]);
    when(mockResponse.getAggregations()).thenReturn(null);
    when(mockResponse.getSuggest()).thenReturn(null);
    when(mockResponse.pointInTimeId()).thenReturn("test-pit-id");

    SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Test with null size
    ScrollResult result =
        handler.extractScrollResult(operationContext, mockResponse, null, "5m", null, true);

    // Should use the default from the service config default
    assertEquals(
        result.getPageSize().intValue(),
        TEST_SEARCH_SERVICE_CONFIG.getLimit().getResults().getApiDefault());
    assertEquals(result.getNumEntities().intValue(), 100);
  }

  @Test
  public void testExtractScrollResultWithLimitConfiguration() {
    // Create a custom SearchServiceConfiguration with specific limits
    SearchServiceConfiguration limitConfig =
        TEST_SEARCH_SERVICE_CONFIG.toBuilder()
            .limit(
                LimitConfig.builder()
                    .results(
                        ResultsLimitConfig.builder().max(40).apiDefault(40).strict(false).build())
                    .build())
            .build();

    SearchRequestHandler limitHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            limitConfig);

    // Test with size above limit
    ScrollResult result = verifyScrollResultSize(limitHandler, 40, 80, true);

    // Should be limited to 40 as the default
    assertEquals(result.getPageSize().intValue(), 40);
    assertNotNull(result.getScrollId()); // Should have next scroll ID since we have full page

    // Test with size below limit - partial page
    result = verifyScrollResultSize(limitHandler, 15, 20, false);

    // Should use the requested size
    assertEquals(result.getPageSize().intValue(), 20);
    assertFalse(result.hasScrollId()); // No next scroll ID since results < page size

    // Test with null size - should use API default
    result = verifyScrollResultSize(limitHandler, 40, null, true);

    // Should use the API default (40)
    assertEquals(result.getPageSize().intValue(), 40);
    assertNotNull(result.getScrollId()); // Should have next scroll ID
  }

  @Test
  public void testExtractScrollResultPaginationLogic() {
    SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            operationContext,
            TestEntitySpecBuilder.getSpec(),
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    // Test when results equal page size - should have scroll ID
    ScrollResult result = verifyScrollResultSize(handler, 10, 10, true);

    assertEquals(result.getPageSize().intValue(), 10);
    assertNotNull(result.getScrollId());

    // Test when results less than page size - should NOT have scroll ID
    result = verifyScrollResultSize(handler, 5, 10, false);

    assertEquals(result.getPageSize().intValue(), 10);
    assertFalse(result.hasScrollId());
  }

  // Helper method to create scroll results with specific sizes
  private ScrollResult verifyScrollResultSize(
      SearchRequestHandler handler,
      int actualResults,
      Integer requestedSize,
      boolean expectScrollId) {

    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(100L, TotalHits.Relation.EQUAL_TO));
    when(mockResponse.getAggregations()).thenReturn(null);
    when(mockResponse.getSuggest()).thenReturn(null);
    when(mockResponse.pointInTimeId()).thenReturn("test-pit-id");

    // Create array of mock hits
    SearchHit[] hits = new SearchHit[actualResults];
    for (int i = 0; i < actualResults; i++) {
      SearchHit mockHit = mock(SearchHit.class);
      when(mockHit.getSourceAsMap())
          .thenReturn(
              ImmutableMap.of(
                  "urn", "urn:li:dataset:(urn:li:dataPlatform:hdfs,test" + i + ",PROD)"));
      when(mockHit.getScore()).thenReturn(1.0f);
      when(mockHit.getHighlightFields()).thenReturn(ImmutableMap.of());
      when(mockHit.getMatchedQueries()).thenReturn(new String[0]);
      when(mockHit.getSortValues()).thenReturn(new Object[] {"sortValue" + i});
      hits[i] = mockHit;
    }
    when(mockHits.getHits()).thenReturn(hits);

    return handler.extractScrollResult(
        operationContext, mockResponse, null, "5m", requestedSize, true);
  }

  private BoolQueryBuilder getQuery(final Criterion filterCriterion) {
    return getQuery(filterCriterion, TestEntitySpecBuilder.getSpec(), true);
  }

  private BoolQueryBuilder getQuery(
      final Criterion filterCriterion, final EntitySpec entitySpec, boolean filterNonLatest) {
    final Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(ImmutableList.of(filterCriterion)))));

    final SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            entitySpec,
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    return (BoolQueryBuilder)
        requestHandler
            .getSearchRequest(
                operationContext.withSearchFlags(
                    flags -> flags.setFulltext(false).setFilterNonLatestVersions(filterNonLatest)),
                "",
                filter,
                null,
                0,
                10,
                List.of())
            .source()
            .query();
  }

  private BoolQueryBuilder getAggregationQuery(
      final Criterion filterCriterion, final EntitySpec entitySpec, boolean filterNonLatest) {
    final Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(ImmutableList.of(filterCriterion)))));

    final SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            operationContext,
            entitySpec,
            testQueryConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    return (BoolQueryBuilder)
        requestHandler
            .getAggregationRequest(
                operationContext.withSearchFlags(
                    flags -> flags.setFulltext(false).setFilterNonLatestVersions(filterNonLatest)),
                "platform",
                filter,
                10)
            .source()
            .query();
  }
}
