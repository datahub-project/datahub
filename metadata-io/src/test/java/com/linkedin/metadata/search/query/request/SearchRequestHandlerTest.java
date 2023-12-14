package com.linkedin.metadata.search.query.request;

import static com.linkedin.metadata.utils.SearchUtil.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@Import(SearchCommonTestConfiguration.class)
public class SearchRequestHandlerTest extends AbstractTestNGSpringContextTests {
  @Autowired private EntityRegistry entityRegistry;

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

  @Test
  public void testDatasetFieldsAndHighlights() {
    EntitySpec entitySpec = entityRegistry.getEntitySpec("dataset");
    SearchRequestHandler datasetHandler =
        SearchRequestHandler.getBuilder(entitySpec, testQueryConfig, null);

    /*
      Ensure efficient query performance, we do not expect upstream/downstream/fineGrained lineage
    */
    List<String> highlightFields =
        datasetHandler.getHighlights().fields().stream()
            .map(HighlightBuilder.Field::name)
            .collect(Collectors.toList());
    assertTrue(
        highlightFields.stream()
            .noneMatch(
                fieldName -> fieldName.contains("upstream") || fieldName.contains("downstream")),
        "unexpected lineage fields in highlights: " + highlightFields);
  }

  @Test
  public void testSearchRequestHandlerHighlightingTurnedOff() {
    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(TestEntitySpecBuilder.getSpec(), testQueryConfig, null);
    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            "testQuery",
            null,
            null,
            0,
            10,
            new SearchFlags().setFulltext(false).setSkipHighlighting(true),
            null);
    SearchSourceBuilder sourceBuilder = searchRequest.source();
    assertEquals(sourceBuilder.from(), 0);
    assertEquals(sourceBuilder.size(), 10);
    // Filters
    Collection<AggregationBuilder> aggBuilders =
        sourceBuilder.aggregations().getAggregatorFactories();
    // Expect 2 aggregations: textFieldOverride and _index
    assertEquals(aggBuilders.size(), 2);
    for (AggregationBuilder aggBuilder : aggBuilders) {
      if (aggBuilder.getName().equals("textFieldOverride")) {
        TermsAggregationBuilder filterPanelBuilder = (TermsAggregationBuilder) aggBuilder;
        assertEquals(filterPanelBuilder.field(), "textFieldOverride.keyword");
      } else if (!aggBuilder.getName().equals("_entityType")) {
        fail("Found unexepected aggregation: " + aggBuilder.getName());
      }
    }
    // Highlights should not be present
    assertNull(sourceBuilder.highlighter());
  }

  @Test
  public void testSearchRequestHandler() {
    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(TestEntitySpecBuilder.getSpec(), testQueryConfig, null);
    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            "testQuery", null, null, 0, 10, new SearchFlags().setFulltext(false), null);
    SearchSourceBuilder sourceBuilder = searchRequest.source();
    assertEquals(sourceBuilder.from(), 0);
    assertEquals(sourceBuilder.size(), 10);
    // Filters
    Collection<AggregationBuilder> aggBuilders =
        sourceBuilder.aggregations().getAggregatorFactories();
    // Expect 2 aggregations: textFieldOverride and _index
    assertEquals(aggBuilders.size(), 2);
    for (AggregationBuilder aggBuilder : aggBuilders) {
      if (aggBuilder.getName().equals("textFieldOverride")) {
        TermsAggregationBuilder filterPanelBuilder = (TermsAggregationBuilder) aggBuilder;
        assertEquals(filterPanelBuilder.field(), "textFieldOverride.keyword");
      } else if (!aggBuilder.getName().equals("_entityType")) {
        fail("Found unexepected aggregation: " + aggBuilder.getName());
      }
    }
    // Highlights
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<String> fields =
        highlightBuilder.fields().stream()
            .map(HighlightBuilder.Field::name)
            .collect(Collectors.toList());
    assertEquals(fields.size(), 22);
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
            "wordGramField");
    highlightableFields.forEach(
        field -> {
          assertTrue(fields.contains(field), "Missing: " + field);
          assertTrue(fields.contains(field + ".*"), "Missing: " + field + ".*");
        });
  }

  @Test
  public void testAggregationsInSearch() {
    SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(TestEntitySpecBuilder.getSpec(), testQueryConfig, null);
    final String nestedAggString =
        String.format("_entityType%stextFieldOverride", AGGREGATION_SEPARATOR_CHAR);
    SearchRequest searchRequest =
        requestHandler.getSearchRequest(
            "*",
            null,
            null,
            0,
            10,
            new SearchFlags().setFulltext(true),
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
            .size(testQueryConfig.getMaxTermBucketSize());
    AggregationBuilder expectedEntityTypeAggregationBuilder =
        AggregationBuilders.terms("_entityType")
            .field("_index")
            .size(testQueryConfig.getMaxTermBucketSize())
            .minDocCount(0);
    AggregationBuilder expectedNestedAggregationBuilder =
        AggregationBuilders.terms(nestedAggString)
            .field("_index")
            .size(testQueryConfig.getMaxTermBucketSize())
            .minDocCount(0)
            .subAggregation(
                AggregationBuilders.terms(nestedAggString)
                    .field("textFieldOverride.keyword")
                    .size(testQueryConfig.getMaxTermBucketSize()));

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
        SearchRequestHandler.getBuilder(TestEntitySpecBuilder.getSpec(), testQueryConfig, null);

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
    final Criterion filterCriterion =
        new Criterion().setField("keyword").setCondition(Condition.EQUAL).setValue("some value");

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
                      "testQuery",
                      filterWithoutRemovedCondition,
                      null,
                      null,
                      null,
                      "5m",
                      10,
                      new SearchFlags().setFulltext(false))
                  .source()
                  .query();
    } else {
      testQuery =
          (BoolQueryBuilder)
              requestHandler
                  .getSearchRequest(
                      "testQuery",
                      filterWithoutRemovedCondition,
                      null,
                      0,
                      10,
                      new SearchFlags().setFulltext(false),
                      null)
                  .source()
                  .query();
    }
    return testQuery;
  }

  private void testFilterQuery(BoolQueryBuilder testQuery) {
    Optional<MatchQueryBuilder> mustNotHaveRemovedCondition =
        testQuery.filter().stream()
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

    assertTrue(
        mustNotHaveRemovedCondition.isPresent(),
        "Expected must not have removed condition to exist" + " if filter does not have it");
  }

  private BoolQueryBuilder constructRemovedQuery(
      SearchRequestHandler requestHandler, boolean scroll) {
    final Criterion filterCriterion =
        new Criterion().setField("keyword").setCondition(Condition.EQUAL).setValue("some value");

    final Criterion removedCriterion =
        new Criterion()
            .setField("removed")
            .setCondition(Condition.EQUAL)
            .setValue(String.valueOf(false));

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
                      "testQuery",
                      filterWithRemovedCondition,
                      null,
                      null,
                      null,
                      "5m",
                      10,
                      new SearchFlags().setFulltext(false))
                  .source()
                  .query();
    } else {
      queryWithRemoved =
          (BoolQueryBuilder)
              requestHandler
                  .getSearchRequest(
                      "testQuery",
                      filterWithRemovedCondition,
                      null,
                      0,
                      10,
                      new SearchFlags().setFulltext(false),
                      null)
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
    final Criterion filterCriterion =
        new Criterion()
            .setField("fieldTags")
            .setCondition(Condition.EQUAL)
            .setValue("v1")
            .setValues(new StringArray("v1", "v2"));

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

  // For fields that are one of EDITABLE_FIELD_TO_QUERY_PAIRS, we want to make sure
  // a filter that has a single value will result in one filter for each field in the
  // pair of fields
  @Test
  public void testFilterFieldTagsByValue() {
    final Criterion filterCriterion =
        new Criterion().setField("fieldTags").setCondition(Condition.EQUAL).setValue("v1");

    final BoolQueryBuilder testQuery = getQuery(filterCriterion);

    // bool -> must -> [bool] -> should -> [bool] -> must -> [bool] -> should -> [bool] -> should ->
    // [match]
    List<MultiMatchQueryBuilder> matchQueryBuilders =
        testQuery.filter().stream()
            .filter(or -> or instanceof BoolQueryBuilder)
            .flatMap(or -> ((BoolQueryBuilder) or).should().stream())
            .filter(should -> should instanceof BoolQueryBuilder)
            .flatMap(should -> ((BoolQueryBuilder) should).filter().stream())
            .filter(must -> must instanceof BoolQueryBuilder)
            .flatMap(must -> ((BoolQueryBuilder) must).should().stream())
            .filter(should -> should instanceof BoolQueryBuilder)
            .flatMap(should -> ((BoolQueryBuilder) should).should().stream())
            .filter(should -> should instanceof MultiMatchQueryBuilder)
            .map(should -> (MultiMatchQueryBuilder) should)
            .collect(Collectors.toList());

    assertTrue(matchQueryBuilders.size() == 2, "Expected to find two match queries");
    Map<String, String> matchMap = new HashMap<>();
    matchQueryBuilders.forEach(
        matchQueryBuilder -> {
          Set<String> fields = matchQueryBuilder.fields().keySet();
          assertTrue(matchQueryBuilder.value() instanceof String);
          fields.forEach(field -> matchMap.put(field, (String) matchQueryBuilder.value()));
        });

    assertTrue(matchMap.containsKey("fieldTags.keyword"));
    assertTrue(matchMap.containsKey("editedFieldTags.keyword"));
    for (String value : matchMap.values()) {
      assertTrue(value.equals("v1"));
    }
  }

  // Test fields not in EDITABLE_FIELD_TO_QUERY_PAIRS with a single value
  @Test
  public void testFilterPlatformByValue() {
    final Criterion filterCriterion =
        new Criterion().setField("platform").setCondition(Condition.EQUAL).setValue("mysql");

    final BoolQueryBuilder testQuery = getQuery(filterCriterion);

    // bool -> filter -> [bool] -> should -> [bool] -> filter -> [bool] -> should -> [match]
    List<MultiMatchQueryBuilder> matchQueryBuilders =
        testQuery.filter().stream()
            .filter(or -> or instanceof BoolQueryBuilder)
            .flatMap(or -> ((BoolQueryBuilder) or).should().stream())
            .filter(should -> should instanceof BoolQueryBuilder)
            .flatMap(should -> ((BoolQueryBuilder) should).filter().stream())
            .filter(must -> must instanceof BoolQueryBuilder)
            .flatMap(must -> ((BoolQueryBuilder) must).should().stream())
            .filter(should -> should instanceof MultiMatchQueryBuilder)
            .map(should -> (MultiMatchQueryBuilder) should)
            .collect(Collectors.toList());

    assertTrue(matchQueryBuilders.size() == 1, "Expected to find one match query");
    MultiMatchQueryBuilder matchQueryBuilder = matchQueryBuilders.get(0);
    assertEquals(
        matchQueryBuilder.fields(),
        Map.of(
            "platform", 1.0f,
            "platform.*", 1.0f));
    assertEquals(matchQueryBuilder.value(), "mysql");
  }

  // Test fields not in EDITABLE_FIELD_TO_QUERY_PAIRS with a list of values
  @Test
  public void testFilterPlatformByValues() {
    final Criterion filterCriterion =
        new Criterion()
            .setField("platform")
            .setCondition(Condition.EQUAL)
            .setValue("mysql")
            .setValues(new StringArray("mysql", "bigquery"));

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
    Criterion missingBrowsePathV2 = new Criterion();
    missingBrowsePathV2.setCondition(Condition.IS_NULL);
    missingBrowsePathV2.setField("browsePathV2");
    // Excludes entities without browsePaths
    Criterion hasBrowsePathV1 = new Criterion();
    hasBrowsePathV1.setCondition(Condition.EXISTS);
    hasBrowsePathV1.setField("browsePaths");

    CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(missingBrowsePathV2);
    criterionArray.add(hasBrowsePathV1);

    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);

    BoolQueryBuilder test = SearchRequestHandler.getFilterQuery(filter);

    assertEquals(test.should().size(), 1);

    BoolQueryBuilder shouldQuery = (BoolQueryBuilder) test.should().get(0);
    assertEquals(shouldQuery.filter().size(), 2);

    BoolQueryBuilder mustNotHaveV2 = (BoolQueryBuilder) shouldQuery.filter().get(0);
    assertEquals(((ExistsQueryBuilder) mustNotHaveV2.mustNot().get(0)).fieldName(), "browsePathV2");

    BoolQueryBuilder mustHaveV1 = (BoolQueryBuilder) shouldQuery.filter().get(1);
    assertEquals(((ExistsQueryBuilder) mustHaveV1.must().get(0)).fieldName(), "browsePaths");
  }

  private BoolQueryBuilder getQuery(final Criterion filterCriterion) {
    final Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(ImmutableList.of(filterCriterion)))));

    final SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(TestEntitySpecBuilder.getSpec(), testQueryConfig, null);

    return (BoolQueryBuilder)
        requestHandler
            .getSearchRequest("", filter, null, 0, 10, new SearchFlags().setFulltext(false), null)
            .source()
            .query();
  }
}
