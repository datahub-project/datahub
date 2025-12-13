package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.List;
import org.testng.annotations.Test;

public class DefaultEntityFiltersUtilTest {

  @Test
  public void testAddDefaultEntityFilters_NoDocument() {
    // When DOCUMENT is not in the entity types, no filters should be added
    List<String> entityTypes = ImmutableList.of(DATASET_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true);

    assertNull(result);
  }

  @Test
  public void testAddDefaultEntityFilters_NoDocumentWithBaseFilter() {
    // When DOCUMENT is not in entity types, base filter should be returned unchanged
    List<String> entityTypes = ImmutableList.of(DATASET_ENTITY_NAME);
    Filter baseFilter = new Filter();

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(baseFilter, entityTypes, true);

    assertSame(result, baseFilter);
  }

  @Test
  public void testAddDefaultEntityFilters_WithDocumentAndShowInGlobalContext() {
    // When DOCUMENT is in entity types with showInGlobalContext=true,
    // should add document filters using negated EQUAL for cross-entity compatibility
    List<String> entityTypes = ImmutableList.of(DATASET_ENTITY_NAME, DOCUMENT_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true);

    assertNotNull(result);
    assertNotNull(result.getOr());
    assertEquals(result.getOr().size(), 1); // Single AND clause with negated conditions

    ConjunctiveCriterion clause = result.getOr().get(0);
    assertEquals(clause.getAnd().size(), 2); // state, showInGlobalContext

    // Verify negated EQUAL conditions (these pass for non-documents since field doesn't exist)
    assertTrue(hasNegatedFieldWithCondition(clause, "state", Condition.EQUAL));
    assertTrue(hasNegatedFieldWithCondition(clause, "showInGlobalContext", Condition.EQUAL));
  }

  @Test
  public void testAddDefaultEntityFilters_WithDocumentWithoutShowInGlobalContext() {
    // When DOCUMENT is in entity types with showInGlobalContext=false,
    // should add document filters without showInGlobalContext criterion
    List<String> entityTypes = ImmutableList.of(DOCUMENT_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, false);

    assertNotNull(result);
    assertNotNull(result.getOr());
    assertEquals(result.getOr().size(), 1); // Single AND clause

    ConjunctiveCriterion clause = result.getOr().get(0);
    assertEquals(clause.getAnd().size(), 1); // state only (no showInGlobalContext)

    assertTrue(hasNegatedFieldWithCondition(clause, "state", Condition.EQUAL));
    assertFalse(hasField(clause, "showInGlobalContext"));
  }

  @Test
  public void testAddDefaultEntityFilters_DocumentOnlyEntity() {
    // When only DOCUMENT is being searched
    List<String> entityTypes = ImmutableList.of(DOCUMENT_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true);

    assertNotNull(result);
    assertEquals(result.getOr().size(), 1); // Single AND clause
  }

  @Test
  public void testAddDefaultEntityFilters_VerifyFilterValues() {
    // Verify the actual filter values are correct
    List<String> entityTypes = ImmutableList.of(DOCUMENT_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true);

    ConjunctiveCriterion clause = result.getOr().get(0);

    // Verify state != UNPUBLISHED (negated EQUAL)
    Criterion stateCriterion = findCriterion(clause, "state");
    assertNotNull(stateCriterion);
    assertEquals(stateCriterion.getCondition(), Condition.EQUAL);
    assertTrue(stateCriterion.isNegated());
    assertTrue(stateCriterion.getValues().contains("UNPUBLISHED"));

    // Verify showInGlobalContext != false (negated EQUAL)
    Criterion showInGlobalContextCriterion = findCriterion(clause, "showInGlobalContext");
    assertNotNull(showInGlobalContextCriterion);
    assertEquals(showInGlobalContextCriterion.getCondition(), Condition.EQUAL);
    assertTrue(showInGlobalContextCriterion.isNegated());
    assertTrue(showInGlobalContextCriterion.getValues().contains("false"));
  }

  private static boolean hasField(ConjunctiveCriterion clause, String field) {
    return findCriterion(clause, field) != null;
  }

  private static boolean hasFieldWithCondition(
      ConjunctiveCriterion clause, String field, Condition condition) {
    Criterion criterion = findCriterion(clause, field);
    return criterion != null
        && condition.equals(criterion.getCondition())
        && !criterion.isNegated();
  }

  private static boolean hasNegatedFieldWithCondition(
      ConjunctiveCriterion clause, String field, Condition condition) {
    Criterion criterion = findCriterion(clause, field);
    return criterion != null && condition.equals(criterion.getCondition()) && criterion.isNegated();
  }

  private static Criterion findCriterion(ConjunctiveCriterion clause, String field) {
    for (Criterion criterion : clause.getAnd()) {
      if (field.equals(criterion.getField())) {
        return criterion;
      }
    }
    return null;
  }

  // ============================================================================
  // Tests for removeDefaultFilterFieldsFromAggregations (SearchResult)
  // ============================================================================

  @Test
  public void testRemoveDefaultFilterFieldsFromAggregations_SearchResult_NoMetadata() {
    // When search result has no metadata, should return unchanged
    SearchResult searchResult = new SearchResult();
    searchResult.setEntities(new SearchEntityArray());
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setNumEntities(0);

    SearchResult result =
        DefaultEntityFiltersUtil.removeDefaultFilterFieldsFromAggregations(searchResult);

    assertSame(result, searchResult);
  }

  @Test
  public void testRemoveDefaultFilterFieldsFromAggregations_SearchResult_NoAggregations() {
    // When search result has metadata but no aggregations, should return unchanged
    SearchResult searchResult = new SearchResult();
    searchResult.setMetadata(new SearchResultMetadata());
    searchResult.setEntities(new SearchEntityArray());
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setNumEntities(0);

    SearchResult result =
        DefaultEntityFiltersUtil.removeDefaultFilterFieldsFromAggregations(searchResult);

    assertSame(result, searchResult);
  }

  @Test
  public void testRemoveDefaultFilterFieldsFromAggregations_SearchResult_RemovesDefaultFields() {
    // Should remove aggregations for "state" and "showInGlobalContext"
    SearchResult searchResult =
        createSearchResultWithAggregations(
            ImmutableList.of("platform", "state", "owners", "showInGlobalContext", "tags"));

    SearchResult result =
        DefaultEntityFiltersUtil.removeDefaultFilterFieldsFromAggregations(searchResult);

    AggregationMetadataArray aggs = result.getMetadata().getAggregations();
    assertEquals(aggs.size(), 3);
    assertTrue(hasAggregation(aggs, "platform"));
    assertTrue(hasAggregation(aggs, "owners"));
    assertTrue(hasAggregation(aggs, "tags"));
    assertFalse(hasAggregation(aggs, "state"));
    assertFalse(hasAggregation(aggs, "showInGlobalContext"));
  }

  @Test
  public void testRemoveDefaultFilterFieldsFromAggregations_SearchResult_NoDefaultFieldsPresent() {
    // When no default filter fields are present, aggregations should remain unchanged
    SearchResult searchResult =
        createSearchResultWithAggregations(ImmutableList.of("platform", "owners", "tags"));

    int originalSize = searchResult.getMetadata().getAggregations().size();

    SearchResult result =
        DefaultEntityFiltersUtil.removeDefaultFilterFieldsFromAggregations(searchResult);

    assertEquals(result.getMetadata().getAggregations().size(), originalSize);
    assertTrue(hasAggregation(result.getMetadata().getAggregations(), "platform"));
    assertTrue(hasAggregation(result.getMetadata().getAggregations(), "owners"));
    assertTrue(hasAggregation(result.getMetadata().getAggregations(), "tags"));
  }

  @Test
  public void testRemoveDefaultFilterFieldsFromAggregations_SearchResult_OnlyDefaultFields() {
    // When only default filter fields are present, should return empty aggregations
    SearchResult searchResult =
        createSearchResultWithAggregations(ImmutableList.of("state", "showInGlobalContext"));

    SearchResult result =
        DefaultEntityFiltersUtil.removeDefaultFilterFieldsFromAggregations(searchResult);

    assertEquals(result.getMetadata().getAggregations().size(), 0);
  }

  // ============================================================================
  // Tests for removeDefaultFilterFieldsFromAggregations (ScrollResult)
  // ============================================================================

  @Test
  public void testRemoveDefaultFilterFieldsFromAggregations_ScrollResult_NoMetadata() {
    // When scroll result has no metadata, should return unchanged
    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(new SearchEntityArray());
    scrollResult.setPageSize(10);
    scrollResult.setNumEntities(0);

    ScrollResult result =
        DefaultEntityFiltersUtil.removeDefaultFilterFieldsFromAggregations(scrollResult);

    assertSame(result, scrollResult);
  }

  @Test
  public void testRemoveDefaultFilterFieldsFromAggregations_ScrollResult_NoAggregations() {
    // When scroll result has metadata but no aggregations, should return unchanged
    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setMetadata(new SearchResultMetadata());
    scrollResult.setEntities(new SearchEntityArray());
    scrollResult.setPageSize(10);
    scrollResult.setNumEntities(0);

    ScrollResult result =
        DefaultEntityFiltersUtil.removeDefaultFilterFieldsFromAggregations(scrollResult);

    assertSame(result, scrollResult);
  }

  @Test
  public void testRemoveDefaultFilterFieldsFromAggregations_ScrollResult_RemovesDefaultFields() {
    // Should remove aggregations for "state" and "showInGlobalContext"
    ScrollResult scrollResult =
        createScrollResultWithAggregations(
            ImmutableList.of("platform", "state", "domains", "showInGlobalContext"));

    ScrollResult result =
        DefaultEntityFiltersUtil.removeDefaultFilterFieldsFromAggregations(scrollResult);

    AggregationMetadataArray aggs = result.getMetadata().getAggregations();
    assertEquals(aggs.size(), 2);
    assertTrue(hasAggregation(aggs, "platform"));
    assertTrue(hasAggregation(aggs, "domains"));
    assertFalse(hasAggregation(aggs, "state"));
    assertFalse(hasAggregation(aggs, "showInGlobalContext"));
  }

  @Test
  public void testRemoveDefaultFilterFieldsFromAggregations_ScrollResult_NoDefaultFieldsPresent() {
    // When no default filter fields are present, aggregations should remain unchanged
    ScrollResult scrollResult =
        createScrollResultWithAggregations(ImmutableList.of("platform", "domains"));

    int originalSize = scrollResult.getMetadata().getAggregations().size();

    ScrollResult result =
        DefaultEntityFiltersUtil.removeDefaultFilterFieldsFromAggregations(scrollResult);

    assertEquals(result.getMetadata().getAggregations().size(), originalSize);
  }

  // ============================================================================
  // Test for DEFAULT_FILTER_FIELDS constant
  // ============================================================================

  @Test
  public void testDefaultFilterFieldsConstant() {
    // Verify the expected fields are in the constant
    assertTrue(DefaultEntityFiltersUtil.DEFAULT_FILTER_FIELDS.contains("state"));
    assertTrue(DefaultEntityFiltersUtil.DEFAULT_FILTER_FIELDS.contains("showInGlobalContext"));
    assertEquals(DefaultEntityFiltersUtil.DEFAULT_FILTER_FIELDS.size(), 2);
  }

  // ============================================================================
  // Helper methods
  // ============================================================================

  private static SearchResult createSearchResultWithAggregations(List<String> aggregationNames) {
    SearchResult searchResult = new SearchResult();
    searchResult.setEntities(new SearchEntityArray());
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setNumEntities(0);

    SearchResultMetadata metadata = new SearchResultMetadata();
    AggregationMetadataArray aggs = new AggregationMetadataArray();

    for (String name : aggregationNames) {
      aggs.add(createAggregationMetadata(name));
    }

    metadata.setAggregations(aggs);
    searchResult.setMetadata(metadata);

    return searchResult;
  }

  private static ScrollResult createScrollResultWithAggregations(List<String> aggregationNames) {
    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(new SearchEntityArray());
    scrollResult.setPageSize(10);
    scrollResult.setNumEntities(0);

    SearchResultMetadata metadata = new SearchResultMetadata();
    AggregationMetadataArray aggs = new AggregationMetadataArray();

    for (String name : aggregationNames) {
      aggs.add(createAggregationMetadata(name));
    }

    metadata.setAggregations(aggs);
    scrollResult.setMetadata(metadata);

    return scrollResult;
  }

  private static AggregationMetadata createAggregationMetadata(String name) {
    AggregationMetadata agg = new AggregationMetadata();
    agg.setName(name);
    agg.setDisplayName(name);
    agg.setAggregations(new LongMap());
    return agg;
  }

  private static boolean hasAggregation(AggregationMetadataArray aggs, String name) {
    for (AggregationMetadata agg : aggs) {
      if (name.equals(agg.getName())) {
        return true;
      }
    }
    return false;
  }
}
