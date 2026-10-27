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

    Filter result =
        DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true, false);

    assertNull(result);
  }

  @Test
  public void testAddDefaultEntityFilters_NoDocumentWithBaseFilter() {
    // When DOCUMENT is not in entity types, base filter should be returned unchanged
    List<String> entityTypes = ImmutableList.of(DATASET_ENTITY_NAME);
    Filter baseFilter = new Filter();

    Filter result =
        DefaultEntityFiltersUtil.addDefaultEntityFilters(baseFilter, entityTypes, true, false);

    assertSame(result, baseFilter);
  }

  @Test
  public void testAddDefaultEntityFilters_WithDocumentAndShowInGlobalContext() {
    // Should produce two OR clauses: lifecycleStage = PUBLISHED, legacy fallback
    List<String> entityTypes = ImmutableList.of(DATASET_ENTITY_NAME, DOCUMENT_ENTITY_NAME);

    Filter result =
        DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true, false);

    assertNotNull(result);
    assertNotNull(result.getOr());
    assertEquals(result.getOr().size(), 2);

    // Clause 1: lifecycleStage = PUBLISHED AND showInGlobalContext != false
    ConjunctiveCriterion publishedClause = result.getOr().get(0);
    assertEquals(publishedClause.getAnd().size(), 2);
    assertTrue(hasFieldWithCondition(publishedClause, "lifecycleStage", Condition.EQUAL));
    assertTrue(
        hasNegatedFieldWithCondition(publishedClause, "showInGlobalContext", Condition.EQUAL));

    // Clause 2: lifecycleStage IS_NULL AND state != UNPUBLISHED AND showInGlobalContext != false
    ConjunctiveCriterion legacyClause = result.getOr().get(1);
    assertEquals(legacyClause.getAnd().size(), 3);
    assertTrue(hasFieldWithCondition(legacyClause, "lifecycleStage", Condition.IS_NULL));
    assertTrue(hasNegatedFieldWithCondition(legacyClause, "state", Condition.EQUAL));
    assertTrue(hasNegatedFieldWithCondition(legacyClause, "showInGlobalContext", Condition.EQUAL));
  }

  @Test
  public void testAddDefaultEntityFilters_WithDocumentWithoutShowInGlobalContext() {
    // Without showInGlobalContext, clauses should omit that criterion
    List<String> entityTypes = ImmutableList.of(DOCUMENT_ENTITY_NAME);

    Filter result =
        DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, false, false);

    assertNotNull(result);
    assertNotNull(result.getOr());
    assertEquals(result.getOr().size(), 2);

    // Clause 1: lifecycleStage = PUBLISHED (no showInGlobalContext)
    ConjunctiveCriterion publishedClause = result.getOr().get(0);
    assertEquals(publishedClause.getAnd().size(), 1);
    assertFalse(hasField(publishedClause, "showInGlobalContext"));

    // Clause 2: lifecycleStage IS_NULL AND state != UNPUBLISHED (no showInGlobalContext)
    ConjunctiveCriterion legacyClause = result.getOr().get(1);
    assertEquals(legacyClause.getAnd().size(), 2);
    assertFalse(hasField(legacyClause, "showInGlobalContext"));
  }

  @Test
  public void testAddDefaultEntityFilters_DocumentOnlyEntity() {
    // When only DOCUMENT is being searched
    List<String> entityTypes = ImmutableList.of(DOCUMENT_ENTITY_NAME);

    Filter result =
        DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true, false);

    assertNotNull(result);
    assertEquals(result.getOr().size(), 2);
  }

  @Test
  public void testAddDefaultEntityFilters_VerifyFilterValues() {
    // Verify the actual filter values are correct
    List<String> entityTypes = ImmutableList.of(DOCUMENT_ENTITY_NAME);

    Filter result =
        DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true, false);

    // Clause 1: lifecycleStage = PUBLISHED
    ConjunctiveCriterion publishedClause = result.getOr().get(0);
    Criterion lifecycleCriterion =
        findCriterionByCondition(publishedClause, "lifecycleStage", Condition.EQUAL);
    assertNotNull(lifecycleCriterion);
    assertFalse(lifecycleCriterion.isNegated());
    assertTrue(lifecycleCriterion.getValues().contains("urn:li:lifecycleStageType:PUBLISHED"));

    Criterion showCtx1 = findNegatedCriterion(publishedClause, "showInGlobalContext");
    assertNotNull(showCtx1);
    assertTrue(showCtx1.getValues().contains("false"));

    // Clause 2: lifecycleStage IS_NULL AND state != UNPUBLISHED
    ConjunctiveCriterion legacyClause = result.getOr().get(1);
    Criterion isNullCriterion =
        findCriterionByCondition(legacyClause, "lifecycleStage", Condition.IS_NULL);
    assertNotNull(isNullCriterion);

    Criterion stateCriterion = findNegatedCriterion(legacyClause, "state");
    assertNotNull(stateCriterion);
    assertTrue(stateCriterion.getValues().contains("UNPUBLISHED"));

    Criterion showCtx2 = findNegatedCriterion(legacyClause, "showInGlobalContext");
    assertNotNull(showCtx2);
    assertTrue(showCtx2.getValues().contains("false"));
  }

  @Test
  public void testAddDefaultEntityFilters_CanManageDocumentsProducesTwoClauses() {
    List<String> entityTypes = ImmutableList.of(DOCUMENT_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true, true);

    assertNotNull(result);
    // 2 clauses: published, legacy (no state filter). ES-level hideInSearch handles hidden stages.
    assertEquals(result.getOr().size(), 2);
  }

  @Test
  public void testAddDefaultEntityFilters_CanManageDocumentsLegacyClauseNoStateFilter() {
    List<String> entityTypes = ImmutableList.of(DOCUMENT_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true, true);

    // Clause 2 (legacy): lifecycleStage IS_NULL, no state != UNPUBLISHED filter
    ConjunctiveCriterion legacyClause = result.getOr().get(1);
    assertTrue(hasFieldWithCondition(legacyClause, "lifecycleStage", Condition.IS_NULL));
    assertNull(findNegatedCriterion(legacyClause, "state"));
  }

  private static boolean hasField(ConjunctiveCriterion clause, String field) {
    return findCriterion(clause, field) != null;
  }

  private static boolean hasFieldWithCondition(
      ConjunctiveCriterion clause, String field, Condition condition) {
    Criterion criterion = findCriterionByCondition(clause, field, condition);
    return criterion != null && !criterion.isNegated();
  }

  private static boolean hasNegatedFieldWithCondition(
      ConjunctiveCriterion clause, String field, Condition condition) {
    Criterion criterion = findCriterionByCondition(clause, field, condition);
    return criterion != null && criterion.isNegated();
  }

  private static Criterion findCriterion(ConjunctiveCriterion clause, String field) {
    for (Criterion criterion : clause.getAnd()) {
      if (field.equals(criterion.getField())) {
        return criterion;
      }
    }
    return null;
  }

  private static Criterion findCriterionByCondition(
      ConjunctiveCriterion clause, String field, Condition condition) {
    for (Criterion criterion : clause.getAnd()) {
      if (field.equals(criterion.getField()) && condition.equals(criterion.getCondition())) {
        return criterion;
      }
    }
    return null;
  }

  private static Criterion findNegatedCriterion(ConjunctiveCriterion clause, String field) {
    for (Criterion criterion : clause.getAnd()) {
      if (field.equals(criterion.getField()) && criterion.isNegated()) {
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
    assertTrue(DefaultEntityFiltersUtil.DEFAULT_FILTER_FIELDS.contains("lifecycleStage"));
    assertTrue(DefaultEntityFiltersUtil.DEFAULT_FILTER_FIELDS.contains("showInGlobalContext"));
    assertEquals(DefaultEntityFiltersUtil.DEFAULT_FILTER_FIELDS.size(), 3);
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
